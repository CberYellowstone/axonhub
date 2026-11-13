// 职责: 用持久化租约调度插件周期任务，并为宿主副作用提供实时围栏校验。
// 关联文件:
//
//	internal/ent/schema/plugin_schedule.go
//	internal/server/plugin/manager.go
//	internal/server/plugin/invoke.go
//	internal/server/biz/plugin_notification.go
//
// 注意事项:
//
//	至少一次投递；失败和租约接管复用 runId。插件必须自行按 runId 幂等。
//	DB 副作用须通过 withScheduleWrite；它仅序列化短 DB 事务，不包裹 JS 或 HTTP，也不保证外部投递 exactly-once。
package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	entplugin "github.com/looplj/axonhub/internal/ent/plugin"
	"github.com/looplj/axonhub/internal/ent/pluginschedule"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
)

const (
	scheduleBudget      = 10 * time.Second
	scheduleLease       = scheduleBudget + 2*time.Second
	scheduleBatch       = 16
	scheduleConcurrency = 4
	scheduleWriteBudget = 2 * time.Second
)

var errScheduleFenced = errors.New("plugin schedule lease or authorization revision is no longer current")

// ScheduleJob is passed as the second argument to onSchedule(ctx, job).
type ScheduleJob struct {
	Key         string    `json:"key"`
	RunID       string    `json:"runId"`
	ScheduledAt time.Time `json:"scheduledAt"`
	Attempt     int       `json:"attempt"`
}

type durableScheduler struct {
	manager           *Manager
	owner             string
	mu                sync.Mutex
	cancel            context.CancelFunc
	done              chan struct{}
	notificationsDone chan struct{}
	signal            chan struct{}
	cursor            int
	// Tests override execution, never the persistence and fencing path.
	invoke             func(context.Context, *LoadedPlugin, ScheduleJob) error
	drainNotifications func(context.Context, int) (int, error)
	// Tests may shorten the budget before starting; no override may extend 10s.
	callTimeout time.Duration
}

func newDurableScheduler(m *Manager) *durableScheduler {
	return &durableScheduler{manager: m, owner: uuid.NewString(), signal: make(chan struct{}, 1)}
}

func (s *durableScheduler) callBudget() time.Duration {
	if s.callTimeout > 0 {
		return min(s.callTimeout, scheduleBudget)
	}
	return scheduleBudget
}

func (s *durableScheduler) start() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return
	}
	s.manager.callsMu.Lock()
	stopping := s.manager.stopping
	s.manager.callsMu.Unlock()
	if stopping {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.notificationsDone = make(chan struct{})
	go s.notificationLoop(ctx)
	go func() {
		defer close(s.done)
		defer func() {
			if value := recover(); value != nil {
				log.Error(ctx, "plugin scheduler panicked", log.Any("panic", value), log.Stack("stack"))
			}
		}()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if err := s.tick(ctx); err != nil && ctx.Err() == nil {
				log.Warn(ctx, "plugin scheduler round failed", log.Cause(err))
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-s.signal:
			}
		}
	}()
}

func (s *durableScheduler) stop() {
	s.mu.Lock()
	cancel, done, notificationsDone := s.cancel, s.done, s.notificationsDone
	s.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
		<-notificationsDone
	}
}

// notificationLoop is independent: slow webhook I/O never consumes schedule leases.
// Keep draining after disable/delete so the outbox can cancel unauthorized entries.
func (s *durableScheduler) notificationLoop(ctx context.Context) {
	defer close(s.notificationsDone)
	defer func() {
		if value := recover(); value != nil {
			log.Error(ctx, "plugin notification loop panicked", log.Any("panic", value), log.Stack("stack"))
		}
	}()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		batchCtx, cancel := context.WithTimeout(scheduleHostContext(ctx), 155*time.Second)
		var err error
		if s.drainNotifications != nil {
			_, err = s.drainNotifications(batchCtx, 5)
		} else {
			_, err = biz.DeliverPluginNotifications(batchCtx, s.manager.db, s.manager.webhooks, 5)
		}
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Warn(ctx, "plugin notification drain failed", log.Cause(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *durableScheduler) wake() {
	select {
	case s.signal <- struct{}{}:
	default:
	}
}

func scheduleHostContext(ctx context.Context) context.Context {
	return authz.WithSystemBypass(authz.NewSystemContext(ctx), "plugin-durable-scheduler")
}

// scheduleDBNow reads the database's UTC clock, not an application instance clock.
func scheduleDBNow(ctx context.Context, db *ent.Client) (time.Time, error) {
	var rows []struct {
		Epoch float64 `json:"epoch"`
	}
	err := db.Plugin.Query().Limit(1).Aggregate(func(sel *sql.Selector) string {
		expression := "(julianday('now') - 2440587.5) * 86400.0"
		switch sel.Dialect() {
		case "postgres":
			expression = "EXTRACT(EPOCH FROM clock_timestamp())"
		case "mysql":
			expression = "UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6))"
		}
		return sql.As(expression, "epoch")
	}).Scan(ctx, &rows)
	if err != nil {
		return time.Time{}, fmt.Errorf("read scheduler database clock: %w", err)
	}
	if len(rows) == 0 {
		return time.Time{}, errScheduleFenced
	}
	seconds, fraction := math.Modf(rows[0].Epoch)
	return time.Unix(int64(seconds), int64(fraction*1e9)).UTC(), nil
}

// Evaluate lease expiry inside the fencing statement, not against an earlier clock
// sample that could become stale while waiting for a connection or database lock.
func scheduleLeaseComparison(sel *sql.Selector, operator string) *sql.Predicate {
	column := sel.C(pluginschedule.FieldLeaseUntil)
	switch sel.Dialect() {
	case "postgres":
		return sql.ExprP(column + " " + operator + " clock_timestamp()")
	case "mysql":
		return sql.ExprP(column + " " + operator + " UTC_TIMESTAMP(6)")
	default:
		// Leases are written in UTC. modernc's default time.String suffix is
		// not accepted by julianday; its first 19 bytes remain UTC seconds.
		// Rounding that fallback down expires a lease early, never late.
		return sql.ExprP("COALESCE(julianday(" + column + "), julianday(substr(" + column + ",1,19))) " + operator + " julianday('now')")
	}
}

func scheduleLeaseLive(sel *sql.Selector) { sel.Where(scheduleLeaseComparison(sel, ">")) }
func scheduleLeaseExpired(sel *sql.Selector) {
	sel.Where(sql.Or(sql.IsNull(sel.C(pluginschedule.FieldLeaseUntil)), scheduleLeaseComparison(sel, "<=")))
}

func scheduleRevision(p *LoadedPlugin) string {
	value, _ := json.Marshal(struct {
		UpdatedAt time.Time
		Package   string
		Config    map[string]any
		Grants    []string
		Network   []string
		Manifest  objects.PluginManifest
		Scope     objects.PluginAutomationScope
	}{p.updatedAt.UTC(), p.PackageSHA256, p.Config, p.GrantedPermissions, p.GrantedNetwork, p.Manifest, p.automationScope})
	hash := sha256.Sum256(value)
	return hex.EncodeToString(hash[:])
}

func (s *durableScheduler) current(ctx context.Context, p *LoadedPlugin) error {
	if err := s.currentGeneration(p); err != nil {
		return err
	}
	row, err := s.manager.db.Plugin.Query().Where(entplugin.IDEQ(p.ID), entplugin.Enabled(true)).Select(pluginMetaColumns...).Only(ctx)
	if err != nil {
		return errScheduleFenced
	}
	if scheduleRevision(recordFromEntity(row).loaded()) != scheduleRevision(p) {
		return errScheduleFenced
	}
	return nil
}

func (s *durableScheduler) currentGeneration(p *LoadedPlugin) error {
	s.manager.callsMu.Lock()
	stopping := s.manager.stopping
	s.manager.callsMu.Unlock()
	if stopping {
		return errScheduleFenced
	}
	local := s.manager.current().ByID(p.ID)
	if local == nil || local.Status != RuntimeStatusActive || scheduleRevision(local) != scheduleRevision(p) {
		return errScheduleFenced
	}
	return nil
}

type scheduleContextKey struct{}
type scheduleCall struct {
	scheduler *durableScheduler
	plugin    *LoadedPlugin
	rowID     int
	token     string
	revision  string
}

func scheduleCallFence(call *scheduleCall) func(*sql.Selector) {
	return func(sel *sql.Selector) {
		sel.Where(sql.And(
			sql.EQ(sel.C(pluginschedule.FieldID), call.rowID),
			sql.EQ(sel.C(pluginschedule.FieldInstallationID), call.plugin.ID),
			sql.EQ(sel.C(pluginschedule.FieldRevision), call.revision),
			sql.EQ(sel.C(pluginschedule.FieldLeaseOwner), call.scheduler.owner),
			sql.EQ(sel.C(pluginschedule.FieldLeaseToken), call.token),
			scheduleLeaseComparison(sel, ">"),
		))
	}
}

func scheduleRowLock(sel *sql.Selector) {
	switch sel.Dialect() {
	case "mysql", "postgres":
		sel.ForUpdate()
	}
}

// withScheduleWrite serializes a fixed host DB operation with lease claim/ack and
// installation updates. Call it BEFORE RunAsHost strips the schedule context.
// fn receives System authority and the same transaction client via ent.FromContext;
// it must contain only bounded host DB work, never plugin callbacks or network I/O.
// Non-schedule calls retain their original context and transaction semantics.
func (m *Manager) withScheduleWrite(ctx context.Context, fn func(context.Context) error) error {
	call, scheduled := ctx.Value(scheduleContextKey{}).(*scheduleCall)
	if !scheduled {
		return fn(ctx)
	}
	if call.scheduler.manager != m {
		return errScheduleFenced
	}
	ctx, cancel := context.WithTimeout(ctx, scheduleWriteBudget)
	defer cancel()
	_, err := authz.RunAsHost(ctx, m.db, "plugin-schedule-write", func(hostCtx context.Context) (bool, error) {
		if err := call.scheduler.currentGeneration(call.plugin); err != nil {
			return false, err
		}
		tx, err := m.db.Tx(hostCtx)
		if err != nil {
			return false, err
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
		client := tx.Client()
		txCtx := ent.NewTxContext(ent.NewContext(hostCtx, client), tx)
		fence := scheduleCallFence(call)
		// UPDATE obtains the row write lock on every dialect, including SQLite.
		// Do not trust affected-row counts: MySQL may return zero for a no-op.
		if _, err := client.PluginSchedule.Update().Where(fence).SetLeaseToken(call.token).Save(txCtx); err != nil {
			return false, err
		}
		if _, err := client.PluginSchedule.Query().Where(fence, scheduleRowLock).Only(txCtx); err != nil {
			if ent.IsNotFound(err) {
				return false, errScheduleFenced
			}
			return false, err
		}
		// Preserve UpdatedAt: changing it would retire our own authorization revision.
		// FOR UPDATE below is a current read even under MySQL repeatable-read.
		if _, err := client.Plugin.Update().Where(entplugin.IDEQ(call.plugin.ID), entplugin.Enabled(true), entplugin.UpdatedAtEQ(call.plugin.updatedAt)).
			SetUpdatedAt(call.plugin.updatedAt).Save(txCtx); err != nil {
			return false, err
		}
		row, err := client.Plugin.Query().Where(entplugin.IDEQ(call.plugin.ID), entplugin.Enabled(true), scheduleRowLock).Select(pluginMetaColumns...).Only(txCtx)
		if err != nil {
			if ent.IsNotFound(err) {
				return false, errScheduleFenced
			}
			return false, err
		}
		if scheduleRevision(recordFromEntity(row).loaded()) != call.revision {
			return false, errScheduleFenced
		}
		if err := fn(txCtx); err != nil {
			return false, err
		}
		if err := call.scheduler.currentGeneration(call.plugin); err != nil {
			return false, err
		}
		// A lease that expires while the closure runs rolls back its DB effects.
		if _, err := client.PluginSchedule.Query().Where(fence, scheduleRowLock).Only(txCtx); err != nil {
			if ent.IsNotFound(err) {
				return false, errScheduleFenced
			}
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		committed = true
		return true, nil
	})
	return err
}

// validateScheduleCall must be called immediately before every host side effect.
// Non-schedule contexts pass unchanged. Never replace the caller's plugin principal.
func validateScheduleCall(ctx context.Context) error {
	call, ok := ctx.Value(scheduleContextKey{}).(*scheduleCall)
	if !ok {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Host reads intentionally detach identity and privacy decisions, preserving cancellation.
	_, err := authz.RunAsHost(ctx, call.scheduler.manager.db, "plugin-schedule-fence", func(hostCtx context.Context) (bool, error) {
		if err := call.scheduler.current(hostCtx, call.plugin); err != nil {
			return false, err
		}
		valid, err := call.scheduler.manager.db.PluginSchedule.Query().Where(scheduleCallFence(call)).Exist(hostCtx)
		if err != nil {
			return false, err
		}
		if !valid {
			return false, errScheduleFenced
		}
		return true, nil
	})
	return err
}

func (s *durableScheduler) tick(ctx context.Context) error {
	gen := s.manager.current()
	if gen == nil {
		return nil
	}
	var active []*LoadedPlugin
	for _, p := range gen.plugins {
		if p.Status == RuntimeStatusActive && len(p.Manifest.Schedules) > 0 {
			active = append(active, p)
		}
	}
	// No schedule table or clock queries for installations without schedules.
	if len(active) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	hostCtx := scheduleHostContext(ctx)
	var workers sync.WaitGroup
	defer workers.Wait()
	permits := make(chan struct{}, scheduleConcurrency)
	count := 0
	start := s.cursor % len(active)
	for offset := 0; offset < len(active) && offset < scheduleBatch; offset++ {
		if count >= scheduleBatch {
			return nil
		}
		p := active[(start+offset)%len(active)]
		s.cursor = (start + offset + 1) % len(active)
		if err := s.current(hostCtx, p); err != nil {
			continue
		}
		rows, err := s.reconcile(hostCtx, p)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if count >= scheduleBatch {
				return nil
			}
			select {
			case permits <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			claimed, err := s.claim(hostCtx, p, row)
			if err != nil {
				<-permits
				return err
			}
			if claimed == nil {
				<-permits
				continue
			}
			count++
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() { <-permits }()
				defer func() {
					if value := recover(); value != nil {
						log.Error(ctx, "plugin schedule worker panicked", log.Any("panic", value), log.Stack("stack"))
					}
				}()
				s.execute(ctx, p, claimed)
			}()
		}
	}
	return nil
}

func (s *durableScheduler) reconcile(ctx context.Context, p *LoadedPlugin) ([]*ent.PluginSchedule, error) {
	now, err := scheduleDBNow(ctx, s.manager.db)
	if err != nil {
		return nil, err
	}
	revision := scheduleRevision(p)
	existing, err := s.manager.db.PluginSchedule.Query().Where(pluginschedule.InstallationIDEQ(p.ID)).All(ctx)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]*ent.PluginSchedule, len(existing))
	for _, row := range existing {
		byKey[row.Key] = row
	}
	keys := make([]string, 0, len(p.Manifest.Schedules))
	for _, spec := range p.Manifest.Schedules {
		keys = append(keys, spec.Key)
		row := byKey[spec.Key]
		if row == nil {
			if err := s.current(ctx, p); err != nil {
				return nil, err
			}
			if err := s.manager.db.PluginSchedule.Create().SetInstallationID(p.ID).SetKey(spec.Key).
				SetIntervalSeconds(spec.IntervalSeconds).SetRevision(revision).SetNextRunAt(now.Add(time.Duration(spec.IntervalSeconds)*time.Second)).
				OnConflictColumns(pluginschedule.FieldInstallationID, pluginschedule.FieldKey).Ignore().Exec(ctx); err != nil {
				return nil, err
			}
			continue
		}
		if row.Revision != revision {
			if err := s.current(ctx, p); err != nil {
				return nil, err
			}
			_, err = s.manager.db.PluginSchedule.Update().Where(pluginschedule.IDEQ(row.ID), pluginschedule.RevisionEQ(row.Revision)).
				SetRevision(revision).SetIntervalSeconds(spec.IntervalSeconds).SetNextRunAt(now.Add(time.Duration(spec.IntervalSeconds) * time.Second)).
				SetRunID("").ClearScheduledAt().SetAttempt(0).SetLeaseOwner("").SetLeaseToken("").ClearLeaseUntil().Save(ctx)
			if err != nil {
				return nil, err
			}
		}
	}
	for _, row := range existing {
		if slices.Contains(keys, row.Key) {
			continue
		}
		// Only remove obsolete keys after validating this exact installation revision.
		if err := s.current(ctx, p); err != nil {
			return nil, err
		}
		if _, err := s.manager.db.PluginSchedule.Delete().Where(pluginschedule.IDEQ(row.ID), pluginschedule.RevisionEQ(row.Revision)).Exec(ctx); err != nil {
			return nil, err
		}
	}
	return s.manager.db.PluginSchedule.Query().Where(pluginschedule.InstallationIDEQ(p.ID), pluginschedule.RevisionEQ(revision), pluginschedule.NextRunAtLTE(now)).Order(ent.Asc(pluginschedule.FieldNextRunAt)).Limit(8).All(ctx)
}

func (s *durableScheduler) claim(ctx context.Context, p *LoadedPlugin, row *ent.PluginSchedule) (*ent.PluginSchedule, error) {
	if err := s.current(ctx, p); err != nil {
		return nil, nil
	}
	now, err := scheduleDBNow(ctx, s.manager.db)
	if err != nil {
		return nil, err
	}
	runID, scheduledAt := row.RunID, row.ScheduledAt
	if runID == "" {
		runID = uuid.NewString()
		value := row.NextRunAt
		scheduledAt = &value
	}
	token := uuid.NewString()
	updated, err := s.manager.db.PluginSchedule.Update().Where(
		pluginschedule.IDEQ(row.ID), pluginschedule.RevisionEQ(scheduleRevision(p)), pluginschedule.NextRunAtLTE(now),
		scheduleLeaseExpired,
		pluginschedule.LeaseTokenEQ(row.LeaseToken), pluginschedule.AttemptEQ(row.Attempt),
	).SetRunID(runID).SetNillableScheduledAt(scheduledAt).SetAttempt(row.Attempt + 1).
		SetLeaseOwner(s.owner).SetLeaseToken(token).SetLeaseUntil(now.Add(s.callBudget() + 2*time.Second)).Save(ctx)
	if err != nil || updated == 0 {
		return nil, err
	}
	claimed := *row
	claimed.RunID, claimed.ScheduledAt, claimed.Attempt = runID, scheduledAt, row.Attempt+1
	until := now.Add(s.callBudget() + 2*time.Second)
	claimed.LeaseOwner, claimed.LeaseToken, claimed.LeaseUntil = s.owner, token, &until
	return &claimed, nil
}

func (s *durableScheduler) execute(parent context.Context, p *LoadedPlugin, row *ent.PluginSchedule) {
	ctx, cancel := context.WithTimeout(parent, s.callBudget())
	defer cancel()
	ctx = context.WithValue(ctx, scheduleContextKey{}, &scheduleCall{s, p, row.ID, row.LeaseToken, row.Revision})
	err := validateScheduleCall(ctx)
	if err == nil {
		job := ScheduleJob{row.Key, row.RunID, *row.ScheduledAt, row.Attempt}
		if s.invoke != nil {
			err = s.invoke(ctx, p, job)
		} else {
			args, _ := json.Marshal([]ScheduleJob{job})
			// Unlike request hooks, schedule execution must not extend the parent
			// deadline after VM acquisition. Zero Timeout uses ctx's entire budget.
			_, err = s.manager.Invoke(ctx, InvokeRequest{InstallationID: p.ID, Export: "onSchedule", Args: args, Config: p.Config, Generation: p.generation, BorrowWait: s.callBudget()})
		}
	}
	ackCtx, ackCancel := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer ackCancel()
	if ackErr := s.ack(scheduleHostContext(ackCtx), p, row, err); ackErr != nil {
		log.Warn(ackCtx, "plugin schedule acknowledgement failed", log.Cause(ackErr))
	}
}

func (s *durableScheduler) ack(ctx context.Context, p *LoadedPlugin, row *ent.PluginSchedule, callErr error) error {
	now, err := scheduleDBNow(ctx, s.manager.db)
	if err != nil {
		return err
	}
	update := s.manager.db.PluginSchedule.Update().Where(pluginschedule.IDEQ(row.ID), pluginschedule.RevisionEQ(row.Revision),
		pluginschedule.LeaseOwnerEQ(s.owner), pluginschedule.LeaseTokenEQ(row.LeaseToken), scheduleLeaseLive).
		SetLeaseOwner("").SetLeaseToken("").ClearLeaseUntil()
	if callErr == nil {
		update.SetRunID("").ClearScheduledAt().SetAttempt(0).SetNextRunAt(now.Add(time.Duration(row.IntervalSeconds) * time.Second))
	} else {
		// Bounded durable backoff; Invoke maintains the shared process circuit breaker.
		backoff := min(5*time.Second*time.Duration(1<<min(row.Attempt-1, 6)), 5*time.Minute)
		update.SetNextRunAt(now.Add(backoff))
	}
	_, err = update.Save(ctx)
	return err
}
