// 职责: 验证插件持久调度的租约、重试、版本围栏和声明契约。
// 关联文件:
//
//	internal/server/plugin/scheduler.go
//	internal/server/plugin/manifest.go
//	internal/server/plugin/jsruntime.go
package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/pluginschedule"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/stretchr/testify/require"
)

func TestScheduleManifestContract(t *testing.T) {
	for _, extra := range []string{
		`"schedules":null`, `"schedules":{}`, `"schedules":[null]`,
		`"schedules":[{"key":"cache-check","intervalSeconds":59}]`,
		`"schedules":[{"key":"cache-check","intervalSeconds":86401}]`,
		`"schedules":[{"key":"UPPER","intervalSeconds":60}]`,
		`"schedules":[{"key":"a","intervalSeconds":60,"unknown":true}]`,
		`"schedules":[{"key":"a","key":"b","intervalSeconds":60}]`,
		`"schedules":[{"key":"a","intervalSeconds":60.5}]`,
		`"schedules":[{"key":"a","intervalSeconds":"60"}]`,
		`"schedules":[{"key":"a","intervalSeconds":60},{"key":"a","intervalSeconds":60}]`,
		`"requiredCapabilities":null`, `"requiredCapabilities":[7]`,
		`"requiredCapabilities":["scheduler.durable.v1","scheduler.durable.v1"]`,
		`"requiredCapabilities":["unknown.v1"]`,
		`"pluginApiVersion":2,"schedules":[{"key":"a","intervalSeconds":60}]`,
	} {
		t.Run(extra, func(t *testing.T) {
			base := strings.TrimSuffix(strings.TrimSpace(validManifestJSON), "}")
			_, err := ParseManifest([]byte(base+","+extra+"}"), "1.0.0")
			require.Error(t, err)
		})
	}
	base := strings.TrimSuffix(strings.TrimSpace(validManifestJSON), "}")
	manifest, err := ParseManifest([]byte(base+`,"schedules":[{"key":"cache-check","intervalSeconds":60}],"requiredCapabilities":["scheduler.durable.v1"]}`), "1.0.0")
	require.NoError(t, err)
	data, err := json.Marshal(manifest)
	require.NoError(t, err)
	var roundtrip objects.PluginManifest
	require.NoError(t, json.Unmarshal(data, &roundtrip))
	require.Equal(t, manifest.Schedules, roundtrip.Schedules)
	require.Equal(t, manifest.RequiredCapabilities, roundtrip.RequiredCapabilities)
	require.ErrorContains(t, ValidateExports(manifest, map[string]ExportFunc{}), "onSchedule")
	require.NoError(t, ValidateExports(manifest, map[string]ExportFunc{"onSchedule": {Function: true}}))
	manifest.Schedules = make([]objects.PluginScheduleSpec, 9)
	require.ErrorContains(t, manifest.Validate(), "at most 8")
}

func TestSchedulePeriodPreview(t *testing.T) {
	old := &objects.PluginManifest{Schedules: []objects.PluginScheduleSpec{{Key: "a", IntervalSeconds: 120}}}
	for _, test := range []struct {
		seconds int
		preview bool
	}{{60, true}, {120, false}, {240, false}} {
		next := &objects.PluginManifest{Schedules: []objects.PluginScheduleSpec{{Key: "a", IntervalSeconds: test.seconds}}}
		require.Equal(t, test.preview, DiffManifest(old, next).NeedsPreview())
	}
	require.True(t, DiffManifest(nil, old).NeedsPreview())
}

func scheduledPlugin(t *testing.T, source string) (*Manager, *LoadedPlugin, *ent.PluginSchedule) {
	t.Helper()
	db := newPluginDB(t)
	m := newTestManager(t, db)
	zip := uiZip(t, func(t *testing.T, manifest map[string]any) {
		manifest["schedules"] = []any{map[string]any{"key": "cache-check", "intervalSeconds": 60}}
		manifest["requiredCapabilities"] = []string{"scheduler.durable.v1"}
	}, map[string]string{"ui/page.html": "<html></html>", "plugin.js": source})
	row := installUI(t, m, zip)
	require.NoError(t, db.Plugin.UpdateOneID(row.ID).SetEnabled(true).Exec(pluginCtx()))
	require.NoError(t, m.ForceRefresh(pluginCtx()))
	p := m.Current().ByID(row.ID)
	require.Equal(t, RuntimeStatusActive, p.Status)
	_, err := m.scheduler.reconcile(pluginCtx(), p)
	require.NoError(t, err)
	now, err := scheduleDBNow(pluginCtx(), db)
	require.NoError(t, err)
	require.NoError(t, db.PluginSchedule.Update().SetNextRunAt(now.Add(-time.Hour)).Exec(pluginCtx()))
	job, err := db.PluginSchedule.Query().Only(pluginCtx())
	require.NoError(t, err)
	return m, p, job
}

func scheduleTestCall(s *durableScheduler, p *LoadedPlugin, row *ent.PluginSchedule) context.Context {
	return context.WithValue(context.Background(), scheduleContextKey{}, &scheduleCall{s, p, row.ID, row.LeaseToken, row.Revision})
}

func TestScheduleTwoManagersClaimOneJob(t *testing.T) {
	m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
	other := newTestManager(t, m.db)
	var workers sync.WaitGroup
	var mu sync.Mutex
	claims := 0
	for _, s := range []*durableScheduler{m.scheduler, other.scheduler} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Errorf("worker panic: %v", recovered)
				}
			}()
			own := s.manager.current().ByID(p.ID)
			claimed, err := s.claim(pluginCtx(), own, row)
			if err != nil {
				require.Contains(t, err.Error(), "locked")
				return
			}
			if claimed != nil {
				mu.Lock()
				claims++
				mu.Unlock()
			}
		}()
	}
	workers.Wait()
	require.Equal(t, 1, claims)
}

func TestScheduleBusyBatchDoesNotStarveNextInstallation(t *testing.T) {
	db := newPluginDB(t)
	m := newTestManager(t, db)
	for _, id := range []string{"acme.fair-a", "acme.fair-b", "acme.fair-c"} {
		installed := installUI(t, m, uiZip(t, func(_ *testing.T, manifest map[string]any) {
			manifest["id"] = id
			specs := make([]objects.PluginScheduleSpec, 8)
			for i := range specs {
				specs[i] = objects.PluginScheduleSpec{Key: "job-" + string(rune('a'+i)), IntervalSeconds: 60}
			}
			manifest["schedules"] = specs
		}, map[string]string{"ui/page.html": "<html></html>", "plugin.js": `export function onSchedule(ctx, job) {}`}))
		db.Plugin.UpdateOneID(installed.ID).SetEnabled(true).SaveX(pluginCtx())
	}
	require.NoError(t, m.ForceRefresh(pluginCtx()))
	counts := map[int]*atomic.Int32{}
	for _, p := range m.current().plugins {
		counts[p.ID] = &atomic.Int32{}
		_, err := m.scheduler.reconcile(pluginCtx(), p)
		require.NoError(t, err)
	}
	require.Len(t, counts, 3)
	m.scheduler.invoke = func(_ context.Context, p *LoadedPlugin, _ ScheduleJob) error {
		counts[p.ID].Add(1)
		return errors.New("retryable test failure")
	}
	for range 2 {
		// Simulate continuously overdue work, such as slow failures whose next
		// retry is already due by the time the other workers finish the round.
		db.PluginSchedule.Update().SetNextRunAt(time.Now().UTC().Add(-time.Hour)).SaveX(pluginCtx())
		require.NoError(t, m.scheduler.tick(context.Background()))
	}
	for id, count := range counts {
		require.Positive(t, count.Load(), "installation %d must receive a turn even when the preceding batch stays full", id)
	}
}

func TestScheduleExpiredWorkerFencedAndRetryIdentity(t *testing.T) {
	m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
	first, err := m.scheduler.claim(pluginCtx(), p, row)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NoError(t, validateScheduleCall(scheduleTestCall(m.scheduler, p, first)))
	now, err := scheduleDBNow(pluginCtx(), m.db)
	require.NoError(t, err)
	require.NoError(t, m.db.PluginSchedule.UpdateOneID(row.ID).SetLeaseUntil(now.Add(-time.Second)).Exec(pluginCtx()))
	require.Error(t, validateScheduleCall(scheduleTestCall(m.scheduler, p, first)))
	other := newTestManager(t, m.db)
	latest, err := m.db.PluginSchedule.Get(pluginCtx(), row.ID)
	require.NoError(t, err)
	second, err := other.scheduler.claim(pluginCtx(), other.current().ByID(p.ID), latest)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, first.RunID, second.RunID)
	require.Equal(t, *first.ScheduledAt, *second.ScheduledAt)
	require.Equal(t, 2, second.Attempt)
	require.NotEqual(t, first.LeaseToken, second.LeaseToken)
	require.NoError(t, m.scheduler.ack(pluginCtx(), p, first, nil))
	after, err := m.db.PluginSchedule.Get(pluginCtx(), row.ID)
	require.NoError(t, err)
	require.Equal(t, second.LeaseToken, after.LeaseToken)
	require.Equal(t, second.RunID, after.RunID)
}

func TestScheduleFailureRestartAndCoalescing(t *testing.T) {
	m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
	first, err := m.scheduler.claim(pluginCtx(), p, row)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NoError(t, m.scheduler.ack(pluginCtx(), p, first, errors.New("retry")))
	stored, err := m.db.PluginSchedule.Get(pluginCtx(), row.ID)
	require.NoError(t, err)
	require.Equal(t, first.RunID, stored.RunID)
	require.Empty(t, stored.LeaseToken)
	now, err := scheduleDBNow(pluginCtx(), m.db)
	require.NoError(t, err)
	require.NoError(t, m.db.PluginSchedule.UpdateOneID(row.ID).SetNextRunAt(now.Add(-24*time.Hour)).Exec(pluginCtx()))
	restarted := newTestManager(t, m.db)
	var seen []ScheduleJob
	restarted.scheduler.invoke = func(ctx context.Context, p *LoadedPlugin, job ScheduleJob) error {
		require.NoError(t, validateScheduleCall(ctx))
		seen = append(seen, job)
		return nil
	}
	require.NoError(t, restarted.scheduler.tick(context.Background()))
	require.Len(t, seen, 1)
	require.Equal(t, first.RunID, seen[0].RunID)
	require.Equal(t, 2, seen[0].Attempt)
	require.NoError(t, restarted.scheduler.tick(context.Background()))
	require.Len(t, seen, 1)
	after, err := m.db.PluginSchedule.Get(pluginCtx(), row.ID)
	require.NoError(t, err)
	require.Empty(t, after.RunID)
	require.True(t, after.NextRunAt.After(now))
}

func TestScheduleRevisionDisabledDeleted(t *testing.T) {
	for _, change := range []string{"config", "scope", "disabled", "deleted"} {
		t.Run(change, func(t *testing.T) {
			m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
			claimed, err := m.scheduler.claim(pluginCtx(), p, row)
			require.NoError(t, err)
			ctx := scheduleTestCall(m.scheduler, p, claimed)
			switch change {
			case "config":
				require.NoError(t, m.db.Plugin.UpdateOneID(p.ID).SetConfig(map[string]any{"threshold": 3}).Exec(pluginCtx()))
			case "scope":
				require.NoError(t, m.db.Plugin.UpdateOneID(p.ID).SetAutomationScope(objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: 1, AllKeys: true}}}).Exec(pluginCtx()))
			case "disabled":
				require.NoError(t, m.db.Plugin.UpdateOneID(p.ID).SetEnabled(false).Exec(pluginCtx()))
			case "deleted":
				require.NoError(t, m.db.Plugin.DeleteOneID(p.ID).Exec(pluginCtx()))
			}
			require.Error(t, validateScheduleCall(ctx))
		})
	}
	require.NoError(t, validateScheduleCall(context.Background()))
}

func TestScheduleDeadlineAndVMPool(t *testing.T) {
	m, _, _ := scheduledPlugin(t, `export function onSchedule(ctx,job) { while(true) {} }`)
	m.scheduler.callTimeout = 40 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	_ = m.scheduler.tick(ctx)
	require.Less(t, time.Since(started), 2*time.Second)
	row, err := m.db.PluginSchedule.Query().Only(pluginCtx())
	require.NoError(t, err)
	require.NotEmpty(t, row.RunID)
	require.Empty(t, row.LeaseToken)
	require.Equal(t, 1, row.Attempt)
	require.LessOrEqual(t, scheduleLease, scheduleBudget+2*time.Second)
}

func TestScheduleParentDeadlineIsNotExtended(t *testing.T) {
	m, _, _ := scheduledPlugin(t, `export function onSchedule(ctx,job) { while(true) {} }`)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	_ = m.scheduler.tick(ctx)
	require.Less(t, time.Since(started), 2*time.Second)
	require.Equal(t, scheduleBudget, m.scheduler.callBudget())
	m.scheduler.callTimeout = time.Minute
	require.Equal(t, scheduleBudget, m.scheduler.callBudget(), "a test override must never extend the production limit")
}

func TestSchedulePeriodChangeInvalidatesOldLease(t *testing.T) {
	m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
	claimed, err := m.scheduler.claim(pluginCtx(), p, row)
	require.NoError(t, err)
	manifest := p.Manifest
	manifest.Schedules = []objects.PluginScheduleSpec{{Key: "cache-check", IntervalSeconds: 120}}
	require.NoError(t, m.db.Plugin.UpdateOneID(p.ID).SetManifest(manifest).Exec(pluginCtx()))
	require.NoError(t, m.ForceRefresh(pluginCtx()))
	require.Error(t, validateScheduleCall(scheduleTestCall(m.scheduler, p, claimed)))
	_, err = m.scheduler.reconcile(pluginCtx(), m.current().ByID(p.ID))
	require.NoError(t, err)
	stored, err := m.db.PluginSchedule.Query().Where(pluginschedule.IDEQ(row.ID)).Only(pluginCtx())
	require.NoError(t, err)
	require.Equal(t, 120, stored.IntervalSeconds)
	require.Empty(t, stored.LeaseToken)
	require.Empty(t, stored.RunID)
}

func TestScheduleBoundedConcurrencyAndCallBudget(t *testing.T) {
	m, p, _ := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
	manifest := p.Manifest
	manifest.Schedules = nil
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		manifest.Schedules = append(manifest.Schedules, objects.PluginScheduleSpec{Key: key, IntervalSeconds: 60})
	}
	require.NoError(t, m.db.Plugin.UpdateOneID(p.ID).SetManifest(manifest).Exec(pluginCtx()))
	require.NoError(t, m.ForceRefresh(pluginCtx()))
	p = m.current().ByID(p.ID)
	_, err := m.scheduler.reconcile(pluginCtx(), p)
	require.NoError(t, err)
	now, err := scheduleDBNow(pluginCtx(), m.db)
	require.NoError(t, err)
	require.NoError(t, m.db.PluginSchedule.Update().SetNextRunAt(now.Add(-time.Hour)).Exec(pluginCtx()))
	var active, maxActive, total atomic.Int32
	m.scheduler.invoke = func(ctx context.Context, p *LoadedPlugin, job ScheduleJob) error {
		current := active.Add(1)
		defer active.Add(-1)
		for observed := maxActive.Load(); current > observed; observed = maxActive.Load() {
			if maxActive.CompareAndSwap(observed, current) {
				break
			}
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > scheduleBudget {
			return errors.New("unbounded schedule call")
		}
		time.Sleep(20 * time.Millisecond)
		total.Add(1)
		return nil
	}
	require.NoError(t, m.scheduler.tick(context.Background()))
	require.EqualValues(t, 8, total.Load())
	require.LessOrEqual(t, maxActive.Load(), int32(scheduleConcurrency))
	require.Greater(t, maxActive.Load(), int32(1))
}

func TestScheduleNoQueriesWithoutDeclarations(t *testing.T) {
	m := newTestManager(t, newPluginDB(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A canceled SQL context would error, but this round does not touch persistence.
	require.NoError(t, m.scheduler.tick(ctx))
}

func TestScheduleNotificationLoopLifecycle(t *testing.T) {
	m := newTestManager(t, newPluginDB(t))
	started := make(chan error, 1)
	m.scheduler.drainNotifications = func(ctx context.Context, limit int) (int, error) {
		err := authz.RequireSystemPrincipal(ctx)
		if limit != 5 || ent.TxFromContext(ctx) != nil {
			err = errors.New("notification batch must be bounded and transaction-free")
		}
		started <- err
		<-ctx.Done()
		return 0, ctx.Err()
	}
	require.NoError(t, m.Start(context.Background()))
	select {
	case err := <-started:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("notification loop did not start without schedules")
	}
	require.NoError(t, m.Stop(context.Background()))
	select {
	case <-m.scheduler.done:
	default:
		t.Fatal("schedule loop did not stop")
	}
	select {
	case <-m.scheduler.notificationsDone:
	default:
		t.Fatal("notification loop did not stop")
	}
}

func scheduleReplica(t *testing.T) *Manager {
	t.Helper()
	// A separate physical connection is essential: one client's single-connection
	// pool would serialize these operations even if the DB fencing were absent.
	db, err := ent.Open("sqlite3", "file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared&_fk=0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return newTestManager(t, db)
}

func writeScheduleKV(ctx context.Context, m *Manager, id int, key string) error {
	return m.client(ctx).PluginStorage.Create().SetInstallationID(id).SetNamespace("schedule-test").SetKey(key).SetValue("written").Exec(ctx)
}

func TestScheduleWriteContextAndAtomicRollback(t *testing.T) {
	m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
	claimed, err := m.scheduler.claim(pluginCtx(), p, row)
	require.NoError(t, err)
	ctx := scheduleTestCall(m.scheduler, p, claimed)
	require.NoError(t, m.withScheduleWrite(ctx, func(txCtx context.Context) error {
		if err := authz.RequireSystemPrincipal(txCtx); err != nil {
			return err
		}
		if ent.TxFromContext(txCtx) == nil || ent.FromContext(txCtx) != m.client(txCtx) {
			return errors.New("missing fenced transaction")
		}
		// Nested fixed host operations must retain the transaction client even
		// though RunAsHost intentionally drops arbitrary caller context values.
		_, err := authz.RunAsHost(txCtx, m.client(txCtx), "schedule-test-nested", func(nested context.Context) (bool, error) {
			return true, writeScheduleKV(nested, m, p.ID, "committed")
		})
		return err
	}))
	stored, err := m.db.PluginStorage.Query().Only(pluginCtx())
	require.NoError(t, err)
	require.Equal(t, "committed", stored.Key)
	installed, err := m.db.Plugin.Get(pluginCtx(), p.ID)
	require.NoError(t, err)
	require.True(t, p.updatedAt.Equal(installed.UpdatedAt), "fencing must not change the installation revision")
	require.NoError(t, validateScheduleCall(ctx))
	failure := errors.New("operation failed")
	require.ErrorIs(t, m.withScheduleWrite(ctx, func(txCtx context.Context) error {
		if err := writeScheduleKV(txCtx, m, p.ID, "rolled-back"); err != nil {
			return err
		}
		return failure
	}), failure)
	count, err := m.db.PluginStorage.Query().Count(pluginCtx())
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestScheduleWriteRejectsStaleLeaseAndLatestRevision(t *testing.T) {
	for _, change := range []string{"token", "revision", "disabled", "deleted"} {
		t.Run(change, func(t *testing.T) {
			m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
			claimed, err := m.scheduler.claim(pluginCtx(), p, row)
			require.NoError(t, err)
			switch change {
			case "token":
				require.NoError(t, m.db.PluginSchedule.UpdateOneID(row.ID).SetLeaseToken("successor").Exec(pluginCtx()))
			case "revision":
				require.NoError(t, m.db.Plugin.UpdateOneID(p.ID).SetConfig(map[string]any{"threshold": 7}).Exec(pluginCtx()))
			case "disabled":
				require.NoError(t, m.db.Plugin.UpdateOneID(p.ID).SetEnabled(false).Exec(pluginCtx()))
			case "deleted":
				require.NoError(t, m.db.Plugin.DeleteOneID(p.ID).Exec(pluginCtx()))
			}
			called := false
			err = m.withScheduleWrite(scheduleTestCall(m.scheduler, p, claimed), func(context.Context) error { called = true; return nil })
			require.ErrorIs(t, err, errScheduleFenced)
			require.False(t, called)
		})
	}
}

func TestScheduleWriteLeaseExpiryRollsBackBeforeTakeover(t *testing.T) {
	m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
	other := scheduleReplica(t)
	claimed, err := m.scheduler.claim(pluginCtx(), p, row)
	require.NoError(t, err)
	now, err := scheduleDBNow(pluginCtx(), m.db)
	require.NoError(t, err)
	until := now.Add(1500 * time.Millisecond)
	require.NoError(t, m.db.PluginSchedule.UpdateOneID(row.ID).SetLeaseUntil(until).Exec(pluginCtx()))
	oldCtx := scheduleTestCall(m.scheduler, p, claimed)
	err = m.withScheduleWrite(oldCtx, func(txCtx context.Context) error {
		if err := writeScheduleKV(txCtx, m, p.ID, "expired"); err != nil {
			return err
		}
		time.Sleep(1600 * time.Millisecond)
		// Even after expiry, another instance cannot claim while this transaction
		// holds the lease row. SQLite may reject contention instead of waiting.
		claimCtx, cancel := context.WithTimeout(pluginCtx(), 100*time.Millisecond)
		defer cancel()
		stolen, claimErr := other.scheduler.claim(claimCtx, other.current().ByID(p.ID), claimed)
		if stolen != nil {
			return errors.New("lease takeover interleaved with a fenced DB write")
		}
		if claimErr != nil && !strings.Contains(claimErr.Error(), "locked") && !errors.Is(claimErr, context.DeadlineExceeded) {
			return claimErr
		}
		return nil
	})
	require.Error(t, err)
	count, err := m.db.PluginStorage.Query().Count(pluginCtx())
	require.NoError(t, err)
	require.Zero(t, count)
	latest, err := m.db.PluginSchedule.Get(pluginCtx(), row.ID)
	require.NoError(t, err)
	successor, err := other.scheduler.claim(pluginCtx(), other.current().ByID(p.ID), latest)
	require.NoError(t, err)
	require.NotNil(t, successor)
	require.Equal(t, claimed.RunID, successor.RunID)
	require.ErrorIs(t, m.withScheduleWrite(oldCtx, func(context.Context) error { t.Error("old worker executed a DB write"); return nil }), errScheduleFenced)
}

func TestScheduleWriteSerializesAcknowledgement(t *testing.T) {
	m, p, row := scheduledPlugin(t, `export function onSchedule(ctx,job) {}`)
	other := scheduleReplica(t)
	claimed, err := m.scheduler.claim(pluginCtx(), p, row)
	require.NoError(t, err)
	// Simulate an old owner acknowledging through a different DB connection.
	other.scheduler.owner = m.scheduler.owner
	require.NoError(t, m.withScheduleWrite(scheduleTestCall(m.scheduler, p, claimed), func(txCtx context.Context) error {
		ackCtx, cancel := context.WithTimeout(pluginCtx(), 100*time.Millisecond)
		defer cancel()
		if err := other.scheduler.ack(ackCtx, other.current().ByID(p.ID), claimed, nil); err == nil {
			return errors.New("ack interleaved with a fenced transaction")
		}
		return writeScheduleKV(txCtx, m, p.ID, "before-ack")
	}))
	stored, err := m.db.PluginSchedule.Get(pluginCtx(), row.ID)
	require.NoError(t, err)
	require.Equal(t, claimed.LeaseToken, stored.LeaseToken)
	require.NoError(t, other.scheduler.ack(pluginCtx(), other.current().ByID(p.ID), claimed, nil))
	stored, err = m.db.PluginSchedule.Get(pluginCtx(), row.ID)
	require.NoError(t, err)
	require.Empty(t, stored.LeaseToken)
}

func TestScheduleWriteNonSchedulePassthrough(t *testing.T) {
	type marker struct{}
	ctx := context.WithValue(context.Background(), marker{}, true)
	m := &Manager{}
	failure := errors.New("ordinary operation error")
	require.ErrorIs(t, m.withScheduleWrite(ctx, func(received context.Context) error {
		require.Same(t, ctx, received)
		return failure
	}), failure)
}
