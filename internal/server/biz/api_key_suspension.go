// 职责: 创建、查询和解除独立插件暂停，并提供不经缓存的鉴权门禁。
// 关联文件:
//
//	internal/server/biz/database_clock.go
//	internal/objects/key_suspension.go
//	internal/server/biz/plugin_automation_access.go
//	internal/server/biz/auth.go
//	internal/server/biz/plugin_notification.go
//
// 遵循的 Rules:
//
//	.agents/rules/biz-services.md
//	.agents/rules/cache-compat.md
//
// 注意事项:
//
//	暂停记录永不修改 API Key 人工状态，过期只由 SQL 时间谓词判定。
//	幂等冲突仅返回原记录，不更新过期时间或解除状态；卸载不移除暂停。
//	期限和审计统一使用数据库 UTC，Retry-After 使用同次鉴权语句的 DB 观察时间。
package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/apikeysuspension"
	"github.com/looplj/axonhub/internal/ent/plugin"
	"github.com/looplj/axonhub/internal/ent/predicate"
	"github.com/looplj/axonhub/internal/ent/privacy"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/scopes"
)

var (
	ErrAPIKeyGateUnavailable         = errors.New("API key authorization gate unavailable")
	ErrSuspensionIdempotencyConflict = errors.New("idempotency key belongs to a different suspension request")
)

// APIKeySuspendedError contains no evidence, credential, or plugin-supplied reason.
// Until is nil if any active hold is permanent; otherwise it is the latest expiry.
type APIKeySuspendedError struct {
	Until      *time.Time
	observedAt time.Time
}

func (e *APIKeySuspendedError) Error() string { return "API key is suspended" }

// RetryAfter 保留旧签名；真实鉴权错误使用同次 SQL 的 DB 观察时间。
// 仅手工构造的错误使用调用者时间，以保持既有调用兼容。
func (e *APIKeySuspendedError) RetryAfter(now time.Time) int {
	if e.Until == nil {
		return 0
	}
	if !e.observedAt.IsZero() {
		now = e.observedAt
	}
	seconds := int(e.Until.Sub(now).Seconds())
	if now.Add(time.Duration(seconds) * time.Second).Before(*e.Until) {
		seconds++
	}
	if seconds < 1 {
		return 1
	}
	return seconds
}

type APIKeySuspensionService struct{ *AbstractService }

type APIKeySuspensionInput = objects.APIKeySuspensionInput
type APIKeySuspensionFilter = objects.APIKeySuspensionFilter
type APIKeySuspensionResult = objects.APIKeySuspensionResult

func suspensionResult(row *ent.APIKeySuspension) *APIKeySuspensionResult {
	return &APIKeySuspensionResult{ID: row.ID, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
		InstallationID: row.InstallationID, PluginID: row.PluginID, APIKeyID: row.APIKeyID, ProjectID: row.ProjectID,
		Reason: row.Reason, Evidence: row.Evidence, CreatedBy: row.CreatedBy, ExpiresAt: row.ExpiresAt,
		ReleasedAt: row.ReleasedAt, ReleasedBy: row.ReleasedBy, ReleaseReason: row.ReleaseReason}
}

func suspensionResults(rows []*ent.APIKeySuspension) []*APIKeySuspensionResult {
	result := make([]*APIKeySuspensionResult, 0, len(rows))
	for _, row := range rows {
		result = append(result, suspensionResult(row))
	}
	return result
}

func NewAPIKeySuspensionService(client *ent.Client) *APIKeySuspensionService {
	return &APIKeySuspensionService{AbstractService: &AbstractService{db: client}}
}

func suspensionActor(ctx context.Context, installationID int) string {
	p, _ := authz.GetPrincipal(ctx)
	if p.IsSystem() {
		return fmt.Sprintf("system:plugin:%d", installationID)
	}
	return p.String()
}

func normalizeSuspensionInput(input objects.APIKeySuspensionInput) (objects.APIKeySuspensionInput, error) {
	if input.APIKeyID <= 0 || input.ProjectID <= 0 {
		return input, fmt.Errorf("positive API key and project IDs are required")
	}
	if len(input.IdempotencyKey) == 0 || len(input.IdempotencyKey) > objects.MaxKeySuspensionIdempotencyBytes || !utf8.ValidString(input.IdempotencyKey) {
		return input, fmt.Errorf("idempotency key must be valid UTF-8 and 1-%d bytes", objects.MaxKeySuspensionIdempotencyBytes)
	}
	if strings.TrimSpace(input.Reason) == "" || len(input.Reason) > objects.MaxKeySuspensionReasonBytes || !utf8.ValidString(input.Reason) {
		return input, fmt.Errorf("reason must be nonempty UTF-8 and at most %d bytes", objects.MaxKeySuspensionReasonBytes)
	}
	maxSeconds := int(objects.MaxKeySuspensionDuration / time.Second)
	if (input.Permanent && input.DurationSeconds != 0) || (!input.Permanent && (input.DurationSeconds <= 0 || input.DurationSeconds > maxSeconds)) {
		return input, fmt.Errorf("choose permanent or a duration between 1 and %d seconds", maxSeconds)
	}
	if input.Evidence == nil {
		input.Evidence = map[string]any{}
	}
	encoded, err := json.Marshal(input.Evidence)
	if err != nil || len(encoded) > objects.MaxKeySuspensionEvidenceBytes {
		return input, fmt.Errorf("evidence must be JSON of at most %d bytes", objects.MaxKeySuspensionEvidenceBytes)
	}
	// Detach nested maps and slices so mutation of the input cannot change the audit.
	if err := json.Unmarshal(encoded, &input.Evidence); err != nil {
		return input, fmt.Errorf("invalid evidence JSON: %w", err)
	}
	return input, nil
}

// lockSuspensionTargets serializes grants/revocation and key changes on SQL engines
// that support row locking. SQLite serializes writers; busy retries restart the
// whole transaction, never just the insert after an authorization snapshot.
func lockSuspensionTargets(ctx context.Context, client *ent.Client, installationID, keyID int) error {
	if client.Driver().Dialect() == dialect.SQLite {
		return nil
	}
	var ids []int
	if installationID > 0 {
		if err := client.Plugin.Query().Where(plugin.IDEQ(installationID)).Modify(func(s *sql.Selector) {
			s.Select(s.C(plugin.FieldID)).ForUpdate()
		}).Scan(ctx, &ids); err != nil {
			return err
		}
	}
	if keyID > 0 {
		return client.APIKey.Query().Where(apikey.IDEQ(keyID)).Modify(func(s *sql.Selector) {
			s.Select(s.C(apikey.FieldID)).ForUpdate()
		}).Scan(ctx, &ids)
	}
	return nil
}

func (s *APIKeySuspensionService) suspensionTransaction(ctx context.Context, fn func(context.Context) error) error {
	client := s.entFromContext(ctx)
	// Fenced host operations may carry only tx.Client() in ent.FromContext.
	// That client implements dialect.Tx even without an ent.Tx context value.
	// Never retry part of an externally owned transaction or commit its writes.
	_, clientIsTransaction := client.Driver().(dialect.Tx)
	for attempt := 0; ; attempt++ {
		err := s.RunInTransaction(ctx, fn)
		if err == nil || attempt >= 5 || clientIsTransaction || ent.TxFromContext(ctx) != nil || client.Driver().Dialect() != dialect.SQLite ||
			(!strings.Contains(err.Error(), "database is locked") && !strings.Contains(err.Error(), "database table is locked")) {
			return err
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func automationTarget(ctx context.Context, client *ent.Client, installed *ent.Plugin, keyID, projectID int) (*ent.APIKey, error) {
	key, err := client.APIKey.Query().Where(apikey.IDEQ(keyID), apikey.ProjectIDEQ(projectID), apikey.DeletedAtEQ(0)).
		Select(apikey.FieldID, apikey.FieldProjectID, apikey.FieldType, apikey.FieldDeletedAt).Only(ctx)
	if err != nil {
		return nil, fmt.Errorf("current API key target unavailable: %w", err)
	}
	// Personal keys are deliberately excluded, even for host automation. A host
	// principal is not evidence that the installation represents the creator.
	if key.Type == apikey.TypeNoauth || key.Type == apikey.TypePersonal {
		return nil, privacy.Denyf("this API key type cannot be targeted by automation")
	}
	if !installed.AutomationScope.Allows(key.ProjectID, key.ID) {
		return nil, privacy.Denyf("API key is outside the current automation scope")
	}
	return key, nil
}

func (s *APIKeySuspensionService) Apply(ctx context.Context, installationID int, input objects.APIKeySuspensionInput) (*APIKeySuspensionResult, error) {
	input, err := normalizeSuspensionInput(input)
	if err != nil {
		return nil, err
	}
	client := s.entFromContext(ctx)
	if _, err := AuthorizePluginAutomation(ctx, client, installationID, objects.PluginPermissionKeySuspensions); err != nil {
		return nil, err
	}
	actor := suspensionActor(ctx, installationID)
	hash := sha256.Sum256([]byte(input.IdempotencyKey))
	idempotencyHash := hex.EncodeToString(hash[:])
	var result *ent.APIKeySuspension
	err = s.suspensionTransaction(ctx, func(txCtx context.Context) error {
		txClient := s.entFromContext(txCtx)
		_, err := authz.RunAsHost(txCtx, txClient, "apply API key suspension", func(hostCtx context.Context) (struct{}, error) {
			if err := lockSuspensionTargets(hostCtx, txClient, installationID, input.APIKeyID); err != nil {
				return struct{}{}, err
			}
			// Re-check authorization inside the same transaction and after row locks.
			installed, err := AuthorizePluginAutomation(txCtx, txClient, installationID, objects.PluginPermissionKeySuspensions)
			if err != nil {
				return struct{}{}, err
			}
			if _, err := automationTarget(hostCtx, txClient, installed, input.APIKeyID, input.ProjectID); err != nil {
				return struct{}{}, err
			}
			existing, err := txClient.APIKeySuspension.Query().Where(apikeysuspension.InstallationIDEQ(installationID), apikeysuspension.IdempotencyKeyHashEQ(idempotencyHash)).Only(hostCtx)
			if err == nil {
				if existing.APIKeyID != input.APIKeyID || existing.ProjectID != input.ProjectID {
					return struct{}{}, ErrSuspensionIdempotencyConflict
				}
				result = existing
				return struct{}{}, nil // Replay never enqueues a second notification.
			}
			if !ent.IsNotFound(err) {
				return struct{}{}, err
			}
			now, err := databaseUTCNow(hostCtx, txClient)
			if err != nil {
				return struct{}{}, err
			}
			create := txClient.APIKeySuspension.Create().SetInstallationID(installationID).SetPluginID(installed.PluginID).
				SetAPIKeyID(input.APIKeyID).SetProjectID(input.ProjectID).SetIdempotencyKeyHash(idempotencyHash).
				SetReason(input.Reason).SetEvidence(input.Evidence).SetCreatedBy(actor).SetCreatedAt(now).SetUpdatedAt(now)
			if !input.Permanent {
				create.SetExpiresAt(now.Add(time.Duration(input.DurationSeconds) * time.Second))
			}
			// Conflict does not update any business field, in particular released_at
			// and expires_at. Database uniqueness, not a pre-read, owns idempotency.
			if err := create.OnConflict(sql.ConflictColumns(apikeysuspension.FieldInstallationID, apikeysuspension.FieldIdempotencyKeyHash), sql.ResolveWithIgnore()).Exec(hostCtx); err != nil {
				return struct{}{}, err
			}
			result, err = txClient.APIKeySuspension.Query().Where(apikeysuspension.InstallationIDEQ(installationID), apikeysuspension.IdempotencyKeyHashEQ(idempotencyHash)).Only(hostCtx)
			if err != nil {
				return struct{}{}, err
			}
			if result.APIKeyID != input.APIKeyID || result.ProjectID != input.ProjectID {
				return struct{}{}, ErrSuspensionIdempotencyConflict
			}
			if input.Notification != nil {
				notification := *input.Notification
				// Bind outbox identity and evidence to the durable hold, not to
				// caller-supplied cross-hold IDs. Enqueue validates the live webhook
				// grant/target in this transaction; any error rolls back the hold.
				notification.IdempotencyKey = "api-key-suspension:" + strconv.Itoa(result.ID)
				notification.SuspensionID = &result.ID
				notification.Evidence = result.Evidence
				if _, err := EnqueuePluginNotification(txCtx, txClient, installationID, notification); err != nil {
					return struct{}{}, err
				}
			}
			return struct{}{}, nil
		})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("apply API key suspension: %w", err)
	}
	return suspensionResult(result), nil
}

// ReleaseOwn can release only this installation's record and still requires live
// installation permission/scope. Administrators use AdminRelease after uninstall.
func (s *APIKeySuspensionService) ReleaseOwn(ctx context.Context, installationID, id int) (*APIKeySuspensionResult, error) {
	client := s.entFromContext(ctx)
	if _, err := AuthorizePluginAutomation(ctx, client, installationID, objects.PluginPermissionKeySuspensions); err != nil {
		return nil, err
	}
	actor := suspensionActor(ctx, installationID)
	var result *ent.APIKeySuspension
	err := s.suspensionTransaction(ctx, func(txCtx context.Context) error {
		txClient := s.entFromContext(txCtx)
		_, err := authz.RunAsHost(txCtx, txClient, "release own API key suspension", func(hostCtx context.Context) (struct{}, error) {
			if err := lockSuspensionTargets(hostCtx, txClient, installationID, 0); err != nil {
				return struct{}{}, err
			}
			installed, err := AuthorizePluginAutomation(txCtx, txClient, installationID, objects.PluginPermissionKeySuspensions)
			if err != nil {
				return struct{}{}, err
			}
			result, err = txClient.APIKeySuspension.Query().Where(apikeysuspension.IDEQ(id), apikeysuspension.InstallationIDEQ(installationID)).Only(hostCtx)
			if err != nil {
				return struct{}{}, err
			}
			if err := lockSuspensionTargets(hostCtx, txClient, 0, result.APIKeyID); err != nil {
				return struct{}{}, err
			}
			if _, err := automationTarget(hostCtx, txClient, installed, result.APIKeyID, result.ProjectID); err != nil {
				return struct{}{}, err
			}
			result, err = releaseSuspension(hostCtx, txClient, result, actor, "released by originating installation")
			return struct{}{}, err
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return suspensionResult(result), nil
}

// releaseSuspension is compare-and-set: a racing release cannot replace the
// first actor, reason, or timestamp, and it never touches another hold or the key.
func releaseSuspension(ctx context.Context, client *ent.Client, existing *ent.APIKeySuspension, actor, reason string) (*ent.APIKeySuspension, error) {
	if existing.ReleasedAt != nil {
		return existing, nil
	}
	now, err := databaseUTCNow(ctx, client)
	if err != nil {
		return nil, err
	}
	_, err = client.APIKeySuspension.Update().Where(apikeysuspension.IDEQ(existing.ID), apikeysuspension.InstallationIDEQ(existing.InstallationID), apikeysuspension.ProjectIDEQ(existing.ProjectID), apikeysuspension.ReleasedAtIsNil()).
		SetReleasedAt(now).SetUpdatedAt(now).SetReleasedBy(actor).SetReleaseReason(reason).Save(ctx)
	if err != nil {
		return nil, err
	}
	q := client.APIKeySuspension.Query().Where(apikeysuspension.IDEQ(existing.ID))
	if client.Driver().Dialect() != dialect.SQLite {
		// A locking read sees the winning CAS even under MySQL repeatable-read;
		// a normal snapshot read could still return the pre-release audit row.
		q.Modify(func(s *sql.Selector) { s.ForUpdate() })
	}
	return q.Only(ctx)
}

func activeSuspensions() predicate.APIKeySuspension {
	return func(s *sql.Selector) {
		expiry := s.C(apikeysuspension.FieldExpiresAt)
		converted := databaseUTCTimestampSQL(s.Dialect(), expiry)
		s.Where(sql.And(sql.IsNull(s.C(apikeysuspension.FieldReleasedAt)), sql.Or(
			sql.IsNull(expiry), sql.ExprP(converted+" > "+databaseUTCNowSQL(s.Dialect())),
			// An unreadable non-NULL timestamp is not permission to resume.
			sql.ExprP(converted+" IS NULL"),
		)))
	}
}

func suspensionFilterQuery(client *ent.Client, filter objects.APIKeySuspensionFilter) *ent.APIKeySuspensionQuery {
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	q := client.APIKeySuspension.Query().Order(ent.Desc(apikeysuspension.FieldID)).Limit(limit)
	if filter.ProjectID > 0 {
		q.Where(apikeysuspension.ProjectIDEQ(filter.ProjectID))
	}
	if filter.APIKeyID > 0 {
		q.Where(apikeysuspension.APIKeyIDEQ(filter.APIKeyID))
	}
	if filter.BeforeID > 0 {
		q.Where(apikeysuspension.IDLT(filter.BeforeID))
	}
	if filter.ActiveOnly {
		q.Where(activeSuspensions())
	}
	return q
}

func (s *APIKeySuspensionService) List(ctx context.Context, installationID int, filter objects.APIKeySuspensionFilter) ([]*APIKeySuspensionResult, error) {
	client := s.entFromContext(ctx)
	installed, err := AuthorizePluginAutomation(ctx, client, installationID, objects.PluginPermissionKeySuspensions)
	if err != nil {
		return nil, err
	}
	rows, err := authz.RunAsHost(ctx, client, "list own API key suspensions", func(hostCtx context.Context) ([]*ent.APIKeySuspension, error) {
		// SQL limits apply after the ceiling; do not load foreign evidence then
		// filter it in the caller or let a broad installation ID act as authority.
		if len(installed.AutomationScope.Projects) == 0 {
			return []*ent.APIKeySuspension{}, nil
		}
		return suspensionFilterQuery(client, filter).Where(apikeysuspension.InstallationIDEQ(installationID), func(s *sql.Selector) {
			key := sql.Table(apikey.Table)
			allowed := make([]*sql.Predicate, 0, len(installed.AutomationScope.Projects))
			for _, p := range installed.AutomationScope.Projects {
				project := sql.EQ(key.C(apikey.FieldProjectID), p.ProjectID)
				if p.AllKeys {
					if len(p.ExcludedAPIKeyIDs) > 0 {
						project = sql.And(project, sql.Not(sql.InInts(key.C(apikey.FieldID), p.ExcludedAPIKeyIDs...)))
					}
					allowed = append(allowed, project)
				} else if len(p.APIKeyIDs) > 0 {
					allowed = append(allowed, sql.And(project, sql.InInts(key.C(apikey.FieldID), p.APIKeyIDs...)))
				}
			}
			if len(allowed) == 0 {
				s.Where(sql.False())
				return
			}
			s.Where(sql.Exists(sql.Select(key.C(apikey.FieldID)).From(key).Where(sql.And(
				sql.ColumnsEQ(key.C(apikey.FieldID), s.C(apikeysuspension.FieldAPIKeyID)),
				sql.ColumnsEQ(key.C(apikey.FieldProjectID), s.C(apikeysuspension.FieldProjectID)),
				sql.EQ(key.C(apikey.FieldDeletedAt), 0),
				sql.NotIn(key.C(apikey.FieldType), string(apikey.TypeNoauth), string(apikey.TypePersonal)),
				sql.Or(allowed...),
			))))
		}).All(hostCtx)
	})
	if err != nil {
		return nil, err
	}
	return suspensionResults(rows), nil
}

func requireSuspensionAdmin(ctx context.Context, projectID int, scope scopes.ScopeSlug) error {
	if err := authz.RequireDirectUser(ctx); err != nil {
		return err
	}
	u, _ := contexts.GetUser(ctx)
	if scopes.IsDirectOwner(ctx) || scopes.EffectiveSystemScope(ctx, u, scope) {
		return nil
	}
	currentProject, ok := contexts.GetProjectID(ctx)
	if projectID <= 0 || !ok || currentProject != projectID || !scopes.EffectiveProjectScope(ctx, u, projectID, scope) {
		return privacy.Denyf("API key suspension management is outside the authorized project")
	}
	return nil
}

func (s *APIKeySuspensionService) AdminRelease(ctx context.Context, id int, reason string) (*APIKeySuspensionResult, error) {
	if err := authz.RequireDirectUser(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(reason) == "" || !utf8.ValidString(reason) || len(reason) > objects.MaxKeySuspensionReasonBytes {
		return nil, fmt.Errorf("a bounded UTF-8 release reason is required")
	}
	p, _ := authz.GetPrincipal(ctx)
	var result *ent.APIKeySuspension
	err := s.suspensionTransaction(ctx, func(txCtx context.Context) error {
		client := s.entFromContext(txCtx)
		_, err := authz.RunAsHost(txCtx, client, "admin release API key suspension", func(hostCtx context.Context) (struct{}, error) {
			var err error
			result, err = client.APIKeySuspension.Get(hostCtx, id)
			if err != nil {
				return struct{}{}, err
			}
			if err := requireSuspensionAdmin(txCtx, result.ProjectID, scopes.ScopeWriteAPIKeys); err != nil {
				return struct{}{}, err
			}
			if !scopes.IsDirectOwner(txCtx) {
				if err := lockSuspensionTargets(hostCtx, client, 0, result.APIKeyID); err != nil {
					return struct{}{}, err
				}
				key, err := client.APIKey.Query().Where(apikey.IDEQ(result.APIKeyID), apikey.ProjectIDEQ(result.ProjectID)).Only(hostCtx)
				if err != nil {
					return struct{}{}, err
				}
				u, _ := contexts.GetUser(txCtx)
				if key.Type == apikey.TypePersonal && key.UserID != u.ID {
					return struct{}{}, privacy.Denyf("personal API key belongs to another user")
				}
			}
			result, err = releaseSuspension(hostCtx, client, result, p.String(), reason)
			return struct{}{}, err
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return suspensionResult(result), nil
}

func (s *APIKeySuspensionService) AdminList(ctx context.Context, filter objects.APIKeySuspensionFilter) ([]*APIKeySuspensionResult, error) {
	if err := requireSuspensionAdmin(ctx, filter.ProjectID, scopes.ScopeReadAPIKeys); err != nil {
		return nil, err
	}
	client := s.entFromContext(ctx)
	rows, err := authz.RunAsHost(ctx, client, "admin list API key suspensions", func(hostCtx context.Context) ([]*ent.APIKeySuspension, error) {
		q := suspensionFilterQuery(client, filter)
		if !scopes.IsDirectOwner(ctx) {
			u, _ := contexts.GetUser(ctx)
			q.Where(func(s *sql.Selector) {
				key := sql.Table(apikey.Table)
				s.Where(sql.Exists(sql.Select(key.C(apikey.FieldID)).From(key).Where(sql.And(
					sql.ColumnsEQ(key.C(apikey.FieldID), s.C(apikeysuspension.FieldAPIKeyID)),
					sql.ColumnsEQ(key.C(apikey.FieldProjectID), s.C(apikeysuspension.FieldProjectID)),
					sql.EQ(key.C(apikey.FieldDeletedAt), 0),
					sql.Or(sql.NEQ(key.C(apikey.FieldType), string(apikey.TypePersonal)), sql.EQ(key.C(apikey.FieldUserID), u.ID)),
				))))
			})
		}
		return q.All(hostCtx)
	})
	if err != nil {
		return nil, err
	}
	return suspensionResults(rows), nil
}

// Check performs a SQL existence probe on every authentication, including cache
// hits. No timer or negative cache can race revocation; requests already admitted
// before a hold commits are intentionally not interrupted.
func (s *APIKeySuspensionService) Check(ctx context.Context, keyID, projectID int) error {
	p, ok := authz.GetPrincipal(ctx)
	if !ok || !p.IsSystem() {
		return privacy.Denyf("suspension gate requires host identity")
	}
	client := s.entFromContext(ctx)
	_, err := authz.RunAsHost(ctx, client, "API key suspension authentication gate", func(hostCtx context.Context) (struct{}, error) {
		var observations []struct {
			ExpiresAt     *time.Time `json:"expires_at"`
			ObservedEpoch float64    `json:"observed_epoch"`
		}
		err := client.APIKeySuspension.Query().Unique(false).Where(apikeysuspension.APIKeyIDEQ(keyID), apikeysuspension.ProjectIDEQ(projectID), activeSuspensions()).Limit(1).
			Select(apikeysuspension.FieldExpiresAt).Order(func(s *sql.Selector) {
			// Explicit NULL ordering is identical on PostgreSQL, MySQL and SQLite:
			// permanent first, then the longest temporary hold, in one snapshot.
			s.OrderExpr(sql.Expr("CASE WHEN " + s.C(apikeysuspension.FieldExpiresAt) + " IS NULL THEN 0 ELSE 1 END"))
			s.OrderExpr(sql.Expr(databaseUTCTimestampSQL(s.Dialect(), s.C(apikeysuspension.FieldExpiresAt)) + " DESC"))
		}).Modify(func(s *sql.Selector) {
			s.AppendSelect(sql.As(databaseUTCEpochSQL(s.Dialect()), "observed_epoch"))
		}).Scan(hostCtx, &observations)
		if err != nil {
			return struct{}{}, fmt.Errorf("%w: %w", ErrAPIKeyGateUnavailable, err)
		}
		if len(observations) == 0 {
			return struct{}{}, nil
		}
		observedAt, err := databaseUTCFromEpoch(observations[0].ObservedEpoch)
		if err != nil {
			return struct{}{}, fmt.Errorf("%w: %w", ErrAPIKeyGateUnavailable, err)
		}
		return struct{}{}, &APIKeySuspendedError{Until: observations[0].ExpiresAt, observedAt: observedAt}
	})
	if err != nil {
		var suspended *APIKeySuspendedError
		if !errors.As(err, &suspended) && !errors.Is(err, ErrAPIKeyGateUnavailable) {
			return fmt.Errorf("%w: %w", ErrAPIKeyGateUnavailable, err)
		}
	}
	return err
}
