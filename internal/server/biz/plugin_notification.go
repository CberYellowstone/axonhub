// 职责: 入队插件通知并以有限重试和租约围栏投递持久化 outbox。
// 关联文件:
//
//	internal/ent/schema/plugin_notification.go
//	internal/server/biz/webhook_notifier.go
//	internal/server/biz/plugin_automation_access.go
//	internal/objects/key_suspension.go
//
// 注意事项: 至少一次投递，接收端应按 eventId 去重；不得在事务内调用 Drain。
package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/google/uuid"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikeysuspension"
	"github.com/looplj/axonhub/internal/ent/pluginnotification"
	"github.com/looplj/axonhub/internal/ent/privacy"
	"github.com/looplj/axonhub/internal/ent/system"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/events"
)

const (
	pluginNotificationBodyLimit    = 64 * 1024
	pluginNotificationMaxAttempts  = 6
	pluginNotificationLeaseSeconds = int64(120)
)

var errPluginNotificationBody = errors.New("invalid or oversized plugin notification body")

// PluginNotificationInput accepts only a configured target name, never transport credentials.
type PluginNotificationInput = objects.PluginNotificationInput

// EnqueuePluginNotification uses the caller's transaction client when present.
// The first enqueue wins for an installation/key pair, including after delivery.
func EnqueuePluginNotification(ctx context.Context, client *ent.Client, installationID int, input PluginNotificationInput) (*ent.PluginNotification, error) {
	if txClient := ent.FromContext(ctx); txClient != nil {
		client = txClient
	}
	installed, err := AuthorizePluginAutomation(ctx, client, installationID, "webhook")
	if err != nil {
		return nil, err
	}
	if input.TargetName == "" || len(input.TargetName) > 128 || !utf8.ValidString(input.TargetName) || strings.TrimSpace(input.IdempotencyKey) == "" || len(input.IdempotencyKey) > 256 || !utf8.ValidString(input.IdempotencyKey) {
		return nil, errors.New("invalid plugin notification target or idempotency key")
	}
	if input.Payload == nil {
		return nil, errors.New("plugin notification payload must be a JSON object")
	}
	if input.SuspensionID != nil && *input.SuspensionID <= 0 {
		return nil, errors.New("invalid suspension ID")
	}
	// Round-trip owns the JSON tree and rejects non-JSON numbers/cycles before persistence.
	encoded, err := json.Marshal(input.Payload)
	if err != nil || len(encoded) > pluginNotificationBodyLimit {
		return nil, errors.New("invalid or oversized plugin notification payload")
	}
	var payload map[string]any
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return nil, errors.New("invalid plugin notification payload")
	}
	sum := sha256.Sum256([]byte(input.IdempotencyKey))
	hash := hex.EncodeToString(sum[:])
	return authz.RunAsHost(ctx, client, "plugin-notification-enqueue", func(hostCtx context.Context) (*ent.PluginNotification, error) {
		if err := pluginNotificationSuspension(hostCtx, client, installed, input.SuspensionID); err != nil {
			return nil, err
		}
		existing, err := client.PluginNotification.Query().Where(pluginnotification.InstallationIDEQ(installationID), pluginnotification.IdempotencyHashEQ(hash)).Only(hostCtx)
		if err == nil {
			return existing, nil
		}
		if !ent.IsNotFound(err) {
			return nil, err
		}
		row := &ent.PluginNotification{InstallationID: installationID, IdempotencyKey: input.IdempotencyKey, EventID: uuid.NewString(), Payload: payload, SuspensionID: input.SuspensionID, Evidence: input.Evidence, CreatedAt: time.Now()}
		if _, err := pluginNotificationTarget(hostCtx, client, input.TargetName, row); err != nil {
			return nil, err
		}
		err = client.PluginNotification.Create().SetInstallationID(installationID).SetIdempotencyHash(hash).SetIdempotencyKey(input.IdempotencyKey).SetEventID(row.EventID).SetTargetName(input.TargetName).SetPayload(payload).SetNillableSuspensionID(input.SuspensionID).SetEvidence(input.Evidence).
			OnConflict(sql.ConflictColumns(pluginnotification.FieldInstallationID, pluginnotification.FieldIdempotencyHash)).Ignore().Exec(hostCtx)
		if err != nil {
			return nil, fmt.Errorf("enqueue plugin notification: %w", err)
		}
		return client.PluginNotification.Query().Where(pluginnotification.InstallationIDEQ(installationID), pluginnotification.IdempotencyHashEQ(hash)).Only(hostCtx)
	})
}

func pluginNotificationSuspension(ctx context.Context, client *ent.Client, installed *ent.Plugin, suspensionID *int) error {
	if suspensionID == nil {
		return nil
	}
	hold, err := client.APIKeySuspension.Query().Where(apikeysuspension.IDEQ(*suspensionID), apikeysuspension.InstallationIDEQ(installed.ID)).Only(ctx)
	if err != nil {
		return fmt.Errorf("notification suspension unavailable: %w", err)
	}
	_, err = automationTarget(ctx, client, installed, hold.APIKeyID, hold.ProjectID)
	return err
}

func pluginNotificationData(row *ent.PluginNotification) map[string]any {
	data := make(map[string]any, len(row.Payload)+5)
	for key, value := range row.Payload {
		switch key {
		case "eventId", "idempotencyKey", "installationId", "suspensionId", "evidence":
			continue
		}
		data[key] = value
	}
	data["eventId"], data["idempotencyKey"], data["installationId"] = row.EventID, row.IdempotencyKey, row.InstallationID
	if row.SuspensionID != nil {
		data["suspensionId"] = *row.SuspensionID
	}
	if row.Evidence != nil {
		data["evidence"] = row.Evidence
	}
	return data
}

// pluginNotificationTarget reads uncached settings so removed/disabled targets fail closed.
func pluginNotificationTarget(ctx context.Context, client *ent.Client, name string, row *ent.PluginNotification) (WebhookTarget, error) {
	setting, err := client.System.Query().Where(system.KeyEQ(SystemKeyWebhookNotifierConfig)).Only(ctx)
	if ent.IsNotFound(err) {
		return WebhookTarget{}, ErrWebhookTargetNotFound
	}
	if err != nil {
		return WebhookTarget{}, err
	}
	var cfg WebhookNotifierConfig
	if err := json.Unmarshal([]byte(setting.Value), &cfg); err != nil {
		return WebhookTarget{}, errors.New("invalid webhook configuration")
	}
	target, found := webhookTargetByName(&cfg, name)
	if !found {
		return WebhookTarget{}, ErrWebhookTargetNotFound
	}
	if !target.Enabled || strings.TrimSpace(target.URL) == "" {
		return WebhookTarget{}, ErrWebhookTargetDisabled
	}
	data := pluginNotificationData(row)
	encoded, err := json.Marshal(data)
	if err != nil || len(encoded) > pluginNotificationBodyLimit {
		return WebhookTarget{}, errPluginNotificationBody
	}
	renderCtx := WebhookRenderContext{Event: "plugin.notification", Severity: events.SeverityInfo, Data: data, OccurredAt: row.CreatedAt.UTC().Format(time.RFC3339)}
	if message, ok := data["message"].(string); ok {
		renderCtx.Message = message
	}
	body, err := renderWebhookTemplate(target.Body, renderCtx)
	if err != nil || !utf8.ValidString(body) || len(body) > pluginNotificationBodyLimit {
		return WebhookTarget{}, errPluginNotificationBody
	}
	return target, nil
}

func pluginNotificationClock(d string) string {
	switch d {
	case dialect.Postgres:
		return "CAST(EXTRACT(EPOCH FROM clock_timestamp()) AS BIGINT)"
	case dialect.MySQL:
		return "UNIX_TIMESTAMP()"
	default:
		return "CAST(strftime('%s','now') AS INTEGER)"
	}
}

// The target is the same uncached snapshot that passed the live enabled check.
func pluginNotificationSend(ctx context.Context, webhooks *WebhookNotifier, target WebhookTarget, row *ent.PluginNotification) error {
	data := pluginNotificationData(row)
	renderCtx := WebhookRenderContext{Event: "plugin.notification", Severity: events.SeverityInfo, Data: data, OccurredAt: row.CreatedAt.In(webhooks.SystemService.TimeLocation(ctx)).Format(time.RFC3339)}
	if message, ok := data["message"].(string); ok {
		renderCtx.Message = message
	}
	body, err := renderWebhookTemplate(target.Body, renderCtx)
	if err != nil || !utf8.ValidString(body) || len(body) > pluginNotificationBodyLimit {
		return errPluginNotificationBody
	}
	headers, err := renderWebhookHeaders(target.Headers, renderCtx)
	if err != nil {
		return ErrWebhookUnavailable
	}
	return webhooks.sendOnce(ctx, target, body, headers)
}

func pluginNotificationDue(s *sql.Selector) {
	clock := pluginNotificationClock(s.Dialect())
	s.Where(sql.ExprP(s.C(pluginnotification.FieldNextAttemptAt) + " <= " + clock))
	s.Where(sql.ExprP(s.C(pluginnotification.FieldLeaseUntil) + " <= " + clock))
	s.Where(sql.EQ(s.C(pluginnotification.FieldStatus), "pending"))
}

func pluginNotificationNow(ctx context.Context, client *ent.Client, id int) (int64, error) {
	var rows []struct {
		Now int64 `json:"now"`
	}
	err := client.PluginNotification.Query().Where(pluginnotification.IDEQ(id)).Aggregate(func(s *sql.Selector) string { return sql.As(pluginNotificationClock(s.Dialect()), "now") }).Scan(ctx, &rows)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, errors.New("notification disappeared")
	}
	return rows[0].Now, nil
}

// DeliverPluginNotifications drains bounded work without a resident goroutine.
// Pass a root client, not a transaction client: HTTP never belongs in a DB transaction.
// Delivery failures are recorded, not returned; DB failures are returned to the scheduler.
func DeliverPluginNotifications(ctx context.Context, client *ent.Client, webhooks *WebhookNotifier, limit int) (int, error) {
	if err := authz.RequireSystemPrincipal(ctx); err != nil {
		return 0, err
	}
	if limit <= 0 {
		limit = 25
	}
	if limit > 100 {
		limit = 100
	}
	if ent.TxFromContext(ctx) != nil || client.IsTransactional() || (ent.FromContext(ctx) != nil && ent.FromContext(ctx).IsTransactional()) {
		return 0, errors.New("plugin notification delivery requires a non-transactional context")
	}
	return authz.RunAsHost(ctx, client, "plugin-notification-drain", func(hostCtx context.Context) (int, error) {
		rows, err := client.PluginNotification.Query().Where(pluginNotificationDue).Order(ent.Asc(pluginnotification.FieldNextAttemptAt), ent.Asc(pluginnotification.FieldID)).Limit(limit).All(hostCtx)
		if err != nil {
			return 0, err
		}
		delivered := 0
		for _, row := range rows {
			if err := hostCtx.Err(); err != nil {
				return delivered, err
			}
			now, err := pluginNotificationNow(hostCtx, client, row.ID)
			if err != nil {
				return delivered, err
			}
			token := uuid.NewString()
			count, err := client.PluginNotification.Update().Where(pluginnotification.IDEQ(row.ID), pluginNotificationDue, func(s *sql.Selector) { s.Where(sql.ExprP(pluginNotificationClock(s.Dialect())+" <= ?", now+5)) }).SetLeaseToken(token).SetLeaseUntil(now + pluginNotificationLeaseSeconds).AddAttempts(1).Save(hostCtx)
			if err != nil {
				return delivered, err
			}
			if count == 0 {
				continue
			}
			// Another drainer may have advanced attempts since the initial due scan.
			// Read the claimed row under our fence; never infer attempts from prefetch.
			row, err = client.PluginNotification.Query().Where(pluginnotification.IDEQ(row.ID), pluginnotification.LeaseTokenEQ(token), func(s *sql.Selector) {
				s.Where(sql.ExprP(s.C(pluginnotification.FieldLeaseUntil) + " > " + pluginNotificationClock(s.Dialect())))
			}).Only(hostCtx)
			if ent.IsNotFound(err) {
				continue
			}
			if err != nil {
				return delivered, err
			}
			status, lastError := "delivered", ""
			if row.Attempts > pluginNotificationMaxAttempts {
				status, lastError = "failed", "retry_limit"
			} else {
				installed, authErr := AuthorizePluginAutomation(hostCtx, client, row.InstallationID, "webhook")
				authErrorCode := "installation_not_authorized"
				if authErr == nil {
					authErr = pluginNotificationSuspension(hostCtx, client, installed, row.SuspensionID)
					authErrorCode = "suspension_not_authorized"
				}
				if authErr != nil {
					status, lastError = "pending", "authorization_check_failed"
					if errors.Is(authErr, privacy.Deny) || ent.IsNotFound(authErr) {
						status, lastError = "cancelled", authErrorCode
					} else if row.Attempts >= pluginNotificationMaxAttempts {
						status = "failed"
					}
				} else if target, targetErr := pluginNotificationTarget(hostCtx, client, row.TargetName, row); targetErr != nil {
					status, lastError = "pending", "target_check_failed"
					if errors.Is(targetErr, ErrWebhookTargetNotFound) || errors.Is(targetErr, ErrWebhookTargetDisabled) || errors.Is(targetErr, errPluginNotificationBody) {
						status, lastError = "cancelled", "target_unavailable"
					} else if row.Attempts >= pluginNotificationMaxAttempts {
						status = "failed"
					}
				} else {
					sendCtx, cancel := context.WithTimeout(hostCtx, 30*time.Second)
					sendErr := pluginNotificationSend(sendCtx, webhooks, target, row)
					cancel()
					if sendErr != nil {
						status, lastError = "pending", "delivery_failed"
						if errors.Is(sendErr, ErrWebhookTargetNotFound) || errors.Is(sendErr, ErrWebhookTargetDisabled) || errors.Is(sendErr, errPluginNotificationBody) {
							status, lastError = "cancelled", "target_unavailable"
						} else if row.Attempts >= pluginNotificationMaxAttempts {
							status = "failed"
						}
					}
				}
			}
			now, err = pluginNotificationNow(hostCtx, client, row.ID)
			if err != nil {
				return delivered, err
			}
			next := int64(math.MaxInt64)
			if status == "pending" {
				next = now + pluginNotificationBackoff(row.Attempts)
			}
			count, err = client.PluginNotification.Update().Where(pluginnotification.IDEQ(row.ID), pluginnotification.LeaseTokenEQ(token), func(s *sql.Selector) {
				s.Where(sql.ExprP(s.C(pluginnotification.FieldLeaseUntil) + " > " + pluginNotificationClock(s.Dialect())))
			}).SetStatus(status).SetLastError(lastError).SetNextAttemptAt(next).SetLeaseToken("").SetLeaseUntil(0).Save(hostCtx)
			if err != nil {
				return delivered, err
			}
			if count > 0 && status == "delivered" {
				delivered++
			}
		}
		return delivered, nil
	})
}

func pluginNotificationBackoff(attempt int) int64 {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		attempt = 6
	}
	return int64(5) << (attempt - 1)
}
