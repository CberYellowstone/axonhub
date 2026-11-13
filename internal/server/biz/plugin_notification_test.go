// 职责: 验证通知 outbox 的幂等、授权撤回、重试及租约围栏。
// 关联文件:
//
//	internal/server/biz/plugin_notification.go
//	internal/ent/schema/plugin_notification.go
package biz

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/ent/pluginnotification"
	"github.com/looplj/axonhub/internal/ent/system"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/llm/httpclient"
)

func pluginNotificationHarness(t *testing.T, handler http.HandlerFunc) (context.Context, *ent.Client, *ent.Plugin, *WebhookNotifier) {
	t.Helper()
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=0")
	t.Cleanup(func() { _ = client.Close() })
	ctx := authz.WithSystemBypass(authz.NewSystemContext(ent.NewContext(t.Context(), client)), "notification-test")
	manifest := objects.PluginManifest{
		SchemaVersion: 1, SchemaVersionSet: true, PluginAPIVersion: 1, PluginAPIVersionSet: true,
		ID: "test.notifications", Name: "Notifications", Version: "1.0.0", AxonhubVersion: "*", Permissions: []string{"webhook"},
	}
	require.NoError(t, manifest.Validate())
	installed, err := client.Plugin.Create().SetPluginID(manifest.ID).SetManifest(manifest).SetEnabled(true).SetGrantedPermissions([]string{"webhook"}).Save(ctx)
	require.NoError(t, err)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	service := newTestSystemServiceWithWebhookConfig(t, client, WebhookNotifierConfig{Targets: []WebhookTarget{{Name: "ops", URL: server.URL, Enabled: true, Body: "{{.Data | json}}"}}})
	return ctx, client, installed, NewWebhookNotifier(service, httpclient.NewHttpClient())
}

func TestPluginNotificationIdempotencyAndRetry(t *testing.T) {
	attempts := 0
	var bodies []map[string]any
	ctx, client, installed, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, r *http.Request) {
		attempts++
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var data map[string]any
		require.NoError(t, json.Unmarshal(body, &data))
		bodies = append(bodies, data)
		if attempts == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	})
	input := PluginNotificationInput{TargetName: "ops", IdempotencyKey: "stable-key", Payload: map[string]any{"message": "暂停通知"}}
	row, err := EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.NoError(t, err)
	input.Payload["message"] = "changed"
	duplicate, err := EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.NoError(t, err)
	require.Equal(t, row.ID, duplicate.ID)
	require.Equal(t, "暂停通知", duplicate.Payload["message"])
	count, err := DeliverPluginNotifications(ctx, client, notifier, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	row, err = client.PluginNotification.Get(ctx, row.ID)
	require.NoError(t, err)
	require.Equal(t, "pending", row.Status)
	require.Equal(t, 1, row.Attempts)
	require.Equal(t, "delivery_failed", row.LastError)
	require.Positive(t, row.NextAttemptAt)
	count, err = DeliverPluginNotifications(ctx, client, notifier, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, 1, attempts)
	require.NoError(t, client.PluginNotification.UpdateOneID(row.ID).SetNextAttemptAt(0).Exec(ctx))
	count, err = DeliverPluginNotifications(ctx, client, notifier, 10)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, bodies[0]["eventId"], bodies[1]["eventId"])
	require.Equal(t, "stable-key", bodies[1]["idempotencyKey"])
	count, err = DeliverPluginNotifications(ctx, client, notifier, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, 2, attempts)
}

func TestPluginNotificationValidationAndRevocation(t *testing.T) {
	calls := 0
	ctx, client, installed, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { calls++; w.WriteHeader(204) })
	input := PluginNotificationInput{TargetName: "missing", IdempotencyKey: "a", Payload: map[string]any{}}
	_, err := EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.ErrorIs(t, err, ErrWebhookTargetNotFound)
	input.TargetName = "ops"
	input.Payload["message"] = strings.Repeat("x", 65536)
	_, err = EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.Error(t, err)
	input.Payload = map[string]any{}
	row, err := EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.NoError(t, err)
	require.NoError(t, client.Plugin.UpdateOneID(installed.ID).SetEnabled(false).Exec(ctx))
	count, err := DeliverPluginNotifications(ctx, client, notifier, 10)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, calls)
	row, err = client.PluginNotification.Get(ctx, row.ID)
	require.NoError(t, err)
	require.Equal(t, "cancelled", row.Status)
	_, err = EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.Error(t, err)
}

func TestPluginNotificationLeaseFence(t *testing.T) {
	var steal func()
	ctx, client, installed, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { steal(); w.WriteHeader(204) })
	row, err := EnqueuePluginNotification(ctx, client, installed.ID, PluginNotificationInput{TargetName: "ops", IdempotencyKey: "lease", Payload: map[string]any{}})
	require.NoError(t, err)
	// A live lease prevents another worker from sending.
	require.NoError(t, client.PluginNotification.UpdateOneID(row.ID).SetLeaseToken("other").SetLeaseUntil(1<<62).Exec(ctx))
	count, err := DeliverPluginNotifications(ctx, client, notifier, 1)
	require.NoError(t, err)
	require.Zero(t, count)
	// Model an expired lease claimed by a new worker during an old HTTP request.
	require.NoError(t, client.PluginNotification.UpdateOneID(row.ID).SetLeaseUntil(0).Exec(ctx))
	steal = func() {
		require.NoError(t, client.PluginNotification.UpdateOneID(row.ID).SetLeaseToken("new-worker").SetLeaseUntil(1<<62).Exec(ctx))
	}
	count, err = DeliverPluginNotifications(ctx, client, notifier, 1)
	require.NoError(t, err)
	require.Zero(t, count)
	row, err = client.PluginNotification.Get(ctx, row.ID)
	require.NoError(t, err)
	require.Equal(t, "new-worker", row.LeaseToken)
	require.Equal(t, "pending", row.Status)
}

func TestPluginNotificationTerminalFailure(t *testing.T) {
	ctx, client, installed, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) })
	row, err := EnqueuePluginNotification(ctx, client, installed.ID, PluginNotificationInput{TargetName: "ops", IdempotencyKey: "terminal", Payload: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, client.PluginNotification.UpdateOneID(row.ID).SetAttempts(pluginNotificationMaxAttempts-1).Exec(ctx))
	count, err := DeliverPluginNotifications(ctx, client, notifier, 1)
	require.NoError(t, err)
	require.Zero(t, count)
	row, err = client.PluginNotification.Query().Where(pluginnotification.IDEQ(row.ID)).Only(ctx)
	require.NoError(t, err)
	require.Equal(t, "failed", row.Status)
	for i, want := range []int64{5, 10, 20, 40, 80, 160, 160} {
		require.Equal(t, want, pluginNotificationBackoff(i+1))
	}
}

func TestPluginNotificationTargetAndGrantRevocation(t *testing.T) {
	for _, mutation := range []string{"revoke_grant", "disable_target", "remove_target", "delete_installation"} {
		t.Run(mutation, func(t *testing.T) {
			calls := 0
			ctx, client, installed, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { calls++; w.WriteHeader(204) })
			row, err := EnqueuePluginNotification(ctx, client, installed.ID, PluginNotificationInput{TargetName: "ops", IdempotencyKey: mutation, Payload: map[string]any{}})
			require.NoError(t, err)
			switch mutation {
			case "revoke_grant":
				require.NoError(t, client.Plugin.UpdateOneID(installed.ID).SetGrantedPermissions([]string{}).Exec(ctx))
			case "delete_installation":
				require.NoError(t, client.Plugin.UpdateOneID(installed.ID).SetDeletedAt(1).Exec(ctx))
			case "disable_target":
				require.NoError(t, notifier.SystemService.SetWebhookNotifierConfig(ctx, &WebhookNotifierConfig{Targets: []WebhookTarget{{Name: "ops", Enabled: false, URL: "http://unused.invalid"}}}))
			case "remove_target":
				require.NoError(t, notifier.SystemService.SetWebhookNotifierConfig(ctx, &WebhookNotifierConfig{}))
			}
			count, err := DeliverPluginNotifications(ctx, client, notifier, 10)
			require.NoError(t, err)
			require.Zero(t, count)
			require.Zero(t, calls)
			row, err = client.PluginNotification.Get(ctx, row.ID)
			require.NoError(t, err)
			require.Equal(t, "cancelled", row.Status)
		})
	}
}

func TestPluginNotificationUsesTransactionContext(t *testing.T) {
	ctx, client, installed, _ := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	tx, err := client.Tx(ctx)
	require.NoError(t, err)
	txCtx := ent.NewContext(ctx, tx.Client())
	_, err = EnqueuePluginNotification(txCtx, client, installed.ID, PluginNotificationInput{TargetName: "ops", IdempotencyKey: "rollback", Payload: map[string]any{}})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	count, err := client.PluginNotification.Query().Count(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestPluginNotificationUsesLiveTargetSnapshot(t *testing.T) {
	oldCalls := make(chan struct{}, 1)
	ctx, client, installed, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { oldCalls <- struct{}{}; w.WriteHeader(204) })
	input := PluginNotificationInput{TargetName: "ops", IdempotencyKey: "fresh-target", Payload: map[string]any{"message": "fresh"}}
	row, err := EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.NoError(t, err)
	// Prime the old configuration, then bypass cache invalidation like another instance.
	_ = notifier.SystemService.WebhookNotifierConfigOrDefault(ctx)
	received := make(chan map[string]string, 1)
	fresh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- map[string]string{"body": string(body), "header": r.Header.Get("X-Event-ID")}
		w.WriteHeader(204)
	}))
	defer fresh.Close()
	cfg := WebhookNotifierConfig{Targets: []WebhookTarget{{Name: "ops", Enabled: true, URL: fresh.URL, Body: `{"fresh":true,"eventId":{{.Data.eventId | json}}}`, Headers: []objects.HeaderEntry{{Key: "X-Event-ID", Value: "{{.Data.eventId}}"}}}}}
	encoded, err := json.Marshal(cfg)
	require.NoError(t, err)
	_, err = client.System.Update().Where(system.KeyEQ(SystemKeyWebhookNotifierConfig)).SetValue(string(encoded)).Save(ctx)
	require.NoError(t, err)
	count, err := DeliverPluginNotifications(ctx, client, notifier, 1)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Empty(t, oldCalls)
	select {
	case request := <-received:
		require.JSONEq(t, `{"fresh":true,"eventId":"`+row.EventID+`"}`, request["body"])
		require.Equal(t, row.EventID, request["header"])
	default:
		t.Fatal("live target did not receive notification")
	}
}

func TestPluginNotificationSuspensionScope(t *testing.T) {
	ctx, client, installed, _ := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	project := client.Project.Create().SetName("notification-project").SaveX(ctx)
	key := client.APIKey.Create().SetName("notification-key").SetKey("notification-test-key").SetProjectID(project.ID).SetType(apikey.TypeServiceAccount).SaveX(ctx)
	scope := objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: project.ID, AllKeys: true}}}
	client.Plugin.UpdateOneID(installed.ID).SetAutomationScope(scope).ExecX(ctx)
	createHold := func(owner int, hash string) *ent.APIKeySuspension {
		return client.APIKeySuspension.Create().SetInstallationID(owner).SetPluginID("notification-test").SetAPIKeyID(key.ID).SetProjectID(project.ID).SetIdempotencyKeyHash(strings.Repeat(hash, 64)).SetReason("test").SetEvidence(map[string]any{}).SetCreatedBy("test").SaveX(ctx)
	}
	foreign := createHold(installed.ID+1, "a")
	input := PluginNotificationInput{TargetName: "ops", IdempotencyKey: "hold", Payload: map[string]any{}, SuspensionID: &foreign.ID}
	_, err := EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.Error(t, err, "another installation's hold must be rejected")
	own := createHold(installed.ID, "b")
	input.SuspensionID = &own.ID
	_, err = EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.NoError(t, err)
	client.Plugin.UpdateOneID(installed.ID).SetAutomationScope(objects.PluginAutomationScope{}).ExecX(ctx)
	_, err = EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.Error(t, err, "current scope must apply even to a duplicate enqueue")
	client.Plugin.UpdateOneID(installed.ID).SetAutomationScope(scope).ExecX(ctx)
	client.APIKey.UpdateOneID(key.ID).SetDeletedAt(1).ExecX(ctx)
	_, err = EnqueuePluginNotification(ctx, client, installed.ID, input)
	require.Error(t, err, "a deleted current key must be rejected")
}

func TestPluginNotificationInputJSON(t *testing.T) {
	var input PluginNotificationInput
	decoder := json.NewDecoder(strings.NewReader(`{"targetName":"ops","payload":{"message":"ok"},"idempotencyKey":"key","suspensionId":7,"evidence":{"ratio":0.1}}`))
	decoder.DisallowUnknownFields()
	require.NoError(t, decoder.Decode(&input))
	require.Equal(t, "ops", input.TargetName)
	require.Equal(t, 7, *input.SuspensionID)
	encoded, err := json.Marshal(input)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"idempotencyKey"`)
	require.NotContains(t, string(encoded), `"TargetName"`)
}

func TestPluginNotificationDeliveryRechecksSuspension(t *testing.T) {
	for _, change := range []string{"scope_revoked", "key_deleted", "key_personal", "foreign_hold"} {
		t.Run(change, func(t *testing.T) {
			calls := make(chan struct{}, 1)
			ctx, client, installed, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { calls <- struct{}{}; w.WriteHeader(204) })
			project := client.Project.Create().SetName("hold-project").SaveX(ctx)
			key := client.APIKey.Create().SetName("hold-key").SetKey("hold-key").SetProjectID(project.ID).SetType(apikey.TypeServiceAccount).SaveX(ctx)
			client.Plugin.UpdateOneID(installed.ID).SetAutomationScope(objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: project.ID, AllKeys: true}}}).ExecX(ctx)
			hold := client.APIKeySuspension.Create().SetInstallationID(installed.ID).SetPluginID(installed.PluginID).SetAPIKeyID(key.ID).SetProjectID(project.ID).SetIdempotencyKeyHash(strings.Repeat("a", 64)).SetReason("test").SetEvidence(map[string]any{}).SetCreatedBy("test").SaveX(ctx)
			row, err := EnqueuePluginNotification(ctx, client, installed.ID, PluginNotificationInput{TargetName: "ops", IdempotencyKey: change, Payload: map[string]any{}, SuspensionID: &hold.ID})
			require.NoError(t, err)
			switch change {
			case "scope_revoked":
				client.Plugin.UpdateOneID(installed.ID).SetAutomationScope(objects.PluginAutomationScope{}).ExecX(ctx)
			case "key_deleted":
				client.APIKey.UpdateOneID(key.ID).SetDeletedAt(1).ExecX(ctx)
			case "key_personal":
				client.APIKey.UpdateOneID(key.ID).SetType(apikey.TypePersonal).ExecX(ctx)
			case "foreign_hold":
				// Model a legacy/tampered outbox row; the immutable hold's owner stays foreign.
				other := client.APIKeySuspension.Create().SetInstallationID(installed.ID + 1).SetPluginID("foreign").SetAPIKeyID(key.ID).SetProjectID(project.ID).SetIdempotencyKeyHash(strings.Repeat("b", 64)).SetReason("test").SetEvidence(map[string]any{}).SetCreatedBy("test").SaveX(ctx)
				client.PluginNotification.DeleteOneID(row.ID).ExecX(ctx)
				row = client.PluginNotification.Create().SetInstallationID(installed.ID).SetIdempotencyHash(strings.Repeat("c", 64)).SetIdempotencyKey(change).SetEventID("legacy").SetTargetName("ops").SetPayload(map[string]any{}).SetSuspensionID(other.ID).SaveX(ctx)
			}
			count, err := DeliverPluginNotifications(ctx, client, notifier, 1)
			require.NoError(t, err)
			require.Zero(t, count)
			require.Empty(t, calls)
			row = client.PluginNotification.GetX(ctx, row.ID)
			require.Equal(t, "cancelled", row.Status)
			require.Equal(t, "suspension_not_authorized", row.LastError)
		})
	}
}

func TestPluginNotificationClaimReadsActualAttempts(t *testing.T) {
	for _, previous := range []int{1, 5, 6} {
		t.Run(string(rune('0'+previous)), func(t *testing.T) {
			calls := make(chan struct{}, 1)
			ctx, client, installed, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) { calls <- struct{}{}; w.WriteHeader(503) })
			row, err := EnqueuePluginNotification(ctx, client, installed.ID, PluginNotificationInput{TargetName: "ops", IdempotencyKey: "attempts", Payload: map[string]any{}})
			require.NoError(t, err)
			advanced := false
			client.PluginNotification.Use(func(next ent.Mutator) ent.Mutator {
				return ent.MutateFunc(func(hookCtx context.Context, mutation ent.Mutation) (ent.Value, error) {
					m := mutation.(*ent.PluginNotificationMutation)
					if token, ok := m.LeaseToken(); ok && token != "" && !advanced {
						advanced = true
						// Deterministically interleave another drainer's completed attempts
						// between this worker's prefetch and its atomic AddAttempts claim.
						if err := client.PluginNotification.UpdateOneID(row.ID).SetAttempts(previous).Exec(hookCtx); err != nil {
							return nil, err
						}
					}
					return next.Mutate(hookCtx, mutation)
				})
			})
			before, err := pluginNotificationNow(ctx, client, row.ID)
			require.NoError(t, err)
			count, err := DeliverPluginNotifications(ctx, client, notifier, 1)
			require.NoError(t, err)
			require.Zero(t, count)
			require.True(t, advanced)
			row = client.PluginNotification.GetX(ctx, row.ID)
			require.Equal(t, previous+1, row.Attempts)
			if previous == 1 {
				after, err := pluginNotificationNow(ctx, client, row.ID)
				require.NoError(t, err)
				require.Equal(t, "pending", row.Status)
				require.GreaterOrEqual(t, row.NextAttemptAt, before+10)
				require.LessOrEqual(t, row.NextAttemptAt, after+10)
			} else {
				require.Equal(t, "failed", row.Status)
			}
			if previous <= 5 {
				require.Len(t, calls, 1)
			} else {
				require.Empty(t, calls)
			}
		})
	}
}

func TestPluginNotificationRejectsTransactionClients(t *testing.T) {
	ctx, client, _, notifier := pluginNotificationHarness(t, func(w http.ResponseWriter, _ *http.Request) {
		t.Error("unexpected HTTP in transaction")
		w.WriteHeader(204)
	})
	tx, err := client.Tx(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.False(t, client.IsTransactional())
	require.True(t, tx.Client().IsTransactional())
	for _, test := range []struct {
		name   string
		ctx    context.Context
		client *ent.Client
	}{
		{"context_transaction", ent.NewTxContext(ctx, tx), client},
		{"direct_transaction_client", ctx, tx.Client()},
		{"debug_transaction_client", ctx, tx.Client().Debug()},
		{"context_transaction_client", ent.NewContext(ctx, tx.Client()), client},
	} {
		t.Run(test.name, func(t *testing.T) {
			count, err := DeliverPluginNotifications(test.ctx, test.client, notifier, 1)
			require.Error(t, err)
			require.Zero(t, count)
		})
	}
}

func TestPluginNotificationReservedPayloadFields(t *testing.T) {
	row := &ent.PluginNotification{EventID: "actual-event", IdempotencyKey: "actual-key", InstallationID: 9, Payload: map[string]any{"eventId": "fake", "idempotencyKey": "fake", "installationId": 100, "suspensionId": 100, "evidence": map[string]any{"fake": true}, "message": "ok"}}
	data := pluginNotificationData(row)
	require.Equal(t, "actual-event", data["eventId"])
	require.Equal(t, "actual-key", data["idempotencyKey"])
	require.Equal(t, 9, data["installationId"])
	require.Equal(t, "ok", data["message"])
	require.NotContains(t, data, "suspensionId")
	require.NotContains(t, data, "evidence")
	id := 7
	row.SuspensionID, row.Evidence = &id, map[string]any{"actual": true}
	data = pluginNotificationData(row)
	require.Equal(t, 7, data["suspensionId"])
	require.Equal(t, row.Evidence, data["evidence"])
	require.Equal(t, 100, row.Payload["suspensionId"], "rendering must not mutate caller payload")
}
