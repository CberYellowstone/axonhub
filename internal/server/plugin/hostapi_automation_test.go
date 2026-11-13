// 职责: 验证真实 JavaScript 自动化调用的授权、幂等、精度与事务围栏。
// 关联文件:
//
//	internal/server/plugin/hostapi_automation.go
//	internal/server/plugin/automation_usage.go
//	internal/server/plugin/kv_cas.go
//	internal/server/plugin/scheduler.go
package plugin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
)

const automationHostTestScript = `
export function onSchedule(ctx, job) {
  try {
  switch (job.operation) {
    case "usage": return ctx.usage.cacheStats(job.input);
    case "apply": return ctx.keySuspensions.apply(job.input);
    case "release": return ctx.keySuspensions.release(job.id);
    case "list": return ctx.keySuspensions.list(job.input);
    case "notify": return ctx.notifications.enqueue(job.input);
    case "set": return ctx.kv.set("progress", "ordinary");
    case "cas": {
      const old = ctx.kv.getEntry("progress");
      const next = ctx.kv.compareAndSet("progress", old === null ? null : old.revision, "next");
      return {next, stale:ctx.kv.compareAndSet("progress", old === null ? null : old.revision, "stale")};
    }
  }
  } catch (error) {
    if (job.catchError) return {error: error.code};
    throw error;
  }
}
`

func TestAutomationHostResultSizeLimit(t *testing.T) {
	host := &hostCall{}
	_, err := host.hostJSONValue(map[string]string{"value": strings.Repeat("x", 2<<20)})
	require.ErrorContains(t, err, "exceeds 2 MiB")
}

type automationHostFixture struct {
	m       *Manager
	p       *LoadedPlugin
	key     *ent.APIKey
	exclude *ent.APIKey
}

func newAutomationHostFixture(t *testing.T) automationHostFixture {
	t.Helper()
	db := newPluginDB(t)
	m := newTestManager(t, db)
	setup := pluginCtx()
	project := db.Project.Create().SetName("host-automation").SaveX(setup)
	key := db.APIKey.Create().SetName("monitored").SetKey("host-test-secret").SetProjectID(project.ID).SetType(apikey.TypeServiceAccount).SaveX(setup)
	exclude := db.APIKey.Create().SetName("excluded").SetKey("excluded-test-secret").SetProjectID(project.ID).SetType(apikey.TypeServiceAccount).SaveX(setup)
	permissions := []string{objects.PluginPermissionUsageMetrics, objects.PluginPermissionKeySuspensions, "webhook"}
	installed := installUI(t, m, uiZip(t, func(_ *testing.T, manifest map[string]any) {
		manifest["permissions"] = permissions
		manifest["schedules"] = []any{map[string]any{"key": "check", "intervalSeconds": 60}}
		manifest["requiredCapabilities"] = HostCapabilities()
	}, map[string]string{"ui/page.html": "<html></html>", "plugin.js": automationHostTestScript}))
	db.Plugin.UpdateOneID(installed.ID).SetEnabled(true).SetGrantedPermissions(permissions).
		SetAutomationScope(objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: project.ID, AllKeys: true, ExcludedAPIKeyIDs: []int{exclude.ID}}}}).SaveX(setup)
	cfg, err := json.Marshal(biz.WebhookNotifierConfig{Targets: []biz.WebhookTarget{{Name: "ops", Enabled: true, URL: "http://127.0.0.1/unused-test-target", Body: "{{.Data | json}}"}}})
	require.NoError(t, err)
	db.System.Create().SetKey(biz.SystemKeyWebhookNotifierConfig).SetValue(string(cfg)).SaveX(setup)
	require.NoError(t, m.ForceRefresh(setup))
	p := m.Current().ByID(installed.ID)
	require.Equal(t, RuntimeStatusActive, p.Status)
	return automationHostFixture{m: m, p: p, key: key, exclude: exclude}
}

func (f automationHostFixture) invoke(t *testing.T, ctx context.Context, operation string, input any, id int) (InvokeResult, error) {
	t.Helper()
	args, err := json.Marshal([]any{map[string]any{"operation": operation, "input": input, "id": id}})
	require.NoError(t, err)
	return f.m.Invoke(ctx, InvokeRequest{InstallationID: f.p.ID, Export: "onSchedule", Args: args, Generation: f.p.generation})
}

func (f automationHostFixture) applyInput() objects.APIKeySuspensionInput {
	return objects.APIKeySuspensionInput{ProjectID: f.key.ProjectID, APIKeyID: f.key.ID, DurationSeconds: 60,
		IdempotencyKey: "rule-v1:key:observation-1", Reason: "low cache rate", Evidence: map[string]any{"ratio": 0.1},
		Notification: &objects.PluginNotificationInput{TargetName: "ops", Payload: map[string]any{"message": "key paused"}}}
}

func (f automationHostFixture) lease(t *testing.T) (context.Context, *ent.PluginSchedule) {
	t.Helper()
	_, err := f.m.scheduler.reconcile(pluginCtx(), f.p)
	require.NoError(t, err)
	now, err := scheduleDBNow(pluginCtx(), f.m.db)
	require.NoError(t, err)
	f.m.db.PluginSchedule.Update().SetNextRunAt(now.Add(-time.Minute)).SaveX(pluginCtx())
	row := f.m.db.PluginSchedule.Query().OnlyX(pluginCtx())
	claimed, err := f.m.scheduler.claim(pluginCtx(), f.p, row)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	return scheduleTestCall(f.m.scheduler, f.p, claimed), claimed
}

func TestAutomationHostJavaScriptRoundTrip(t *testing.T) {
	f := newAutomationHostFixture(t)
	ctx, _ := f.lease(t)
	result, err := f.invoke(t, ctx, "usage", CacheUsageOptions{ProjectID: f.key.ProjectID, AllKeys: true, LastRequests: 2}, 0)
	require.NoError(t, err)
	var page biz.CacheUsagePage
	require.NoError(t, json.Unmarshal(result.Value, &page))
	require.Len(t, page.Items, 1)
	require.Equal(t, f.key.ID, page.Items[0].KeyID)
	require.False(t, page.Items[0].SampleComplete)
	require.NotContains(t, string(result.Value), f.key.Key)

	result, err = f.invoke(t, ctx, "cas", nil, 0)
	require.NoError(t, err)
	var cas struct{ Next, Stale KVCASResult }
	require.NoError(t, json.Unmarshal(result.Value, &cas))
	require.True(t, cas.Next.Swapped)
	require.NotEmpty(t, cas.Next.Entry.Revision)
	require.False(t, cas.Stale.Swapped)

	input := f.applyInput()
	result, err = f.invoke(t, ctx, "apply", input, 0)
	require.NoError(t, err)
	var hold objects.APIKeySuspensionResult
	require.NoError(t, json.Unmarshal(result.Value, &hold))
	require.Positive(t, hold.ID)
	require.NotNil(t, hold.ExpiresAt)
	require.NotContains(t, string(result.Value), "idempotency_key_hash")
	_, err = f.invoke(t, ctx, "apply", input, 0)
	require.NoError(t, err)
	require.Equal(t, 1, f.m.db.APIKeySuspension.Query().CountX(pluginCtx()))
	require.Equal(t, 1, f.m.db.PluginNotification.Query().CountX(pluginCtx()))
	_, err = f.invoke(t, ctx, "list", objects.APIKeySuspensionFilter{ProjectID: f.key.ProjectID, ActiveOnly: true}, 0)
	require.NoError(t, err)
	result, err = f.invoke(t, ctx, "release", nil, hold.ID)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(result.Value, &hold))
	require.NotNil(t, hold.ReleasedAt)
	require.Equal(t, apikey.StatusEnabled, f.m.db.APIKey.GetX(pluginCtx(), f.key.ID).Status)
}

func TestAutomationHostRejectsForgedScopeAndRevokedGrant(t *testing.T) {
	f := newAutomationHostFixture(t)
	assertCode := func(ctx context.Context, operation string, input any, code string) {
		t.Helper()
		args, err := json.Marshal([]any{map[string]any{"operation": operation, "input": input, "catchError": true}})
		require.NoError(t, err)
		result, err := f.m.Invoke(ctx, InvokeRequest{InstallationID: f.p.ID, Export: "onSchedule", Args: args, Generation: f.p.generation})
		require.NoError(t, err)
		require.JSONEq(t, `{"error":"`+code+`"}`, string(result.Value))
	}
	assertCode(context.Background(), "usage", map[string]any{"projectId": f.key.ProjectID, "allKeys": true, "installationId": 99}, "INVALID_ARGUMENT")
	input := f.applyInput()
	input.APIKeyID = f.exclude.ID
	assertCode(context.Background(), "apply", input, "PERMISSION_DENIED")
	require.Zero(t, f.m.db.APIKeySuspension.Query().CountX(pluginCtx()))

	ctx, _ := f.lease(t)
	f.m.db.Plugin.UpdateOneID(f.p.ID).SetGrantedPermissions([]string{}).SaveX(pluginCtx())
	assertCode(ctx, "apply", f.applyInput(), "INVALID_STATE")
	require.Zero(t, f.m.db.APIKeySuspension.Query().CountX(pluginCtx()))
	assertCode(context.Background(), "apply", f.applyInput(), "PERMISSION_DENIED")
}

func TestAutomationHostFencedWritesRollback(t *testing.T) {
	for _, operation := range []string{"apply", "set", "cas", "notify"} {
		t.Run(operation, func(t *testing.T) {
			f := newAutomationHostFixture(t)
			ctx, row := f.lease(t)
			// Invalidate the lease after the DB write starts. The enclosing host
			// fence must roll back both the business write and this test mutation.
			invalidate := func(next ent.Mutator) ent.Mutator {
				return ent.MutateFunc(func(txCtx context.Context, mutation ent.Mutation) (ent.Value, error) {
					value, err := next.Mutate(txCtx, mutation)
					if err == nil {
						err = f.m.client(txCtx).PluginSchedule.UpdateOneID(row.ID).SetLeaseToken("retired-by-test").Exec(txCtx)
					}
					return value, err
				})
			}
			var input any
			switch operation {
			case "apply":
				input = f.applyInput()
				f.m.db.APIKeySuspension.Use(invalidate)
			case "notify":
				input = objects.PluginNotificationInput{TargetName: "ops", Payload: map[string]any{}, IdempotencyKey: "notify"}
				f.m.db.PluginNotification.Use(invalidate)
			default:
				f.m.db.PluginStorage.Use(invalidate)
			}
			_, err := f.invoke(t, ctx, operation, input, 0)
			require.Error(t, err)
			require.Zero(t, f.m.db.APIKeySuspension.Query().CountX(pluginCtx()))
			require.Zero(t, f.m.db.PluginNotification.Query().CountX(pluginCtx()))
			require.Zero(t, f.m.db.PluginStorage.Query().CountX(pluginCtx()))
			require.Equal(t, row.LeaseToken, f.m.db.PluginSchedule.GetX(pluginCtx(), row.ID).LeaseToken)
		})
	}
}

func TestAutomationCacheUsageSafeIntegerBoundary(t *testing.T) {
	for _, value := range []int64{1<<53 - 2, 1<<53 - 1, 1 << 53, 1<<53 + 1} {
		page := biz.CacheUsagePage{Items: []biz.CacheKeyUsage{{PromptTokens: value}}}
		err := validateCacheUsageNumbers(page)
		require.Equal(t, value >= 1<<53, err != nil)
		page.Items[0].PromptTokens = 0
		page.Items[0].Samples = []biz.CacheUsageSample{{CachedTokens: value}}
		require.Equal(t, value >= 1<<53, validateCacheUsageNumbers(page) != nil)
	}
	f := newAutomationHostFixture(t)
	setup := pluginCtx()
	now := time.Now().UTC().Add(-time.Second)
	r := f.m.db.Request.Create().SetProjectID(f.key.ProjectID).SetAPIKeyID(f.key.ID).SetModelID("m").
		SetStatus(request.StatusCompleted).SetCompletedAt(now).SetCreatedAt(now).SetUpdatedAt(now).SetRequestBody(objects.JSONRawMessage(`{}`)).SaveX(setup)
	f.m.db.UsageLog.Create().SetRequestID(r.ID).SetProjectID(f.key.ProjectID).SetAPIKeyID(f.key.ID).SetModelID("m").
		SetCreatedAt(now).SetPromptTokens(1<<53 + 1).SetCacheReadStatus("reported").SaveX(setup)
	_, err := f.invoke(t, context.Background(), "usage", CacheUsageOptions{ProjectID: f.key.ProjectID, KeyIDs: []int{f.key.ID}, LastRequests: 1, LowThreshold: lo.ToPtr(0.5)}, 0)
	require.Error(t, err, "unsafe totals must never reach JavaScript rounded")
}
