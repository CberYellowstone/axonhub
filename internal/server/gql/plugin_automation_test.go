// 职责: 通过真实 HTTP GraphQL 委托验证插件自动化授权、指标整数及 KV CAS 隔离。
// 关联文件:
//
//	internal/server/gql/plugin_call.go
//	internal/server/gql/plugin_automation.resolvers.go
//	internal/server/gql/plugin.resolvers.go
//	internal/server/plugin/automation.go
//	internal/server/plugin/kv_cas.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//	.agents/rules/ent-graphql.md
//
// 注意事项: 使用安装 ZIP 解码合法 manifest；MetricCount 按 JSON 数字原文断言，避免 float64 丢失精度。
package gql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/ent/user"
	"github.com/looplj/axonhub/internal/objects"
)

const automationPluginID = "acme.automation"
const automationOtherID = "acme.automation-other"

type automationEndpointFixture struct {
	*pluginEndpointFixture
	projectID      int
	otherProjectID int
	key            *ent.APIKey
	otherKey       *ent.APIKey
}

func newAutomationEndpointFixture(t *testing.T, name string) *automationEndpointFixture {
	t.Helper()
	fx := newPluginEndpointFixture(t, name)
	ctx := fx.setupCtx
	// The shared helper writes explicit schemaVersion/pluginApiVersion JSON;
	// installation therefore sets the required manifest presence flags itself.
	permissions := []string{"manage_key_suspensions", "read_usage_metrics", "read_dashboard"}
	for _, id := range []string{automationPluginID, automationOtherID} {
		installed, err := fx.manager.Install(ctx, pluginEndpointZip(t, id, permissions, true), nil, nil)
		require.NoError(t, err)
		require.NoError(t, fx.manager.Enable(ctx, installed.Plugin.ID, permissions, nil))
		if id == automationPluginID {
			fx.pages = installed.Plugin
		} else {
			fx.other = installed.Plugin
		}
	}
	p := fx.db.Project.Create().SetName(name + "-allowed").SaveX(ctx)
	other := fx.db.Project.Create().SetName(name + "-other").SaveX(ctx)
	key := fx.db.APIKey.Create().SetProjectID(p.ID).SetName("allowed-key").SetKey(name + "-secret").SetType(apikey.TypeServiceAccount).SaveX(ctx)
	otherKey := fx.db.APIKey.Create().SetProjectID(other.ID).SetName("other-key").SetKey(name + "-other-secret").SetType(apikey.TypeServiceAccount).SaveX(ctx)
	writer := fx.db.User.Query().Where(user.FirstNameEQ("Writer")).OnlyX(ctx)
	fx.db.User.UpdateOne(writer).SetScopes([]string{"read_plugins", "write_plugins"}).ExecX(ctx)
	// read_dashboard is currently system-only; a project membership alone does
	// not grant it. The installation ceiling still restricts each query's project.
	fx.db.User.UpdateOneID(fx.readerID).SetScopes([]string{"read_dashboard"}).ExecX(ctx)
	fx.db.UserProject.Create().SetUserID(fx.readerID).SetProjectID(p.ID).SetScopes([]string{"read_dashboard"}).SaveX(ctx)
	return &automationEndpointFixture{pluginEndpointFixture: fx, projectID: p.ID, otherProjectID: other.ID, key: key, otherKey: otherKey}
}

func automationData(t *testing.T, code int, payload map[string]any, raw, field string) any {
	t.Helper()
	require.Equal(t, http.StatusOK, code, raw)
	require.NotContains(t, payload, "errors", raw)
	data, ok := payload["data"].(map[string]any)
	require.True(t, ok, raw)
	return data[field]
}

func (f *automationEndpointFixture) setScope(t *testing.T, installationID int, projectIDs ...int) {
	t.Helper()
	projects := make([]map[string]any, 0, len(projectIDs))
	for _, id := range projectIDs {
		projects = append(projects, map[string]any{"projectId": id, "allKeys": true})
	}
	code, payload, raw := f.post("/admin/graphql", f.ownerToken,
		`mutation($id: ID!, $scope: PluginAutomationScopeInput!) { setPluginAutomationScope(id:$id, scope:$scope) { automationScope { projects { projectId allKeys } } } }`,
		map[string]any{"id": fmt.Sprintf("gid://axonhub/Plugin/%d", installationID), "scope": map[string]any{"projects": projects}})
	result := automationData(t, code, payload, raw, "setPluginAutomationScope").(map[string]any)
	require.Len(t, result["automationScope"].(map[string]any)["projects"], len(projectIDs))
}

func (f *automationEndpointFixture) apply(t *testing.T, pluginID, token, idempotency string, key *ent.APIKey) (int, map[string]any, string) {
	t.Helper()
	return f.postPluginVars(token, pluginID,
		`mutation($input: PluginKeySuspensionInput!) { applyPluginKeySuspension(input:$input) { id installationId apiKeyId projectId permanent: expiresAt releasedAt evidence } }`,
		map[string]any{"input": map[string]any{"apiKeyId": key.ID, "projectId": key.ProjectID, "idempotencyKey": idempotency, "reason": "cache policy", "permanent": true, "evidence": map[string]any{"sample": 1}}})
}

func TestPluginAutomationOwnerBoundary(t *testing.T) {
	f := newAutomationEndpointFixture(t, "automation_owner_boundary")
	grant := fmt.Sprintf(`mutation { setPluginAutomationScope(id:"gid://axonhub/Plugin/%d", scope:{projects:[{projectId:%d,allKeys:true}]}) { id } }`, f.pages.ID, f.projectID)
	code, payload, raw := f.postAdmin(f.writerToken, grant)
	require.Equal(t, http.StatusOK, code, raw)
	require.Contains(t, payload, "errors", raw)
	require.Empty(t, f.db.Plugin.GetX(f.setupCtx, f.pages.ID).AutomationScope.Projects)
	code, payload, raw = f.postPlugin(f.ownerToken, automationPluginID, grant)
	require.Equal(t, http.StatusOK, code, raw)
	require.Contains(t, payload, "errors", "a delegated Owner is not a direct Owner: "+raw)
	f.setScope(t, f.pages.ID, f.projectID)

	settings := fmt.Sprintf(`mutation { updatePluginSettings(id:"gid://axonhub/Plugin/%d",priority:77,config:{}) { priority } }`, f.pages.ID)
	code, payload, raw = f.postAdmin(f.writerToken, settings)
	require.Equal(t, http.StatusOK, code, raw)
	require.Contains(t, payload, "errors", raw)
	require.NotEqual(t, 77, f.db.Plugin.GetX(f.setupCtx, f.pages.ID).Priority)
	for _, mutation := range []string{
		`setPluginKV(key:"rules",value:"unsafe") { value }`,
		`compareAndSetPluginKV(key:"rules",expectedRevision:null,value:"unsafe") { swapped }`,
		`deletePluginKV(key:"rules")`,
	} {
		code, payload, raw = f.postPlugin(f.writerToken, automationPluginID, "mutation { "+mutation+" }")
		require.Equal(t, http.StatusOK, code, raw)
		require.Equal(t, "FORBIDDEN", graphqlErrorCode(t, payload), raw)
	}
	entry, err := f.manager.KVGet(f.setupCtx, automationPluginID, f.pages.ID, "", "rules")
	require.NoError(t, err)
	require.Nil(t, entry)

	for _, forged := range []string{`installationId:999`, `apiKey:"credential"`, `token:"credential"`} {
		query := fmt.Sprintf(`mutation { setPluginAutomationScope(id:"gid://axonhub/Plugin/%d",scope:{projects:[],%s}) { id } }`, f.pages.ID, forged)
		_, payload, raw = f.postAdmin(f.ownerToken, query)
		require.Contains(t, payload, "errors", raw)
		require.Len(t, f.db.Plugin.GetX(f.setupCtx, f.pages.ID).AutomationScope.Projects, 1)
	}
	// Neither ordinary GraphQL nor caller-provided input can acquire plugin identity.
	apply := fmt.Sprintf(`mutation { applyPluginKeySuspension(input:{apiKeyId:%d,projectId:%d,idempotencyKey:"direct",reason:"test",permanent:true}) { id } }`, f.key.ID, f.projectID)
	code, payload, raw = f.postAdmin(f.ownerToken, apply)
	require.Equal(t, http.StatusOK, code, raw)
	require.Equal(t, "FORBIDDEN", graphqlErrorCode(t, payload), raw)
	for _, forged := range []string{`installationId:999`, `apiKey:"credential"`} {
		query := fmt.Sprintf(`mutation { applyPluginKeySuspension(input:{apiKeyId:%d,projectId:%d,idempotencyKey:"forged",reason:"test",permanent:true,%s}) { id } }`, f.key.ID, f.projectID, forged)
		_, payload, raw = f.postPlugin(f.ownerToken, automationPluginID, query)
		require.Contains(t, payload, "errors", raw)
	}
	require.Zero(t, f.db.APIKeySuspension.Query().CountX(f.setupCtx))
}

func TestPluginAutomationHoldsAndUninstallAdministration(t *testing.T) {
	f := newAutomationEndpointFixture(t, "automation_holds")
	f.setScope(t, f.pages.ID, f.projectID)
	f.setScope(t, f.other.ID, f.projectID)
	code, payload, raw := f.apply(t, automationPluginID, f.ownerToken, "own", f.key)
	hold := automationData(t, code, payload, raw, "applyPluginKeySuspension").(map[string]any)
	id := int(hold["id"].(float64))
	require.EqualValues(t, f.pages.ID, hold["installationId"])
	require.Nil(t, hold["permanent"])
	code, payload, raw = f.apply(t, automationPluginID, f.ownerToken, "own", f.key)
	require.Equal(t, hold["id"], automationData(t, code, payload, raw, "applyPluginKeySuspension").(map[string]any)["id"])
	code, payload, raw = f.apply(t, automationPluginID, f.ownerToken, "outside", f.otherKey)
	require.Equal(t, http.StatusOK, code, raw)
	require.Contains(t, payload, "errors", raw)
	code, payload, raw = f.apply(t, automationPluginID, f.writerToken, "writer", f.key)
	require.Equal(t, http.StatusOK, code, raw)
	require.Equal(t, "FORBIDDEN", graphqlErrorCode(t, payload), raw)

	list := fmt.Sprintf(`query { pluginKeySuspensions(filter:{projectId:%d}) { id evidence } }`, f.projectID)
	code, payload, raw = f.postPlugin(f.ownerToken, automationPluginID, list)
	require.Len(t, automationData(t, code, payload, raw, "pluginKeySuspensions"), 1)
	code, payload, raw = f.postPlugin(f.ownerToken, automationOtherID, list)
	require.Empty(t, automationData(t, code, payload, raw, "pluginKeySuspensions"))
	code, payload, raw = f.postPlugin(f.readerToken, automationPluginID, list)
	require.Equal(t, http.StatusOK, code, raw)
	require.Equal(t, "FORBIDDEN", graphqlErrorCode(t, payload), raw)
	release := fmt.Sprintf(`mutation { releasePluginKeySuspension(id:%d) { id releasedAt } }`, id)
	code, payload, raw = f.postPlugin(f.ownerToken, automationOtherID, release)
	require.Equal(t, http.StatusOK, code, raw)
	require.Contains(t, payload, "errors", raw)
	require.Nil(t, f.db.APIKeySuspension.GetX(f.setupCtx, id).ReleasedAt)
	f.setScope(t, f.pages.ID)
	code, payload, raw = f.postPlugin(f.ownerToken, automationPluginID, release)
	require.Equal(t, http.StatusOK, code, raw)
	require.Contains(t, payload, "errors", raw)
	code, payload, raw = f.postPlugin(f.ownerToken, automationPluginID, list)
	require.Empty(t, automationData(t, code, payload, raw, "pluginKeySuspensions"))
	f.setScope(t, f.pages.ID, f.projectID)
	code, payload, raw = f.postPlugin(f.ownerToken, automationPluginID, release)
	require.NotNil(t, automationData(t, code, payload, raw, "releasePluginKeySuspension").(map[string]any)["releasedAt"])

	code, payload, raw = f.apply(t, automationPluginID, f.ownerToken, "permanent-uninstall", f.key)
	permanent := automationData(t, code, payload, raw, "applyPluginKeySuspension").(map[string]any)
	permanentID := int(permanent["id"].(float64))
	code, payload, raw = f.postAdmin(f.ownerToken, fmt.Sprintf(`mutation { deletePlugin(id:"gid://axonhub/Plugin/%d") }`, f.pages.ID))
	require.Equal(t, true, automationData(t, code, payload, raw, "deletePlugin"))
	stored := f.db.APIKeySuspension.GetX(f.setupCtx, permanentID)
	require.Nil(t, stored.ExpiresAt)
	require.Nil(t, stored.ReleasedAt)
	code, payload, raw = f.postAdmin(f.ownerToken, fmt.Sprintf(`query { apiKeySuspensions(filter:{projectId:%d,activeOnly:true}) { id evidence } }`, f.projectID))
	active := automationData(t, code, payload, raw, "apiKeySuspensions").([]any)
	require.Len(t, active, 1)
	require.EqualValues(t, permanentID, active[0].(map[string]any)["id"])
	code, payload, raw = f.postAdmin(f.ownerToken, fmt.Sprintf(`mutation { releaseAPIKeySuspension(id:%d,reason:"uninstalled policy") { id releasedAt releaseReason } }`, permanentID))
	released := automationData(t, code, payload, raw, "releaseAPIKeySuspension").(map[string]any)
	require.NotNil(t, released["releasedAt"])
	require.Equal(t, "uninstalled policy", released["releaseReason"])
}

func TestPluginAutomationMetricsProjectAndMetricCount(t *testing.T) {
	f := newAutomationEndpointFixture(t, "automation_metrics")
	f.setScope(t, f.pages.ID, f.projectID)
	query := `query($options:PluginCacheUsageOptions!) { pluginCacheUsage(options:$options) { items { keyId count validCount promptTokens cachedTokens samples { promptTokens cachedTokens } } } }`
	options := map[string]any{"projectId": f.projectID, "keyIds": []int{f.key.ID}, "lastRequests": 1, "includeSamples": true}
	code, payload, raw := f.postPluginVars(f.writerToken, automationPluginID, query, map[string]any{"options": options})
	require.Equal(t, http.StatusOK, code, raw)
	require.Equal(t, "FORBIDDEN", graphqlErrorCode(t, payload), raw)
	otherOptions := map[string]any{"projectId": f.otherProjectID, "keyIds": []int{f.otherKey.ID}, "lastRequests": 1}
	code, payload, raw = f.postPluginVars(f.readerToken, automationPluginID, query, map[string]any{"options": otherOptions})
	require.Equal(t, http.StatusOK, code, raw)
	require.Contains(t, payload, "errors", "dashboard read must not exceed the installation's project ceiling: "+raw)
	for i, tokens := range []int64{int64(math.MaxInt32) + 1, 9007199254740991, 9007199254740992, 9007199254740993, math.MaxInt64} {
		t.Run(fmt.Sprint(tokens), func(t *testing.T) {
			at := time.Now().UTC().Add(-time.Minute).Add(time.Duration(i) * time.Second)
			r := f.db.Request.Create().SetProjectID(f.projectID).SetAPIKeyID(f.key.ID).SetModelID("metric-model").SetCreatedAt(at).SetCompletedAt(at).SetStatus(request.StatusCompleted).SetRequestBody(objects.JSONRawMessage(`{}`)).SaveX(f.setupCtx)
			f.db.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(f.key.ID).SetRequestID(r.ID).SetModelID("metric-model").SetCreatedAt(at).SetPromptTokens(tokens).SetPromptCachedTokens(tokens).SetCacheReadStatus("reported").SaveX(f.setupCtx)
			code, payload, raw = f.postPluginVars(f.readerToken, automationPluginID, query, map[string]any{"options": options})
			if tokens > 1<<53-1 {
				require.Equal(t, http.StatusOK, code, raw)
				require.Contains(t, graphqlErrorMessage(t, payload), "safe integer range", raw)
				require.Nil(t, payload["data"], "unsafe metrics must never reach the plugin consumer")
				return
			}
			automationData(t, code, payload, raw, "pluginCacheUsage")
			var exact struct {
				Data struct {
					Page struct {
						Items []struct {
							Count        json.Number
							ValidCount   json.Number
							PromptTokens json.Number
							CachedTokens json.Number
							Samples      []struct {
								PromptTokens json.Number
								CachedTokens json.Number
							}
						}
					} `json:"pluginCacheUsage"`
				}
			}
			decoder := json.NewDecoder(strings.NewReader(raw))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&exact))
			require.Len(t, exact.Data.Page.Items, 1)
			item := exact.Data.Page.Items[0]
			require.Equal(t, "1", item.Count.String())
			require.Equal(t, "1", item.ValidCount.String())
			want := fmt.Sprint(tokens)
			require.Equal(t, want, item.PromptTokens.String())
			require.Equal(t, want, item.CachedTokens.String())
			require.Len(t, item.Samples, 1)
			require.Equal(t, want, item.Samples[0].PromptTokens.String())
			require.Equal(t, want, item.Samples[0].CachedTokens.String())
			require.Contains(t, raw, `"promptTokens":`+want, "MetricCount must remain a JSON number, not a string")
			require.NotContains(t, raw, f.key.Key)
		})
	}
	f.setScope(t, f.pages.ID, f.otherProjectID)
	code, payload, raw = f.postPluginVars(f.readerToken, automationPluginID, query, map[string]any{"options": options})
	require.Equal(t, http.StatusOK, code, raw)
	require.Contains(t, payload, "errors", "dashboard read alone must not exceed the live installation ceiling: "+raw)
}

func TestPluginAutomationMetricCountScalarInt64(t *testing.T) {
	// The scalar remains exact int64. The public metrics wrapper imposes the
	// stricter JavaScript-safe range before reaching this serializer.
	ec := executionContext{}
	for _, value := range []int64{math.MinInt64, 0, int64(math.MaxInt32) + 1, 1 << 53, 1<<53 + 1, math.MaxInt64} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			var buf bytes.Buffer
			ec.marshalNMetricCount2int64(context.Background(), nil, value).MarshalGQL(&buf)
			require.Equal(t, fmt.Sprint(value), buf.String())
		})
	}
}

func TestPluginKVCASRevisionIsolationAndRecreation(t *testing.T) {
	f := newAutomationEndpointFixture(t, "automation_kv_cas")
	query := `mutation($expected:String,$value:String!) { compareAndSetPluginKV(key:"rules",expectedRevision:$expected,value:$value) { swapped entry { value revision } } }`
	cas := func(id string, expected any, value string) map[string]any {
		t.Helper()
		code, payload, raw := f.postPluginVars(f.ownerToken, id, query, map[string]any{"expected": expected, "value": value})
		return automationData(t, code, payload, raw, "compareAndSetPluginKV").(map[string]any)
	}
	created := cas(automationPluginID, nil, "v1")
	require.Equal(t, true, created["swapped"])
	revision := created["entry"].(map[string]any)["revision"].(string)
	require.NotEmpty(t, revision)
	require.Equal(t, false, cas(automationPluginID, nil, "null-overwrite")["swapped"])
	updated := cas(automationPluginID, revision, "v2")
	require.Equal(t, true, updated["swapped"])
	revision2 := updated["entry"].(map[string]any)["revision"].(string)
	require.NotEqual(t, revision, revision2)
	require.Equal(t, false, cas(automationPluginID, revision, "stale")["swapped"])
	other := cas(automationOtherID, nil, "other")
	require.Equal(t, true, other["swapped"])
	require.Equal(t, false, cas(automationOtherID, revision2, "cross-installation")["swapped"])
	require.Equal(t, false, cas(automationPluginID, other["entry"].(map[string]any)["revision"], "reverse-cross")["swapped"])
	code, payload, raw := f.postPlugin(f.ownerToken, automationPluginID, `mutation { deletePluginKV(key:"rules") }`)
	require.Equal(t, true, automationData(t, code, payload, raw, "deletePluginKV"))
	require.Equal(t, false, cas(automationPluginID, revision2, "stale-create")["swapped"])
	recreated := cas(automationPluginID, nil, "v3")
	require.Equal(t, true, recreated["swapped"])
	revision3 := recreated["entry"].(map[string]any)["revision"]
	require.NotEqual(t, revision, revision3)
	require.NotEqual(t, revision2, revision3)
	require.Equal(t, false, cas(automationPluginID, revision2, "aba")["swapped"])
	entry, err := f.manager.KVGet(f.setupCtx, automationPluginID, f.pages.ID, "", "rules")
	require.NoError(t, err)
	require.Equal(t, "v3", entry.Value)
}
