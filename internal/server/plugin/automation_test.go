// 职责: 验证插件自动化授权只能由直接 Owner 配置并始终约束到项目。
// 关联文件:
//
//	internal/server/plugin/automation.go
//	internal/objects/plugin_automation.go
package plugin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
)

func TestAutomationScopeOwnerAndProjectBoundary(t *testing.T) {
	db := newPluginDB(t)
	m := newTestManager(t, db)
	installed := installUI(t, m, uiZip(t, nil, nil))
	setup := pluginCtx()
	project := db.Project.Create().SetName("automation-project").SaveX(setup)
	other := db.Project.Create().SetName("other-project").SaveX(setup)
	key := db.APIKey.Create().SetName("target").SetKey("test-only-secret").SetProjectID(project.ID).SaveX(setup)
	foreign := db.APIKey.Create().SetName("foreign").SetKey("other-test-secret").SetProjectID(other.ID).SaveX(setup)
	scope := objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: project.ID, APIKeyIDs: []int{key.ID}}}}
	ownerCtx := authz.NewUserContext(contexts.WithUser(context.Background(), &ent.User{ID: 1, IsOwner: true}), 1)
	require.ErrorContains(t, m.SetAutomationScope(context.Background(), installed.ID, scope), "owner")
	require.NoError(t, m.SetAutomationScope(ownerCtx, installed.ID, scope))
	got := db.Plugin.GetX(setup, installed.ID)
	require.True(t, got.AutomationScope.Allows(project.ID, key.ID))
	require.False(t, got.AutomationScope.Allows(other.ID, foreign.ID))
	bad := objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: project.ID, APIKeyIDs: []int{foreign.ID}}}}
	require.ErrorContains(t, m.SetAutomationScope(ownerCtx, installed.ID, bad), "specified project")
	got = db.Plugin.GetX(setup, installed.ID)
	require.True(t, got.AutomationScope.Allows(project.ID, key.ID))
	require.NoError(t, m.SetAutomationScope(ownerCtx, installed.ID, objects.PluginAutomationScope{}))
	require.False(t, db.Plugin.GetX(setup, installed.ID).AutomationScope.Allows(project.ID, key.ID))
}

func TestAutomationScopeKVRechecksNewSuspensionGrant(t *testing.T) {
	db := newPluginDB(t)
	m := newTestManager(t, db)
	installed := installUI(t, m, uiZip(t, func(_ *testing.T, manifest map[string]any) {
		manifest["permissions"] = []string{objects.PluginPermissionKeySuspensions}
	}, nil))
	u := &ent.User{ID: 7, Scopes: []string{"write_plugins"}}
	parent := contexts.WithUser(authz.NewUserContext(context.Background(), u.ID), u)
	delegated, err := authz.DelegatePlugin(parent, installed.ID, nil)
	require.NoError(t, err)
	// The ordinary write qualification was computed before the Owner added the
	// suspension grant. It must not authorize edits to privileged rules afterward.
	ctx := WithCall(delegated, Call{ManifestID: installed.PluginID, InstallationID: installed.ID, KVWritable: true})
	_, err = m.KVSet(ctx, installed.PluginID, installed.ID, "", "rules", "before", 0)
	require.NoError(t, err)
	db.Plugin.UpdateOneID(installed.ID).SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions}).SaveX(pluginCtx())
	_, err = m.KVSet(ctx, installed.PluginID, installed.ID, "", "rules", "after", 0)
	require.ErrorContains(t, err, "owner")
	_, err = m.KVCompareAndSet(ctx, installed.PluginID, installed.ID, "", "new-rules", nil, "after", 0)
	require.ErrorContains(t, err, "owner")
	_, err = m.KVDelete(ctx, installed.PluginID, installed.ID, "", "rules")
	require.ErrorContains(t, err, "owner")
	got, err := m.KVGet(ctx, installed.PluginID, installed.ID, "", "rules")
	require.NoError(t, err)
	require.Equal(t, "before", got.Value)
}
