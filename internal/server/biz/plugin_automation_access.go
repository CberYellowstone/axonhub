// 职责: 为插件后台自动化校验真实调用身份及数据库中的即时授权。
// 关联文件:
//
//	internal/objects/plugin_automation.go
//	internal/server/biz/api_key_suspension.go
//	internal/authz/host.go
//
// 遵循的 Rules:
//
//	.agents/rules/biz-services.md
//
// 注意事项: installationID 只是目标，不是身份；普通用户不能据此取得插件权限。
package biz

import (
	"context"
	"fmt"
	"slices"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/authz/identity"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/plugin"
	"github.com/looplj/axonhub/internal/ent/privacy"
	"github.com/looplj/axonhub/internal/objects"
)

// AuthorizePluginAutomation verifies both the caller and live installation grants.
// The returned scope is an owner-approved ceiling, not permission to run arbitrary
// caller code as host. Callers must additionally check each current key/project.
func AuthorizePluginAutomation(ctx context.Context, client *ent.Client, installationID int, permission string) (*ent.Plugin, error) {
	p, ok := authz.GetPrincipal(ctx)
	if !ok || installationID <= 0 {
		return nil, privacy.Denyf("plugin automation requires an authenticated host or delegation")
	}
	if permission != objects.PluginPermissionKeySuspensions && permission != objects.PluginPermissionUsageMetrics && permission != "webhook" {
		return nil, privacy.Denyf("unknown plugin automation permission")
	}
	if !p.IsSystem() && !(p.IsPlugin() && p.PluginInstallationID == installationID && p.UserID != nil && identity.PluginGranted(ctx, *p.UserID, permission)) {
		return nil, privacy.Denyf("plugin automation caller does not match installation")
	}
	if txClient := ent.FromContext(ctx); txClient != nil {
		client = txClient
	}
	return authz.RunAsHost(ctx, client, "plugin automation live authorization", func(hostCtx context.Context) (*ent.Plugin, error) {
		installed, err := client.Plugin.Query().Where(plugin.IDEQ(installationID), plugin.EnabledEQ(true), plugin.DeletedAtEQ(0)).
			Select(plugin.FieldID, plugin.FieldPluginID, plugin.FieldEnabled, plugin.FieldDeletedAt,
				plugin.FieldGrantedPermissions, plugin.FieldAutomationScope).Only(hostCtx)
		if err != nil {
			return nil, fmt.Errorf("plugin installation unavailable: %w", err)
		}
		if !slices.Contains(installed.GrantedPermissions, permission) {
			return nil, privacy.Denyf("plugin automation permission is not currently granted")
		}
		if err := installed.AutomationScope.Validate(); err != nil {
			return nil, privacy.Denyf("invalid plugin automation scope")
		}
		return installed, nil
	})
}
