// 职责: 管理插件后台自动化的 Owner 授权范围及配置写入边界。
// 关联文件:
//
//	internal/objects/plugin_automation.go
//	internal/ent/schema/plugin.go
//	internal/server/gql/plugin_automation.resolvers.go
//	internal/server/plugin/manager.go
//
// 注意事项: 自动化范围由 Owner 单独授予，插件配置及 KV 不能扩大授权。
package plugin

import (
	"context"
	"fmt"
	"slices"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/scopes"
)

// SetAutomationScope replaces the complete, owner-approved automation ceiling.
// An empty scope revokes access while retaining existing suspension audit rows.
func (m *Manager) SetAutomationScope(ctx context.Context, id int, scope objects.PluginAutomationScope) error {
	if !scopes.IsDirectOwner(ctx) {
		return fmt.Errorf("only a direct owner can grant plugin automation scope")
	}
	if id <= 0 {
		return fmt.Errorf("plugin installation id must be positive")
	}
	if err := scope.Validate(); err != nil {
		return err
	}
	_, err := authz.RunAsHost(ctx, m.client(ctx), "plugin-automation-grant", func(hostCtx context.Context) (bool, error) {
		return true, m.runInTx(hostCtx, func(txCtx context.Context) error {
			if _, err := m.oneRecord(txCtx, id); err != nil {
				return err
			}
			client := m.client(txCtx)
			for _, grant := range scope.Projects {
				if _, err := client.Project.Get(txCtx, grant.ProjectID); err != nil {
					return fmt.Errorf("automation scope project is unavailable: %w", err)
				}
				for _, ids := range [][]int{grant.APIKeyIDs, grant.ExcludedAPIKeyIDs} {
					if len(ids) == 0 {
						continue
					}
					n, err := client.APIKey.Query().Where(apikey.ProjectIDEQ(grant.ProjectID), apikey.IDIn(ids...)).Count(txCtx)
					if err != nil {
						return fmt.Errorf("validate automation scope keys: %w", err)
					}
					if n != len(ids) {
						return fmt.Errorf("automation scope keys must exist in their specified project")
					}
				}
			}
			return client.Plugin.UpdateOneID(id).SetAutomationScope(scope).Exec(txCtx)
		})
	})
	if err != nil {
		return err
	}
	return m.finishWrite(ctx)
}

// RequireSettingsAuthority protects automation-driving configuration from users
// who may edit ordinary plugin settings but cannot manage API keys.
func (m *Manager) RequireSettingsAuthority(ctx context.Context, id int) error {
	row, err := authz.RunAsHost(ctx, m.client(ctx), "plugin-automation-settings", func(hostCtx context.Context) (pluginRecord, error) {
		return m.oneRecord(hostCtx, id)
	})
	if err != nil {
		return err
	}
	if slices.Contains(row.Granted, objects.PluginPermissionKeySuspensions) && !scopes.IsDirectOwner(ctx) {
		return fmt.Errorf("only a direct owner can change key-suspending plugin settings")
	}
	return nil
}
