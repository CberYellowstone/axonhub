// 职责: 验证 SQLite 旧数据升级插件自动化表结构后的安全默认值与迁移幂等性。
// 关联文件:
//
//	internal/ent/migrate/schema.go
//	internal/ent/schema/plugin.go
//	internal/ent/schema/plugin_storage.go
//	internal/objects/plugin_automation.go
//
// 注意事项: 仅使用独立内存库及 Ent 迁移接口；旧表夹具不修改共享生成定义。
package biz

import (
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"entgo.io/ent/dialect/sql/schema"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	_ "github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/ent/migrate"
	"github.com/looplj/axonhub/internal/ent/pluginstorage"
	"github.com/looplj/axonhub/internal/objects"
)

func TestPluginAutomationMigration(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:"+t.Name()+"?mode=memory&cache=shared&_fk=1")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.SQLite, db)))
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithSystemBypass(authz.NewSystemContext(ent.NewContext(t.Context(), client)), "plugin-automation-migration-test")

	// 从深拷贝中移除本次扩展，避免维护整份易过期的旧版生成 schema。
	legacy := pluginAutomationLegacyTables(t)
	require.NoError(t, migrate.Create(ctx, client.Schema, legacy))
	newTables := []string{"api_key_suspensions", "plugin_notifications", "plugin_schedules"}
	for _, table := range newTables {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&count))
		require.Zero(t, count, "legacy fixture must not contain %s", table)
	}
	for table, column := range pluginAutomationAddedColumns() {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM pragma_table_info(?) WHERE name = ?", table, column).Scan(&count))
		require.Zero(t, count, "legacy fixture must not contain %s.%s", table, column)
	}

	// DML 使用旧列集合，确保没有通过当前 Ent builder 意外写入新字段。
	for _, statement := range []string{
		`INSERT INTO projects (id, name) VALUES (1, 'legacy-project')`,
		`INSERT INTO api_keys (id, key, name, project_id) VALUES (1, 'legacy-key', 'legacy-key', 1)`,
		`INSERT INTO plugins (id, plugin_id, enabled, granted_permissions, config) VALUES (1, 'legacy.automation', true, '["read_usage_metrics"]', '{"keep":"config"}')`,
		`INSERT INTO plugin_storages (id, installation_id, namespace, key, value) VALUES (1, 1, 'default', 'cursor', 'legacy-value')`,
		`INSERT INTO requests (id, model_id, request_body, status, project_id, api_key_id) VALUES (1, 'legacy-model', '{"keep":"request"}', 'completed', 1, 1)`,
		`INSERT INTO usage_logs (id, model_id, prompt_tokens, prompt_cached_tokens, total_tokens, project_id, request_id, api_key_id) VALUES (1, 'legacy-model', 10, 4, 10, 1, 1, 1)`,
	} {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}

	// 与启动升级一致，不启用 DropColumn 或 DropIndex。
	require.NoError(t, client.Schema.Create(ctx))
	assertLegacy := func() {
		t.Helper()
		installed, err := client.Plugin.Get(ctx, 1)
		require.NoError(t, err)
		require.True(t, installed.Enabled)
		require.Equal(t, map[string]any{"keep": "config"}, installed.Config)
		require.Equal(t, []string{objects.PluginPermissionUsageMetrics}, installed.GrantedPermissions)
		require.Empty(t, installed.AutomationScope.Projects)
		require.False(t, installed.AutomationScope.Allows(1, 1), "old grants must not imply background access")
		usage, err := client.UsageLog.Get(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, "unknown", usage.CacheReadStatus)
		require.EqualValues(t, 4, usage.PromptCachedTokens)
		require.EqualValues(t, 10, usage.TotalTokens)
		request, err := client.Request.Get(ctx, 1)
		require.NoError(t, err)
		require.Nil(t, request.CompletedAt, "old completion timestamps must not be invented")
		require.Equal(t, "legacy-model", request.ModelID)
	}
	assertLegacy()
	stored, err := client.PluginStorage.Get(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, stored.Revision)
	require.Equal(t, "legacy-value", stored.Value)

	// 验证旧空 token 可被带 revision 谓词的原子更新认领，且旧 token 不能再次写入。
	// Manager 的令牌生成另由 plugin 包测试覆盖；这里仅验证升级后的存储契约。
	updated, err := client.PluginStorage.Update().Where(pluginstorage.IDEQ(1), pluginstorage.RevisionEQ("")).
		SetRevision("migration-token").SetValue(stored.Value).Save(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, updated)
	updated, err = client.PluginStorage.Update().Where(pluginstorage.IDEQ(1), pluginstorage.RevisionEQ("")).
		SetRevision("stale-token").SetValue("must-not-overwrite").Save(ctx)
	require.NoError(t, err)
	require.Zero(t, updated)

	// 每张新表都实际写入，随后重跑迁移并核对整行，防止仅检查表名掩盖破坏性迁移。
	suspension, err := client.APIKeySuspension.Create().SetInstallationID(1).SetAPIKeyID(1).SetProjectID(1).
		SetPluginID("legacy.automation").SetIdempotencyKeyHash(strings.Repeat("a", 64)).
		SetReason("migration audit").SetEvidence(map[string]any{"keep": "audit"}).SetCreatedBy("migration-test").Save(ctx)
	require.NoError(t, err)
	notification, err := client.PluginNotification.Create().SetInstallationID(1).
		SetIdempotencyHash(strings.Repeat("b", 64)).SetIdempotencyKey("migration-event").SetEventID("event-1").
		SetTargetName("ops").SetPayload(map[string]any{"keep": "outbox"}).Save(ctx)
	require.NoError(t, err)
	schedule, err := client.PluginSchedule.Create().SetInstallationID(1).SetKey("poll").SetIntervalSeconds(60).
		SetRevision("schedule-v1").SetNextRunAt(time.Now().UTC().Truncate(time.Second)).Save(ctx)
	require.NoError(t, err)

	require.NoError(t, client.Schema.Create(ctx))
	assertLegacy()
	stored, err = client.PluginStorage.Get(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, "legacy-value", stored.Value)
	require.Equal(t, "migration-token", stored.Revision)
	gotSuspension, err := client.APIKeySuspension.Get(ctx, suspension.ID)
	require.NoError(t, err)
	require.Equal(t, suspension.String(), gotSuspension.String())
	gotNotification, err := client.PluginNotification.Get(ctx, notification.ID)
	require.NoError(t, err)
	require.Equal(t, notification.String(), gotNotification.String())
	gotSchedule, err := client.PluginSchedule.Get(ctx, schedule.ID)
	require.NoError(t, err)
	require.Equal(t, schedule.String(), gotSchedule.String())
}

func pluginAutomationAddedColumns() map[string]string {
	return map[string]string{
		"plugins":         "automation_scope",
		"plugin_storages": "revision",
		"usage_logs":      "cache_read_status",
		"requests":        "completed_at",
	}
}

func pluginAutomationLegacyTables(t *testing.T) []*schema.Table {
	t.Helper()
	tables, err := schema.CopyTables(migrate.Tables)
	require.NoError(t, err)
	addedColumns := pluginAutomationAddedColumns()
	legacy := make([]*schema.Table, 0, len(tables)-3)
	for _, table := range tables {
		if slices.Contains([]string{"api_key_suspensions", "plugin_notifications", "plugin_schedules"}, table.Name) {
			continue
		}
		removed := addedColumns[table.Name]
		if removed != "" {
			table.Columns = slices.DeleteFunc(table.Columns, func(column *schema.Column) bool { return column.Name == removed })
		}
		table.Indexes = slices.DeleteFunc(table.Indexes, func(index *schema.Index) bool {
			return index.Name == "usage_logs_by_project_request_recent" || slices.ContainsFunc(index.Columns, func(column *schema.Column) bool {
				return removed != "" && column.Name == removed
			})
		})
		legacy = append(legacy, table)
	}
	return legacy
}
