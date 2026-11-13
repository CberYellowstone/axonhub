// 职责: 将插件缓存指标查询限定在即时授权的项目和 Key 范围内。
// 关联文件:
//
//	internal/server/biz/plugin_usage_metrics.go
//	internal/server/biz/plugin_automation_access.go
//	internal/objects/plugin_automation.go
//
// 注意事项: 插件参数只能缩小 Owner 授权范围；指标读取使用一致的数据库快照。
package plugin

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"slices"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/privacy"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
)

func restrictCacheUsage(scope objects.PluginAutomationScope, query biz.CacheUsageQuery) (biz.CacheUsageQuery, error) {
	var granted *objects.PluginProjectAccess
	for _, project := range scope.Projects {
		if project.ProjectID == query.ProjectID {
			granted = &project
			break
		}
	}
	if granted == nil {
		return query, privacy.Denyf("project is outside the plugin automation scope")
	}
	if query.AllKeys {
		if len(query.KeyIDs) > 0 {
			return query, fmt.Errorf("select keyIds or allKeys, not both")
		}
		if granted.AllKeys {
			for _, excluded := range granted.ExcludedAPIKeyIDs {
				if !slices.Contains(query.ExcludeKeyIDs, excluded) {
					query.ExcludeKeyIDs = append(slices.Clone(query.ExcludeKeyIDs), excluded)
				}
			}
		} else {
			query.AllKeys = false
			for _, id := range granted.APIKeyIDs {
				if !slices.Contains(query.ExcludeKeyIDs, id) {
					query.KeyIDs = append(query.KeyIDs, id)
				}
			}
			query.ExcludeKeyIDs = nil
		}
	} else {
		query.KeyIDs = slices.Clone(query.KeyIDs)
		for _, id := range query.KeyIDs {
			if !scope.Allows(query.ProjectID, id) {
				return query, privacy.Denyf("key is outside the plugin automation scope")
			}
		}
	}
	return query, nil
}

// CacheUsage returns aggregate metrics only. Installation identity must already
// be authenticated by the host call or delegated page endpoint.
func (m *Manager) CacheUsage(ctx context.Context, installationID int, query biz.CacheUsageQuery) (biz.CacheUsagePage, error) {
	// Establish the caller's installation identity before crossing the host boundary.
	if _, err := biz.AuthorizePluginAutomation(ctx, m.client(ctx), installationID, objects.PluginPermissionUsageMetrics); err != nil {
		return biz.CacheUsagePage{}, err
	}
	return authz.RunAsHost(ctx, m.client(ctx), "plugin-cache-usage", func(hostCtx context.Context) (biz.CacheUsagePage, error) {
		client := m.client(hostCtx)
		if client.IsTransactional() {
			return authorizedCacheUsage(hostCtx, client, installationID, query)
		}
		// A stable snapshot keeps separate sample/count/token queries coherent.
		isolation := stdsql.LevelRepeatableRead
		if client.Driver().Dialect() == "sqlite3" {
			isolation = stdsql.LevelSerializable
		}
		tx, err := client.BeginTx(hostCtx, &stdsql.TxOptions{Isolation: isolation, ReadOnly: false})
		if err != nil {
			return biz.CacheUsagePage{}, fmt.Errorf("begin cache metrics snapshot: %w", err)
		}
		defer tx.Rollback()
		result, err := authorizedCacheUsage(ent.NewContext(hostCtx, tx.Client()), tx.Client(), installationID, query)
		if err != nil {
			return biz.CacheUsagePage{}, err
		}
		if err := tx.Commit(); err != nil {
			return biz.CacheUsagePage{}, fmt.Errorf("finish cache metrics snapshot: %w", err)
		}
		return result, nil
	})
}

func authorizedCacheUsage(ctx context.Context, client *ent.Client, installationID int, query biz.CacheUsageQuery) (biz.CacheUsagePage, error) {
	// Scope and metrics belong to the same snapshot. A concurrent grant change
	// cannot combine a new key set with an earlier authorization decision.
	installed, err := biz.AuthorizePluginAutomation(ctx, client, installationID, objects.PluginPermissionUsageMetrics)
	if err != nil {
		return biz.CacheUsagePage{}, err
	}
	query, err = restrictCacheUsage(installed.AutomationScope, query)
	if err != nil {
		return biz.CacheUsagePage{}, err
	}
	if !query.AllKeys && len(query.KeyIDs) == 0 {
		return biz.CacheUsagePage{}, fmt.Errorf("no keys are authorized by the plugin automation scope")
	}
	if query.End.IsZero() {
		query.End, err = scheduleDBNow(ctx, client)
		if err != nil {
			return biz.CacheUsagePage{}, err
		}
	}
	page, err := biz.QueryCacheUsage(ctx, client, query)
	if err != nil {
		return biz.CacheUsagePage{}, err
	}
	if err := validateCacheUsageNumbers(page); err != nil {
		return biz.CacheUsagePage{}, err
	}
	return page, nil
}

// Plugin consumers use JSON numbers in JavaScript. Reject an unrepresentable
// result before serialization so an automation rule never evaluates rounded data.
func validateCacheUsageNumbers(page biz.CacheUsagePage) error {
	const maxSafeInteger int64 = 1<<53 - 1
	check := func(values ...int64) error {
		for _, value := range values {
			if value < -maxSafeInteger || value > maxSafeInteger {
				return fmt.Errorf("cache metrics exceed the JavaScript safe integer range; narrow the query")
			}
		}
		return nil
	}
	for _, item := range page.Items {
		if err := check(item.Count, item.ValidCount, item.UnknownCount, item.InvalidCount,
			item.PromptTokens, item.CachedTokens, item.TrailingLowCount); err != nil {
			return err
		}
		for _, sample := range item.Samples {
			if err := check(sample.PromptTokens, sample.CachedTokens); err != nil {
				return err
			}
		}
	}
	return nil
}
