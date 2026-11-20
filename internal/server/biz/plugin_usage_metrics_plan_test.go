// 职责: 用真实 SQLite 查询计划验证缓存指标查询按项目和 Key 使用窄索引。
// 关联文件:
//
//	internal/server/biz/plugin_usage_metrics.go
//	internal/ent/schema/request.go
//	internal/ent/schema/usage_log.go
//
// 注意事项: 只记录测试查询与计划，不输出参数或业务请求正文。
package biz

import (
	"context"
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/privacy"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/ent/usagelog"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/scopes"
)

type cacheMetricsPlanQuery struct {
	query string
	args  any
}

type cacheMetricsPlanDriver struct {
	dialect.Driver
	queries []cacheMetricsPlanQuery
}

func (d *cacheMetricsPlanDriver) Query(ctx context.Context, query string, args, value any) error {
	d.queries = append(d.queries, cacheMetricsPlanQuery{query: query, args: args})
	return d.Driver.Query(ctx, query, args, value)
}

func TestCacheUsageQueryPlan(t *testing.T) {
	f := newCacheUsageFixture(t)
	key := f.key(t, f.projectID)
	other := f.key(t, f.projectID)
	for i := range 200 {
		id := key
		if i%2 == 0 {
			id = other
		}
		f.usage(t, id, time.Duration(i+1)*time.Minute, 100, 20, "reported")
	}
	driver := &cacheMetricsPlanDriver{Driver: f.client.Driver()}
	client := ent.NewClient(ent.Driver(driver))
	for _, tc := range []struct {
		last, queries int
	}{{0, 3}, {10, 2}} {
		q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key, other}, End: f.end,
			Window: time.Hour, LastRequests: tc.last, IncludeSamples: true}
		reference, err := QueryCacheUsage(f.ctx, f.client, q)
		require.NoError(t, err)
		first := len(driver.queries)
		actual, err := authz.RunAsHost(f.ctx, client, "test-cache-usage-plan", func(ctx context.Context) (CacheUsagePage, error) {
			return QueryCacheUsage(ctx, client, q)
		})
		require.NoError(t, err)
		require.Equal(t, reference, actual)
		require.Len(t, driver.queries[first:], tc.queries, "query count must not grow per key")
	}
	var details []string
	for _, query := range driver.queries {
		require.NotContains(t, query.query, "cache_usage_newer")
		rows := &sql.Rows{}
		require.NoError(t, f.client.Driver().Query(f.ctx, "EXPLAIN QUERY PLAN "+query.query, query.args, rows))
		var plan []struct {
			ID      int    `json:"id"`
			Parent  int    `json:"parent"`
			NotUsed int    `json:"notused"`
			Detail  string `json:"detail"`
		}
		err := sql.ScanSlice(rows, &plan)
		require.NoError(t, rows.Close())
		require.NoError(t, err)
		for _, item := range plan {
			details = append(details, item.Detail)
			if strings.Contains(item.Detail, "cache_usage_latest") {
				require.Contains(t, item.Detail, "usage_logs_by_project_request_recent", "each latest-log lookup must use the project/request index")
			}
			require.NotContains(t, item.Detail, "SCAN cache_usage_latest", "do not scan historical logs")
		}
	}
	observed := strings.Join(details, "\n")
	require.Contains(t, observed, "requests_by_project_key_completed", "the completion index must serve an actual metrics query")
	require.Contains(t, observed, "requests_by_api_key_id_created_at", "legacy window lookups must use a key-specific index")
	require.NotContains(t, observed, "requests_by_project_id_created_at", "do not scan the project's entire window for a key page")
	require.Contains(t, observed, "usage_logs_by_project_request_recent", "final-log association must retain its project/request lookup index")
	t.Log(observed)
}

func TestCacheUsageBatchDoesNotBypassCallerPrivacy(t *testing.T) {
	f := newCacheUsageFixture(t)
	keyID := f.key(t, f.projectID)
	f.usage(t, keyID, time.Minute, 100, 20, "reported")
	key, err := f.client.APIKey.UpdateOneID(keyID).SetScopes([]string{string(scopes.ScopeReadAPIKeys)}).Save(f.ctx)
	require.NoError(t, err)
	ctx := contexts.WithAPIKey(authz.NewAPIKeyContext(t.Context(), keyID, f.projectID), key)
	_, err = f.client.APIKey.Get(ctx, keyID)
	require.NoError(t, err, "the caller can read API keys but not requests or usage logs")
	_, err = QueryCacheUsage(ctx, f.client, CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{keyID}, End: f.end, LastRequests: 1})
	require.ErrorIs(t, err, privacy.Deny)
}

func TestCacheUsageBatchRetainsLegacyEvidenceID(t *testing.T) {
	f := newCacheUsageFixture(t)
	key := f.key(t, f.projectID)
	req, err := f.client.Request.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetModelID("model").
		SetStatus(request.StatusCompleted).SetCreatedAt(f.end).SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
	require.NoError(t, err)
	usage, err := f.client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetRequestID(req.ID).
		SetModelID("model").SetCreatedAt(f.end).SetCacheReadStatus("reported").SetPromptTokens(80).Save(f.ctx)
	require.NoError(t, err)
	for _, last := range []int{0, 1} {
		q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key}, End: f.end,
			Window: time.Hour, LastRequests: last, IncludeSamples: true}
		reference, err := QueryCacheUsage(f.ctx, f.client, q)
		require.NoError(t, err)
		actual, err := authz.RunAsHost(f.ctx, f.client, "test-cache-legacy-id", func(ctx context.Context) (CacheUsagePage, error) {
			return QueryCacheUsage(ctx, f.client, q)
		})
		require.NoError(t, err)
		require.Equal(t, reference, actual)
		require.Equal(t, usage.ID, actual.Items[0].LatestUsageID)
		require.Equal(t, int64(1), actual.Items[0].UnknownCount)
		require.Zero(t, actual.Items[0].Samples[0].PromptTokens)
	}
}

func TestCacheUsageBatchDeduplicatesBeforeFiltering(t *testing.T) {
	f := newCacheUsageFixture(t)
	key, wrongKey := f.key(t, f.projectID), f.key(t, f.projectID)
	other, err := f.client.Project.Create().SetName("other-metrics-project").Save(f.ctx)
	require.NoError(t, err)
	latest := f.usage(t, key, time.Minute, 100, 20, "reported")
	mismatched := f.usage(t, key, 2*time.Minute, 100, 20, "reported")
	filtered := f.usage(t, key, 3*time.Minute, 100, 20, "reported")
	addDuplicate := func(log *ent.UsageLog, projectID, keyID int, at time.Time, source usagelog.Source) {
		t.Helper()
		_, err := f.client.UsageLog.Create().SetProjectID(projectID).SetAPIKeyID(keyID).SetRequestID(log.RequestID).
			SetModelID("different-model").SetCreatedAt(at).SetSource(source).SetCacheReadStatus("reported").SetPromptTokens(200).Save(f.ctx)
		require.NoError(t, err)
	}
	addDuplicate(latest, other.ID, key, f.end, usagelog.SourceAPI)
	addDuplicate(latest, f.projectID, key, f.end.Add(time.Second), usagelog.SourceAPI)
	addDuplicate(mismatched, f.projectID, wrongKey, mismatched.CreatedAt, usagelog.SourceAPI)
	addDuplicate(filtered, f.projectID, key, filtered.CreatedAt, usagelog.SourceTest)
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key}, End: f.end, Window: time.Hour,
		ModelIDs: []string{"model"}, IncludeSamples: true}
	for _, last := range []int{0, 2} {
		q.LastRequests = last
		reference, err := QueryCacheUsage(f.ctx, f.client, q)
		require.NoError(t, err)
		page, err := authz.RunAsHost(f.ctx, f.client, "test-cache-usage-dedup", func(ctx context.Context) (CacheUsagePage, error) {
			return QueryCacheUsage(ctx, f.client, q)
		})
		require.NoError(t, err)
		require.Equal(t, reference, page)
		item := page.Items[0]
		require.Equal(t, int64(2), item.Count)
		require.Equal(t, int64(1), item.UnknownCount)
		require.Equal(t, latest.ID, item.LatestUsageID)
		require.Equal(t, mismatched.RequestID, item.Samples[1].RequestID)
	}
}

func TestCacheUsageBatchUsesPassedTransaction(t *testing.T) {
	f := newCacheUsageFixture(t)
	keyID := f.key(t, f.projectID)
	tx, err := f.client.Tx(f.ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
	inserted := f
	inserted.client = tx.Client()
	inserted.ctx = ent.NewContext(f.ctx, inserted.client)
	usage := inserted.usage(t, keyID, time.Minute, 100, 20, "reported")
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{keyID}, End: f.end, Window: time.Hour, IncludeSamples: true}
	page, err := authz.RunAsHost(inserted.ctx, inserted.client, "test-cache-usage-transaction", func(ctx context.Context) (CacheUsagePage, error) {
		return QueryCacheUsage(ctx, inserted.client, q)
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Items[0].ValidCount)
	require.Equal(t, usage.ID, page.Items[0].LatestUsageID, "both metrics reads must see the same uncommitted fixture data")
}
