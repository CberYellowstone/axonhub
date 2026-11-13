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

	"github.com/looplj/axonhub/internal/ent"
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
	for _, last := range []int{0, 10} {
		_, err := QueryCacheUsage(f.ctx, client, CacheUsageQuery{ProjectID: f.projectID,
			KeyIDs: []int{key}, End: f.end, Window: time.Hour, LastRequests: last, IncludeSamples: true})
		require.NoError(t, err)
	}
	var details []string
	for _, query := range driver.queries {
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
		}
	}
	observed := strings.Join(details, "\n")
	require.Contains(t, observed, "requests_by_project_key_completed", "the completion index must serve an actual metrics query")
	require.Contains(t, observed, "usage_logs_by_project_request_recent", "final-log association must retain its project/request lookup index")
	for _, detail := range details {
		if strings.Contains(detail, "cache_usage_newer") {
			require.Contains(t, detail, "usage_logs_by_project_request_recent", "each correlated dedup probe must use its request equality prefix")
		}
	}
	t.Log(observed)
}
