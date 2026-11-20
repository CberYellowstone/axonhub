// 职责: 在独立 PostgreSQL 测试库验证多 Key 聚合、快照去重和没用量的位置语义。
// 关联文件:
//
//	internal/server/biz/plugin_usage_metrics.go
//	internal/server/biz/plugin_usage_metrics_test.go
//
// 注意事项: 仅接受明确命名的本机测试库；不会访问生产 DSN。
package biz

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/usagelog"
)

func newCacheUsagePostgresFixture(t *testing.T, label string) (*sql.DB, cacheUsageFixture) {
	t.Helper()
	dsn := os.Getenv("AXONHUB_CACHE_METRICS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set AXONHUB_CACHE_METRICS_TEST_PG_DSN to a disposable local axonhub_metrics_test database")
	}
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Equal(t, "postgres", parsed.Scheme)
	require.Equal(t, "127.0.0.1", parsed.Hostname())
	require.Equal(t, "/axonhub_metrics_test", parsed.Path)
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithTestBypass(ent.NewContext(t.Context(), client))
	require.NoError(t, client.Schema.Create(ctx))
	project, err := client.Project.Create().SetName(label + "-pg-" + time.Now().UTC().Format("20060102150405.000000000")).Save(ctx)
	require.NoError(t, err)
	return db, cacheUsageFixture{client: client, ctx: ctx, projectID: project.ID, end: time.Date(2026, 11, 19, 2, 0, 0, 0, time.UTC)}
}

func TestCacheUsagePostgresBatch(t *testing.T) {
	_, f := newCacheUsagePostgresFixture(t, "batch")
	client, ctx := f.client, f.ctx
	a, b, empty := f.key(t, f.projectID), f.key(t, f.projectID), f.key(t, f.projectID)
	newest := f.usage(t, a, time.Minute, 100, 20, "reported")
	missing := f.completedRequest(t, a, 2*time.Minute)
	f.usage(t, a, 3*time.Minute, 100, 20, "reported")
	// 同一请求的快照后日志不得改变选择；同时间的较大 ID 优先。
	_, err := client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(a).SetRequestID(newest.RequestID).
		SetModelID("model").SetCreatedAt(newest.CreatedAt).SetPromptTokens(200).SetPromptCachedTokens(100).
		SetCacheReadStatus("reported").Save(ctx)
	require.NoError(t, err)
	_, err = client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(a).SetRequestID(newest.RequestID).
		SetModelID("model").SetCreatedAt(f.end.Add(time.Second)).SetPromptTokens(999).SetPromptCachedTokens(999).
		SetCacheReadStatus("reported").Save(ctx)
	require.NoError(t, err)
	filtered := f.usage(t, b, time.Minute, 100, 0, "reported")
	_, err = client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(b).SetRequestID(filtered.RequestID).
		SetSource(usagelog.SourceTest).SetModelID("model").SetCreatedAt(filtered.CreatedAt).
		SetPromptTokens(100).SetCacheReadStatus("reported").Save(ctx)
	require.NoError(t, err)
	f.usage(t, b, 2*time.Minute, 100, 110, "reported")

	compare := func(q CacheUsageQuery) CacheUsagePage {
		t.Helper()
		reference, err := QueryCacheUsage(ctx, client, q)
		require.NoError(t, err)
		page, err := authz.RunAsHost(ctx, client, "cache-metrics-postgres-test", func(hostCtx context.Context) (CacheUsagePage, error) {
			return QueryCacheUsage(hostCtx, client, q)
		})
		require.NoError(t, err)
		require.Equal(t, reference, page, "System batch and caller-authorized reference must agree")
		return page
	}
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{a, b, empty}, End: f.end, Window: time.Hour}
	page := compare(q)
	require.Len(t, page.Items, 3)
	require.Equal(t, []int{a, b, empty}, []int{page.Items[0].KeyID, page.Items[1].KeyID, page.Items[2].KeyID})
	require.Equal(t, int64(3), page.Items[0].Count)
	require.Equal(t, int64(1), page.Items[0].UnknownCount)
	require.Equal(t, int64(300), page.Items[0].PromptTokens)
	require.Equal(t, int64(120), page.Items[0].CachedTokens)
	require.Equal(t, int64(1), page.Items[1].Count)
	require.Equal(t, int64(1), page.Items[1].InvalidCount)
	require.Empty(t, page.Items[0].Samples)
	require.Zero(t, page.Items[2].Count)

	q.LastRequests, q.MinSamples, q.IncludeSamples, q.LowThreshold = 2, 2, true, lo.ToPtr(0.6)
	page = compare(q)
	require.Len(t, page.Items[0].Samples, 2)
	require.Equal(t, newest.RequestID, page.Items[0].Samples[0].RequestID)
	require.Equal(t, missing.ID, page.Items[0].Samples[1].RequestID)
	require.Equal(t, int64(1), page.Items[0].TrailingLowCount)
	require.False(t, page.Items[0].SampleComplete)
}

// TestCacheUsagePostgresHotPage is opt-in: it adds synthetic rows to the same
// disposable PostgreSQL database, then checks a five-key page near deploy scale.
func TestCacheUsagePostgresHotPage(t *testing.T) {
	if os.Getenv("AXONHUB_CACHE_METRICS_TEST_LOAD") != "1" {
		t.Skip("set AXONHUB_CACHE_METRICS_TEST_LOAD=1 to seed the local load fixture")
	}
	db, f := newCacheUsagePostgresFixture(t, "hot")
	ctx := f.ctx
	keys := make([]int, 5)
	for i := range keys {
		keys[i] = f.key(t, f.projectID)
	}
	background := f.key(t, f.projectID)
	const seed = `
WITH inserted AS (
  INSERT INTO requests (project_id, api_key_id, model_id, status, source, created_at, completed_at, request_body)
  SELECT $1, $2, 'cache-test', 'completed', 'api', $3::timestamptz - i * interval '1 second' - interval '1 second',
         $3::timestamptz - i * interval '1 second', '{}'::jsonb
  FROM generate_series(1, $4::integer) AS i
  RETURNING id, api_key_id, project_id, completed_at
)
INSERT INTO usage_logs (request_id, api_key_id, project_id, model_id, source, created_at, cache_read_status, prompt_tokens, prompt_cached_tokens)
SELECT id, api_key_id, project_id, 'cache-test', 'api', completed_at, 'reported', 100, 90 FROM inserted`
	_, err := db.ExecContext(ctx, seed, f.projectID, background, f.end, 250_000)
	require.NoError(t, err)
	for _, key := range keys {
		_, err = db.ExecContext(ctx, seed, f.projectID, key, f.end, 12_000)
		require.NoError(t, err)
	}
	for _, table := range []string{"requests", "usage_logs"} {
		_, err = db.ExecContext(ctx, "ANALYZE "+table)
		require.NoError(t, err)
	}
	for _, last := range []int{0, 20} {
		q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: keys, End: f.end, Window: 24 * time.Hour, LastRequests: last}
		want := int64(12_000)
		if last != 0 {
			want = int64(last)
			q.IncludeSamples, q.LowThreshold = true, lo.ToPtr(0.95)
		}
		for i := range 3 {
			driver := &cacheMetricsPlanDriver{Driver: f.client.Driver()}
			client := ent.NewClient(ent.Driver(driver))
			started := time.Now()
			page, err := authz.RunAsHost(ctx, client, "cache-metrics-postgres-load-test", func(hostCtx context.Context) (CacheUsagePage, error) {
				deadline, cancel := context.WithTimeout(hostCtx, 7*time.Second)
				defer cancel()
				return QueryCacheUsage(deadline, client, q)
			})
			require.NoError(t, err)
			t.Logf("keys=%d lastRequests=%d run=%d latency=%v", len(keys), last, i+1, time.Since(started))
			require.Len(t, page.Items, len(keys))
			for _, item := range page.Items {
				require.Equal(t, want, item.ValidCount)
				require.Equal(t, want*90, item.CachedTokens)
				require.True(t, item.SampleComplete)
				if last != 0 {
					require.Len(t, item.Samples, last)
					require.Equal(t, want, item.TrailingLowCount)
				}
			}
			if i == 0 {
				maxQueries := 3
				if last != 0 {
					maxQueries = 2
				}
				require.LessOrEqual(t, len(driver.queries), maxQueries)
				assertCacheUsagePostgresPlan(t, db, driver.queries)
			}
		}
	}
}

func assertCacheUsagePostgresPlan(t *testing.T, db *sql.DB, queries []cacheMetricsPlanQuery) {
	t.Helper()
	var plans string
	for _, query := range queries {
		args, ok := query.args.([]any)
		require.True(t, ok)
		ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
		var plan json.RawMessage
		err := db.QueryRowContext(ctx, "EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) "+query.query, args...).Scan(&plan)
		cancel()
		require.NoError(t, err)
		var summaries []struct {
			Ms float64 `json:"Execution Time"`
		}
		require.NoError(t, json.Unmarshal(plan, &summaries))
		require.Len(t, summaries, 1)
		t.Logf("EXPLAIN ANALYZE: %.1f ms", summaries[0].Ms)
		plans += string(plan)
	}
	require.Contains(t, plans, "requests_by_project_key_completed")
	require.Contains(t, plans, "usage_logs_by_project_request_recent")
}
