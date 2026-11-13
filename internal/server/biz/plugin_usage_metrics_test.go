// 职责: 验证插件缓存统计的隔离、可信度、分页和最近 N 条语义。
// 关联文件:
//
//	internal/server/biz/plugin_usage_metrics.go
//	internal/server/biz/usage_log.go
//	internal/ent/schema/usage_log.go
//
// 注意事项:
//
//	unknown 必须占据最近 N 条的位置并终止连续低命中，不得为凑样本跳过。
package biz

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/ent/usagelog"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/llm"
)

type cacheUsageFixture struct {
	client    *ent.Client
	ctx       context.Context
	projectID int
	end       time.Time
}

func newCacheUsageFixture(t *testing.T) cacheUsageFixture {
	t.Helper()
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=0")
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := authz.WithTestBypass(ent.NewContext(context.Background(), client))
	project, err := client.Project.Create().SetName("cache-stats").Save(ctx)
	require.NoError(t, err)
	return cacheUsageFixture{client: client, ctx: ctx, projectID: project.ID, end: time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)}
}

func (f cacheUsageFixture) key(t *testing.T, projectID int) int {
	t.Helper()
	k, err := f.client.APIKey.Create().SetProjectID(projectID).SetName("cache-key").SetKey("fixture-" + uuid.NewString()).Save(f.ctx)
	require.NoError(t, err)
	return k.ID
}

func (f cacheUsageFixture) usage(t *testing.T, keyID int, age time.Duration, prompt, cached int64, status string) *ent.UsageLog {
	t.Helper()
	r := f.completedRequest(t, keyID, age)
	row, err := f.client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(keyID).SetRequestID(r.ID).
		SetModelID("model").SetCreatedAt(f.end.Add(-age)).SetPromptTokens(prompt).SetPromptCachedTokens(cached).
		SetCacheReadStatus(status).Save(f.ctx)
	require.NoError(t, err)
	return row
}

func (f cacheUsageFixture) completedRequest(t *testing.T, keyID int, age time.Duration) *ent.Request {
	t.Helper()
	r, err := f.client.Request.Create().SetProjectID(f.projectID).SetAPIKeyID(keyID).SetModelID("model").
		SetCreatedAt(f.end.Add(-age - time.Second)).SetUpdatedAt(f.end.Add(-age)).
		SetCompletedAt(f.end.Add(-age)).
		SetStatus(request.StatusCompleted).SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
	require.NoError(t, err)
	return r
}

func TestCacheUsage_MissingUsageInterruptsRecentLow(t *testing.T) {
	f := newCacheUsageFixture(t)
	key := f.key(t, f.projectID)
	old := f.usage(t, key, 3*time.Minute, 100, 0, "reported")
	missing := f.completedRequest(t, key, 2*time.Minute)
	newest := f.usage(t, key, time.Minute, 100, 0, "reported")
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key}, End: f.end,
		LastRequests: 2, LowThreshold: lo.ToPtr(0.5), IncludeSamples: true}
	page, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	item := page.Items[0]
	require.Equal(t, int64(2), item.Count)
	require.Equal(t, int64(1), item.ValidCount)
	require.Equal(t, int64(1), item.UnknownCount)
	require.Equal(t, int64(1), item.TrailingLowCount)
	require.False(t, item.SampleComplete)
	require.Len(t, item.Samples, 2)
	require.Equal(t, newest.RequestID, item.Samples[0].RequestID)
	require.Equal(t, missing.ID, item.Samples[1].RequestID)
	require.Equal(t, llm.CacheReadUnknown, item.Samples[1].Status)
	require.Nil(t, item.Samples[1].CacheRate)
	require.NotEqual(t, old.RequestID, item.Samples[1].RequestID)

	q.LastRequests, q.Window = 0, time.Hour
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, int64(3), page.Items[0].Count)
	require.Equal(t, int64(1), page.Items[0].UnknownCount)
	require.Equal(t, int64(1), page.Items[0].TrailingLowCount)
	require.Equal(t, int64(200), page.Items[0].PromptTokens)

	latestMissing := f.completedRequest(t, key, 0)
	q.LastRequests, q.Window = 1, 0
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, latestMissing.ID, page.Items[0].LatestRequestID)
	require.Zero(t, page.Items[0].LatestUsageID)
	require.Zero(t, page.Items[0].TrailingLowCount)
	require.Equal(t, int64(1), page.Items[0].UnknownCount)
	require.Nil(t, page.Items[0].CacheRate)

	q.LastRequests, q.Window, q.IncludeSamples, q.LowThreshold = 0, time.Hour, false, nil
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, int64(4), page.Items[0].Count)
	require.Equal(t, int64(2), page.Items[0].UnknownCount)
	require.Equal(t, latestMissing.ID, page.Items[0].LatestRequestID)
	require.Zero(t, page.Items[0].LatestUsageID)
}

func TestQueryCacheUsage_PerKeyLatestNIncludesUnknown(t *testing.T) {
	f := newCacheUsageFixture(t)
	a, b, empty := f.key(t, f.projectID), f.key(t, f.projectID), f.key(t, f.projectID)
	f.usage(t, a, 5*time.Minute, 100, 0, "reported") // outside latest N
	f.usage(t, a, 4*time.Minute, 100, 0, "unknown")
	latest := f.usage(t, a, time.Minute, 100, 0, "reported")
	f.usage(t, b, 3*time.Minute, 900, 900, "reported")
	f.usage(t, b, 2*time.Minute, 100, 0, "reported")
	page, err := QueryCacheUsage(f.ctx, f.client, CacheUsageQuery{ProjectID: f.projectID, AllKeys: true,
		End: f.end, LastRequests: 2, LowThreshold: lo.ToPtr(0.5), IncludeSamples: true})
	require.NoError(t, err)
	require.Len(t, page.Items, 3)
	x := page.Items[0]
	require.Equal(t, a, x.KeyID)
	require.Equal(t, "cache-key", x.KeyName)
	require.Equal(t, f.projectID, x.ProjectID)
	require.Equal(t, latest.RequestID, x.LatestRequestID)
	require.Equal(t, latest.ID, x.LatestUsageID)
	require.Equal(t, int64(2), x.Count)
	require.Equal(t, int64(1), x.ValidCount)
	require.Equal(t, int64(1), x.UnknownCount)
	require.Equal(t, int64(1), x.TrailingLowCount)
	require.False(t, x.SampleComplete)
	require.False(t, x.EnoughSamples)
	require.Equal(t, latest.RequestID, x.Samples[0].RequestID)
	require.Equal(t, llm.CacheReadUnknown, x.Samples[1].Status)
	require.NotNil(t, x.CacheRate)
	require.Zero(t, *x.CacheRate) // explicit reported zero is a valid rate
	require.Nil(t, x.Samples[1].CacheRate)
	require.InDelta(t, 0.9, *page.Items[1].CacheRate, 1e-12) // weighted, not mean of rates
	require.True(t, page.Items[1].SampleComplete)
	require.Equal(t, int64(1), page.Items[1].TrailingLowCount)
	require.Equal(t, empty, page.Items[2].KeyID)
	require.Zero(t, page.Items[2].Count)
	require.Nil(t, page.Items[2].CacheRate)
	require.False(t, page.Items[2].SampleComplete)
	require.Zero(t, page.Items[2].LatestUsageID)
	serialized, err := json.Marshal(page)
	require.NoError(t, err)
	require.NotContains(t, string(serialized), "fixture-")
}

func TestQueryCacheUsage_FiltersSnapshotPaginationAndDedup(t *testing.T) {
	f := newCacheUsageFixture(t)
	a, b, c := f.key(t, f.projectID), f.key(t, f.projectID), f.key(t, f.projectID)
	otherProject, err := f.client.Project.Create().SetName("other").Save(f.ctx)
	require.NoError(t, err)
	other := f.key(t, otherProject.ID)
	first := f.usage(t, a, time.Minute, 100, 100, "reported")
	// Equal timestamps must prefer the larger usage-log ID for this request.
	_, err = f.client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(a).SetRequestID(first.RequestID).
		SetModelID("model").SetCreatedAt(first.CreatedAt).SetPromptTokens(100).SetCacheReadStatus("unknown").Save(f.ctx)
	require.NoError(t, err)
	f.usage(t, a, 2*time.Hour, 100, 100, "reported")  // max age excludes
	f.usage(t, a, -time.Minute, 100, 100, "reported") // beyond snapshot
	failed := f.usage(t, a, 2*time.Minute, 100, 100, "reported")
	_, err = f.client.Request.UpdateOneID(failed.RequestID).SetStatus(request.StatusFailed).Save(f.ctx)
	require.NoError(t, err)
	filtered := f.usage(t, a, 3*time.Minute, 100, 100, "reported")
	_, err = f.client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(a).SetRequestID(filtered.RequestID).
		SetModelID("model").SetCreatedAt(filtered.CreatedAt).SetSource(usagelog.SourceTest).
		SetPromptTokens(100).SetPromptCachedTokens(100).SetCacheReadStatus("reported").Save(f.ctx)
	require.NoError(t, err)
	q := CacheUsageQuery{ProjectID: f.projectID, AllKeys: true, ExcludeKeyIDs: []int{b}, Limit: 1,
		End: f.end, Window: time.Hour, LastRequests: 5, Sources: []usagelog.Source{usagelog.SourceAPI}, IncludeSamples: true}
	page, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, a, page.Items[0].KeyID)
	require.Equal(t, a, page.NextAfterKeyID)
	require.Equal(t, int64(1), page.Items[0].UnknownCount)
	require.Equal(t, int64(1), page.Items[0].Count)
	filters := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{a}, End: f.end, Window: time.Hour,
		Sources: []usagelog.Source{usagelog.SourceTest}}
	testPage, err := QueryCacheUsage(f.ctx, f.client, filters)
	require.NoError(t, err)
	require.Equal(t, int64(1), testPage.Items[0].Count)
	filters.ModelIDs = []string{"not-this-model"}
	testPage, err = QueryCacheUsage(f.ctx, f.client, filters)
	require.NoError(t, err)
	require.Zero(t, testPage.Items[0].Count)
	filters.ModelIDs, filters.ChannelIDs = nil, []int{99999}
	testPage, err = QueryCacheUsage(f.ctx, f.client, filters)
	require.NoError(t, err)
	require.Zero(t, testPage.Items[0].Count)
	q.AfterKeyID, q.End = page.NextAfterKeyID, page.End
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
	require.Equal(t, c, page.Items[0].KeyID)
	require.Zero(t, page.NextAfterKeyID)
	q.AllKeys, q.ExcludeKeyIDs, q.KeyIDs, q.AfterKeyID = false, nil, []int{other}, 0
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Empty(t, page.Items)
}

func TestQueryCacheUsage_WindowLargeTokensAndInvalid(t *testing.T) {
	f := newCacheUsageFixture(t)
	a := f.key(t, f.projectID)
	empty := f.key(t, f.projectID)
	const large int64 = 1 << 40
	f.usage(t, a, time.Minute, large, large/2, "reported")
	f.usage(t, a, 2*time.Minute, large, large/4, "reported")
	f.usage(t, a, 3*time.Minute, 0, 0, "reported")
	f.usage(t, a, 4*time.Minute, 10, 11, "reported")
	f.usage(t, a, 5*time.Minute, 100, 0, "unknown")
	page, err := QueryCacheUsage(f.ctx, f.client, CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{a, empty}, End: f.end, Window: time.Hour})
	require.NoError(t, err)
	x := page.Items[0]
	require.Equal(t, int64(5), x.Count)
	require.Equal(t, int64(2), x.ValidCount)
	require.Equal(t, int64(2), x.InvalidCount)
	require.Equal(t, int64(1), x.UnknownCount)
	require.Equal(t, 2*large, x.PromptTokens)
	require.Equal(t, large*3/4, x.CachedTokens)
	require.InDelta(t, 0.375, *x.CacheRate, 1e-12)
	require.False(t, x.SampleComplete)
	require.Zero(t, page.Items[1].Count)
	require.Nil(t, page.Items[1].CacheRate)
}

func TestQueryCacheUsage_StableLastNAndOverflow(t *testing.T) {
	f := newCacheUsageFixture(t)
	a := f.key(t, f.projectID)
	f.usage(t, a, time.Minute, 100, 100, "reported")
	newest := f.usage(t, a, time.Minute, 100, 0, "reported")
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{a}, End: f.end, LastRequests: 1, IncludeSamples: true}
	page, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, newest.RequestID, page.Items[0].Samples[0].RequestID)
	require.Zero(t, *page.Items[0].CacheRate)
	b := f.key(t, f.projectID)
	f.usage(t, b, time.Minute, math.MaxInt64, 1, "reported")
	f.usage(t, b, 2*time.Minute, 1, 0, "reported")
	q.KeyIDs, q.LastRequests = []int{b}, 2
	_, err = QueryCacheUsage(f.ctx, f.client, q)
	require.ErrorContains(t, err, "int64")
	q.LastRequests, q.Window = 0, time.Hour
	_, err = QueryCacheUsage(f.ctx, f.client, q)
	require.Error(t, err) // SQL SUM must also fail instead of wrapping int64.
}

func TestQueryCacheUsage_InclusiveWindowBounds(t *testing.T) {
	f := newCacheUsageFixture(t)
	a := f.key(t, f.projectID)
	atEnd := f.usage(t, a, 0, 10, 0, "reported")
	f.usage(t, a, time.Hour, 10, 0, "reported")
	f.usage(t, a, time.Hour+time.Second, 10, 10, "reported")
	f.usage(t, a, -time.Second, 10, 10, "reported")
	page, err := QueryCacheUsage(f.ctx, f.client, CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{a}, End: f.end, Window: time.Hour})
	require.NoError(t, err)
	require.Equal(t, int64(2), page.Items[0].Count)
	require.True(t, page.Items[0].SampleComplete)
	require.Equal(t, atEnd.ID, page.Items[0].LatestUsageID)
	require.Equal(t, atEnd.RequestID, page.Items[0].LatestRequestID)
}

func TestQueryCacheUsage_RequestSettlementTime(t *testing.T) {
	f := newCacheUsageFixture(t)
	key := f.key(t, f.projectID)
	// Usage is persisted before inbound completion. Ordering by log creation
	// would incorrectly choose the second request, despite the later settlement.
	completedAt := f.end.Add(-time.Minute)
	lastRequest, err := f.client.Request.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetModelID("model").
		SetCreatedAt(f.end.Add(-4 * time.Minute)).SetCompletedAt(completedAt).
		SetStatus(request.StatusCompleted).SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
	require.NoError(t, err)
	settledLast, err := f.client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetRequestID(lastRequest.ID).
		SetModelID("model").SetCreatedAt(f.end.Add(-3 * time.Minute)).SetPromptTokens(100).
		SetCacheReadStatus("reported").Save(f.ctx)
	require.NoError(t, err)
	f.usage(t, key, 2*time.Minute, 100, 100, "reported")
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key}, End: f.end,
		LastRequests: 1, IncludeSamples: true}
	page, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, settledLast.RequestID, page.Items[0].LatestRequestID)
	require.Equal(t, settledLast.ID, page.Items[0].LatestUsageID)
	require.True(t, page.Items[0].Samples[0].CreatedAt.Equal(completedAt))
	q.LastRequests, q.Window = 0, 90*time.Second
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Items[0].Count)
	require.Equal(t, int64(1), page.Items[0].ValidCount)
	require.Zero(t, *page.Items[0].CacheRate)
}

func TestQueryCacheUsage_UpdatedAtDoesNotMoveSamples(t *testing.T) {
	f := newCacheUsageFixture(t)
	key := f.key(t, f.projectID)
	old := f.usage(t, key, 3*time.Minute, 100, 100, "reported")
	f.usage(t, key, 2*time.Minute, 100, 0, "reported")
	newest := f.usage(t, key, time.Minute, 100, 0, "reported")
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key}, End: f.end,
		Window: 150 * time.Second, LastRequests: 2, IncludeSamples: true, LowThreshold: lo.ToPtr(0.5)}
	before, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	// Content downloads move updated_at, potentially even beyond the query End.
	_, err = f.client.Request.UpdateOneID(newest.RequestID).SetContentSaved(true).
		SetUpdatedAt(f.end.Add(time.Hour)).Save(f.ctx)
	require.NoError(t, err)
	_, err = f.client.Request.UpdateOneID(old.RequestID).SetUpdatedAt(f.end).Save(f.ctx)
	require.NoError(t, err)
	after, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, newest.RequestID, after.Items[0].LatestRequestID)
	require.Equal(t, int64(2), after.Items[0].TrailingLowCount)
	q.LastRequests = 0
	window, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, int64(2), window.Items[0].Count)
	require.Equal(t, int64(2), window.Items[0].ValidCount)
	require.Zero(t, *window.Items[0].CacheRate)
}

func TestQueryCacheUsage_NormalizesEndTimezone(t *testing.T) {
	f := newCacheUsageFixture(t)
	key := f.key(t, f.projectID)
	f.usage(t, key, time.Minute, 100, 20, "reported")
	query := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key}, End: f.end, Window: time.Hour, IncludeSamples: true}
	utc, err := QueryCacheUsage(f.ctx, f.client, query)
	require.NoError(t, err)
	query.End = query.End.In(time.FixedZone("UTC+8", 8*60*60))
	offset, err := QueryCacheUsage(f.ctx, f.client, query)
	require.NoError(t, err)
	require.Equal(t, utc, offset, "the same instant must select the same request and usage records")
}

func TestQueryCacheUsage_LegacyReportedIsUnknown(t *testing.T) {
	f := newCacheUsageFixture(t)
	key := f.key(t, f.projectID)
	f.usage(t, key, 3*time.Minute, 100, 0, "reported")
	legacyAt := f.end.Add(-2 * time.Minute)
	legacy, err := f.client.Request.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetModelID("model").
		SetCreatedAt(legacyAt).SetUpdatedAt(f.end.Add(time.Hour)).SetStatus(request.StatusCompleted).
		SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
	require.NoError(t, err)
	_, err = f.client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetRequestID(legacy.ID).
		SetModelID("model").SetCreatedAt(legacyAt).SetPromptTokens(100).SetCacheReadStatus("reported").Save(f.ctx)
	require.NoError(t, err)
	f.usage(t, key, time.Minute, 100, 0, "reported")
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key}, End: f.end,
		LastRequests: 2, IncludeSamples: true, LowThreshold: lo.ToPtr(0.5)}
	page, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	item := page.Items[0]
	require.Equal(t, int64(2), item.Count)
	require.Equal(t, int64(1), item.ValidCount)
	require.Equal(t, int64(1), item.UnknownCount)
	require.Equal(t, int64(1), item.TrailingLowCount)
	require.False(t, item.SampleComplete)
	require.Equal(t, int64(100), item.PromptTokens)
	require.Equal(t, legacy.ID, item.Samples[1].RequestID)
	require.True(t, legacyAt.Equal(item.Samples[1].CreatedAt))
	require.Equal(t, llm.CacheReadUnknown, item.Samples[1].Status)
	require.Nil(t, item.Samples[1].CacheRate)
	require.Zero(t, item.Samples[1].PromptTokens)
	q.LastRequests, q.Window = 0, time.Hour
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, int64(3), page.Items[0].Count)
	require.Equal(t, int64(2), page.Items[0].ValidCount)
	require.Equal(t, int64(1), page.Items[0].UnknownCount)
	require.Equal(t, int64(200), page.Items[0].PromptTokens)
	require.Equal(t, int64(1), page.Items[0].TrailingLowCount)
}

func TestQueryCacheUsage_MissingUsageFiltersAndSnapshot(t *testing.T) {
	f := newCacheUsageFixture(t)
	key := f.key(t, f.projectID)
	missing := f.completedRequest(t, key, time.Minute)
	// A usage log arriving beyond End does not retroactively become evidence at
	// this boundary, but the already completed request must still occupy a slot.
	_, err := f.client.UsageLog.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetRequestID(missing.ID).
		SetModelID("model").SetCreatedAt(f.end.Add(time.Second)).SetCacheReadStatus("reported").
		SetPromptTokens(100).SetPromptCachedTokens(100).Save(f.ctx)
	require.NoError(t, err)
	_, err = f.client.Request.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetModelID("model").
		SetCreatedAt(f.end.Add(-time.Minute)).SetUpdatedAt(f.end).SetCompletedAt(f.end).SetSource(request.SourceTest).
		SetStatus(request.StatusCompleted).SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
	require.NoError(t, err)
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key}, End: f.end, Window: time.Hour}
	page, err := QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Items[0].Count)
	require.Equal(t, int64(1), page.Items[0].UnknownCount)
	require.Equal(t, missing.ID, page.Items[0].LatestRequestID)
	require.Zero(t, page.Items[0].LatestUsageID)
	q.Sources, q.ModelIDs = []usagelog.Source{usagelog.SourceTest}, []string{"model"}
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Items[0].Count)
	require.Equal(t, int64(1), page.Items[0].UnknownCount)
	q.ModelIDs = []string{"other"}
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Zero(t, page.Items[0].Count)
	q.ModelIDs, q.ChannelIDs = nil, []int{99999}
	page, err = QueryCacheUsage(f.ctx, f.client, q)
	require.NoError(t, err)
	require.Zero(t, page.Items[0].Count)
}

func TestQueryCacheUsage_ContextAndNoImplicitPrivilege(t *testing.T) {
	f := newCacheUsageFixture(t)
	a := f.key(t, f.projectID)
	q := CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{a}, LastRequests: 1}
	_, err := QueryCacheUsage(context.Background(), f.client, q)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	_, err = QueryCacheUsage(ctx, f.client, q)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCacheUsageQueryValidation(t *testing.T) {
	valid := CacheUsageQuery{ProjectID: 1, KeyIDs: []int{1}, LastRequests: 2}
	for name, mutate := range map[string]func(*CacheUsageQuery){
		"missing project":   func(q *CacheUsageQuery) { q.ProjectID = 0 },
		"no scope":          func(q *CacheUsageQuery) { q.KeyIDs = nil },
		"mixed scope":       func(q *CacheUsageQuery) { q.AllKeys = true },
		"excluded explicit": func(q *CacheUsageQuery) { q.ExcludeKeyIDs = []int{2} },
		"negative cursor":   func(q *CacheUsageQuery) { q.AfterKeyID = -1 },
		"too many keys":     func(q *CacheUsageQuery) { q.KeyIDs = make([]int, 1001) },
		"zero key":          func(q *CacheUsageQuery) { q.KeyIDs = []int{0} },
		"limit":             func(q *CacheUsageQuery) { q.Limit = 101 },
		"lastN":             func(q *CacheUsageQuery) { q.LastRequests = 1001 },
		"missing period":    func(q *CacheUsageQuery) { q.LastRequests = 0 },
		"window":            func(q *CacheUsageQuery) { q.Window = MaxCacheUsageWindow + 1 },
		"samples":           func(q *CacheUsageQuery) { q.MinSamples = 3 },
		"NaN":               func(q *CacheUsageQuery) { q.LowThreshold = lo.ToPtr(math.NaN()) },
		"threshold":         func(q *CacheUsageQuery) { q.LowThreshold = lo.ToPtr(1.1) },
		"source":            func(q *CacheUsageQuery) { q.Sources = []usagelog.Source{"invalid"} },
		"model":             func(q *CacheUsageQuery) { q.ModelIDs = []string{""} },
	} {
		t.Run(name, func(t *testing.T) {
			q := valid
			mutate(&q)
			_, err := normalizeCacheUsageQuery(q)
			require.Error(t, err)
		})
	}
	query, err := normalizeCacheUsageQuery(valid)
	require.NoError(t, err)
	require.Equal(t, []usagelog.Source{usagelog.SourceAPI}, query.Sources)
	valid.KeyIDs = lo.RangeFrom(1, 1000)
	_, err = normalizeCacheUsageQuery(valid)
	require.NoError(t, err) // Filter cardinality is independent of the 100-key page cap.
}

func TestUsageLogCacheReadStatusPersistence(t *testing.T) {
	f := newCacheUsageFixture(t)
	legacyRequest, err := f.client.Request.Create().SetProjectID(f.projectID).SetModelID("model").SetStatus(request.StatusCompleted).SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
	require.NoError(t, err)
	legacy, err := f.client.UsageLog.Create().SetProjectID(f.projectID).SetRequestID(legacyRequest.ID).SetModelID("model").Save(f.ctx)
	require.NoError(t, err)
	require.Equal(t, "unknown", legacy.CacheReadStatus)
	svc := NewUsageLogService(f.client, NewSystemService(SystemServiceParams{Ent: f.client, CacheConfig: xcache.Config{}}), NewChannelServiceForTest(f.client))
	for _, status := range []llm.CacheReadStatus{"", llm.CacheReadUnknown, llm.CacheReadReported} {
		r, err := f.client.Request.Create().SetProjectID(f.projectID).SetModelID("model").SetStatus(request.StatusCompleted).SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
		require.NoError(t, err)
		row, err := svc.CreateUsageLog(f.ctx, CreateUsageLogParams{RequestID: r.ID, ProjectID: f.projectID,
			ActualModelID: "model", Source: usagelog.SourceAPI, Usage: &llm.Usage{PromptTokens: 10, CacheReadStatus: status, PromptTokensDetails: &llm.PromptTokensDetails{AudioTokens: 1}}})
		require.NoError(t, err)
		expected := llm.CacheReadUnknown
		if status == llm.CacheReadReported {
			expected = llm.CacheReadReported
		}
		require.Equal(t, string(expected), row.CacheReadStatus)
		require.Zero(t, row.PromptCachedTokens)
	}
}
