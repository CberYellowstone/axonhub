// 职责: 从持久化的最终请求用量按项目和 API key 查询缓存命中统计。
// 关联文件:
//
//	internal/ent/schema/usage_log.go
//	internal/ent/schema/request.go
//	internal/server/biz/plugin_usage_metrics_batch.go
//	internal/server/biz/usage_log.go
//	internal/server/orchestrator/events.go
//
// 遵循的 Rules:
//
//	.agents/rules/biz-services.md
//	.agents/rules/database-indexes.md
//
// 注意事项:
//
//	调用者必须先校验实时安装授权；本文件不提升调用上下文权限，也不返回凭证明文。
//	先选最近 N 条再判断可信度，unknown 和无效样本均不能被跳过以形成连续低命中。
package biz

import (
	"context"
	"fmt"
	"math"
	"time"

	"entgo.io/ent/dialect/sql"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/predicate"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/ent/usagelog"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/llm"
)

const (
	MaxCacheUsageKeys     = 100
	MaxCacheUsageRequests = 1000
	MaxCacheUsageWindow   = 90 * 24 * time.Hour
)

// CacheUsageQuery is an explicitly authorized project/key scope. KeyIDs and
// AllKeys are mutually exclusive; ExcludeKeyIDs is allowed only with AllKeys.
// Window and LastRequests may be combined: Window then acts as maximum age.
// End is inclusive; zero captures now. Reuse the returned End on every page.
// Only existing (non-deleted) API keys and completed requests are considered.
// Time filters/order use COALESCE(Request.completed_at, Request.created_at),
// then Request.id. Legacy requests without completed_at always count as unknown;
// their fixed creation time only locates them. Sources defaults to api.
type CacheUsageQuery struct {
	ProjectID      int               `json:"projectId"`
	KeyIDs         []int             `json:"keyIds,omitempty"`
	AllKeys        bool              `json:"allKeys,omitempty"`
	ExcludeKeyIDs  []int             `json:"excludeKeyIds,omitempty"`
	AfterKeyID     int               `json:"afterKeyId,omitempty"`
	Limit          int               `json:"limit,omitempty"`
	End            time.Time         `json:"end,omitempty"`
	Window         time.Duration     `json:"window,omitempty"`
	LastRequests   int               `json:"lastRequests,omitempty"`
	MinSamples     int               `json:"minSamples,omitempty"`
	Sources        []usagelog.Source `json:"sources,omitempty"`
	ModelIDs       []string          `json:"modelIds,omitempty"`
	ChannelIDs     []int             `json:"channelIds,omitempty"`
	LowThreshold   *float64          `json:"lowThreshold,omitempty"`
	IncludeSamples bool              `json:"includeSamples,omitempty"`
}

type CacheUsagePage struct {
	End            time.Time       `json:"end"`
	Items          []CacheKeyUsage `json:"items"`
	NextAfterKeyID int             `json:"nextAfterKeyId,omitempty"`
}

type CacheKeyUsage struct {
	KeyID           int    `json:"keyId"`
	KeyName         string `json:"keyName"`
	ProjectID       int    `json:"projectId"`
	LatestRequestID int    `json:"latestRequestId"`
	LatestUsageID   int    `json:"latestUsageId"`
	Count           int64  `json:"count"`
	ValidCount      int64  `json:"validCount"`
	UnknownCount    int64  `json:"unknownCount"`
	InvalidCount    int64  `json:"invalidCount"`
	// Token totals include only reported, valid samples. Rate is token weighted.
	PromptTokens  int64    `json:"promptTokens"`
	CachedTokens  int64    `json:"cachedTokens"`
	CacheRate     *float64 `json:"cacheRate"`
	EnoughSamples bool     `json:"enoughSamples"`
	// SampleComplete requires all selected samples valid, minimum samples, and
	// (in latest-N mode) the complete requested N. Unknown never justifies action.
	SampleComplete    bool               `json:"sampleComplete"`
	TrailingLowCount  int64              `json:"trailingLowCount"`
	TrailingTruncated bool               `json:"trailingTruncated"`
	Samples           []CacheUsageSample `json:"samples,omitempty"`
	SamplesTruncated  bool               `json:"samplesTruncated"`
}

// CacheUsageSample is newest first, ordered by completion time then Request.id.
// CreatedAt exposes completed_at, or fixed created_at for legacy unknown samples.
type CacheUsageSample struct {
	RequestID    int                 `json:"requestId"`
	CreatedAt    time.Time           `json:"createdAt"`
	Status       llm.CacheReadStatus `json:"status"`
	PromptTokens int64               `json:"promptTokens"`
	CachedTokens int64               `json:"cachedTokens"`
	CacheRate    *float64            `json:"cacheRate"`
}

// QueryCacheUsage requires caller authorization for the entire explicit scope.
// It intentionally uses ctx unchanged: a host wrapper must authorize the live
// installation grants first, and only then invoke this fixed operation inside
// authz.RunAsHost. Never expose an unscoped query or a caller-supplied callback.
// End fixes the time boundary, not a database transaction snapshot; callers
// needing strict isolation from concurrent/backdated writes must use a read tx.
func QueryCacheUsage(ctx context.Context, client *ent.Client, query CacheUsageQuery) (CacheUsagePage, error) {
	q, err := normalizeCacheUsageQuery(query)
	if err != nil {
		return CacheUsagePage{}, err
	}
	page := CacheUsagePage{End: q.End, Items: []CacheKeyUsage{}}
	keys := client.APIKey.Query().Where(apikey.ProjectIDEQ(q.ProjectID), apikey.IDGT(q.AfterKeyID))
	if !q.AllKeys {
		keys.Where(apikey.IDIn(q.KeyIDs...))
	}
	if len(q.ExcludeKeyIDs) > 0 {
		keys.Where(apikey.IDNotIn(q.ExcludeKeyIDs...))
	}
	// Do not select the key credential or any other APIKey payload.
	selected, err := keys.Order(apikey.ByID()).Limit(q.Limit+1).Select(apikey.FieldID, apikey.FieldName).All(ctx)
	if err != nil {
		return page, fmt.Errorf("query cache usage keys: %w", err)
	}
	if len(selected) > q.Limit {
		selected = selected[:q.Limit]
		page.NextAfterKeyID = selected[len(selected)-1].ID
	}
	if len(selected) == 0 {
		return page, nil
	}
	if canBatchCacheUsage(ctx, client.Driver().Dialect()) {
		page.Items, err = queryCacheUsageBatch(ctx, client, q, selected)
		if err != nil {
			return CacheUsagePage{}, fmt.Errorf("query cache usage batch: %w", err)
		}
		return page, nil
	}
	// Other callers must retain Ent's Request and UsageLog privacy checks.
	for _, key := range selected {
		item, err := queryCacheKeyUsage(ctx, client, q, key.ID)
		if err != nil {
			return CacheUsagePage{}, fmt.Errorf("query cache usage for key %d: %w", key.ID, err)
		}
		item.KeyName, item.ProjectID = key.Name, q.ProjectID
		page.Items = append(page.Items, item)
	}
	return page, nil
}

func normalizeCacheUsageQuery(q CacheUsageQuery) (CacheUsageQuery, error) {
	invalid := func(reason string) (CacheUsageQuery, error) {
		return q, fmt.Errorf("invalid cache usage query: %s", reason)
	}
	if q.ProjectID <= 0 || q.AfterKeyID < 0 {
		return invalid("positive projectId and nonnegative afterKeyId required")
	}
	if q.AllKeys == (len(q.KeyIDs) > 0) || (!q.AllKeys && len(q.ExcludeKeyIDs) > 0) {
		return invalid("select keyIds or allKeys; exclusions require allKeys")
	}
	for _, ids := range [][]int{q.KeyIDs, q.ExcludeKeyIDs, q.ChannelIDs} {
		if len(ids) > objects.MaxPluginAutomationKeys {
			return invalid("at most 1000 IDs per filter")
		}
		for _, id := range ids {
			if id <= 0 {
				return invalid("filter IDs must be positive")
			}
		}
	}
	if q.Limit == 0 {
		q.Limit = MaxCacheUsageKeys
	}
	if q.Limit < 1 || q.Limit > MaxCacheUsageKeys || q.LastRequests < 0 || q.LastRequests > MaxCacheUsageRequests {
		return invalid("limit must be 1..100 and lastRequests 0..1000")
	}
	if q.Window < 0 || q.Window > MaxCacheUsageWindow || (q.Window == 0 && q.LastRequests == 0) {
		return invalid("window (up to 90 days) or lastRequests is required")
	}
	if q.MinSamples == 0 {
		q.MinSamples = max(1, q.LastRequests)
	}
	if q.MinSamples < 1 || q.MinSamples > MaxCacheUsageRequests || (q.LastRequests > 0 && q.MinSamples > q.LastRequests) {
		return invalid("minSamples must be 1..1000 and not exceed lastRequests")
	}
	if len(q.Sources) > 3 || len(q.ModelIDs) > MaxCacheUsageKeys {
		return invalid("too many source/model filters")
	}
	if len(q.Sources) == 0 {
		q.Sources = []usagelog.Source{usagelog.SourceAPI}
	}
	for _, source := range q.Sources {
		if source != usagelog.SourceAPI && source != usagelog.SourcePlayground && source != usagelog.SourceTest {
			return invalid("unsupported source")
		}
	}
	for _, model := range q.ModelIDs {
		if model == "" || len(model) > 256 {
			return invalid("model IDs must be 1..256 bytes")
		}
	}
	if q.LowThreshold != nil && (math.IsNaN(*q.LowThreshold) || math.IsInf(*q.LowThreshold, 0) || *q.LowThreshold < 0 || *q.LowThreshold > 1) {
		return invalid("lowThreshold must be finite and within 0..1")
	}
	if q.End.IsZero() {
		q.End = time.Now().UTC()
	}
	q.End = q.End.UTC()
	return q, nil
}

func cacheUsageLogPredicates(q CacheUsageQuery, keyID int) []predicate.UsageLog {
	return []predicate.UsageLog{
		usagelog.ProjectIDEQ(q.ProjectID), usagelog.APIKeyIDEQ(keyID), usagelog.CreatedAtLTE(q.End),
		// A request may have duplicate persisted logs (request_id is not unique).
		// Select the latest at the fixed end BEFORE evaluating any usage filters.
		func(s *sql.Selector) {
			newer := sql.Table(usagelog.Table).As("cache_usage_newer")
			next := sql.Select(newer.C(usagelog.FieldID)).From(newer).Where(sql.And(
				sql.EQ(newer.C(usagelog.FieldProjectID), q.ProjectID),
				sql.ColumnsEQ(newer.C(usagelog.FieldRequestID), s.C(usagelog.FieldRequestID)),
				sql.LTE(newer.C(usagelog.FieldCreatedAt), q.End),
				sql.Or(sql.ColumnsGT(newer.C(usagelog.FieldCreatedAt), s.C(usagelog.FieldCreatedAt)),
					sql.And(sql.ColumnsEQ(newer.C(usagelog.FieldCreatedAt), s.C(usagelog.FieldCreatedAt)),
						sql.ColumnsGT(newer.C(usagelog.FieldID), s.C(usagelog.FieldID)))),
			))
			s.Where(sql.Not(sql.Exists(next)))
		},
	}
}

func cacheUsageLogFilters(q CacheUsageQuery) []predicate.UsageLog {
	var filters []predicate.UsageLog
	if len(q.Sources) > 0 {
		filters = append(filters, usagelog.SourceIn(q.Sources...))
	}
	if len(q.ModelIDs) > 0 {
		filters = append(filters, usagelog.ModelIDIn(q.ModelIDs...))
	}
	if len(q.ChannelIDs) > 0 {
		filters = append(filters, usagelog.ChannelIDIn(q.ChannelIDs...))
	}
	return filters
}

func cacheUsageRequestPredicates(q CacheUsageQuery, keyID int) []predicate.Request {
	predicates := []predicate.Request{
		request.ProjectIDEQ(q.ProjectID), request.APIKeyIDEQ(keyID), request.StatusEQ(request.StatusCompleted),
	}
	completed := []predicate.Request{request.CompletedAtNotNil(), request.CompletedAtLTE(q.End)}
	legacy := []predicate.Request{request.CompletedAtIsNil(), request.CreatedAtLTE(q.End)}
	if q.Window > 0 {
		completed = append(completed, request.CompletedAtGTE(q.End.Add(-q.Window)))
		legacy = append(legacy, request.CreatedAtGTE(q.End.Add(-q.Window)))
	}
	predicates = append(predicates, request.Or(request.And(completed...), request.And(legacy...)))
	// Preserve settlement-log filters when usage exists (e.g. actual model), but
	// use request metadata for missing usage rather than dropping the request.
	missing := []predicate.Request{request.Not(request.HasUsageLogsWith(cacheUsageLogPredicates(q, keyID)...))}
	if len(q.Sources) > 0 {
		sources := make([]request.Source, len(q.Sources))
		for i, source := range q.Sources {
			sources[i] = request.Source(source)
		}
		missing = append(missing, request.SourceIn(sources...))
	}
	if len(q.ModelIDs) > 0 {
		missing = append(missing, request.ModelIDIn(q.ModelIDs...))
	}
	if len(q.ChannelIDs) > 0 {
		missing = append(missing, request.ChannelIDIn(q.ChannelIDs...))
	}
	withUsage := append(cacheUsageLogPredicates(q, keyID), cacheUsageLogFilters(q)...)
	return append(predicates, request.Or(request.HasUsageLogsWith(withUsage...), request.And(missing...)))
}

func validCacheUsage() predicate.UsageLog {
	return usagelog.And(usagelog.CacheReadStatusEQ(string(llm.CacheReadReported)), usagelog.PromptTokensGT(0),
		usagelog.PromptCachedTokensGTE(0), func(s *sql.Selector) {
			s.Where(sql.ColumnsLTE(s.C(usagelog.FieldPromptCachedTokens), s.C(usagelog.FieldPromptTokens)))
		})
}

func queryCacheKeyUsage(ctx context.Context, client *ent.Client, q CacheUsageQuery, keyID int) (CacheKeyUsage, error) {
	item := CacheKeyUsage{KeyID: keyID}
	requests := client.Request.Query().Where(cacheUsageRequestPredicates(q, keyID)...)
	logs := client.UsageLog.Query().Where(cacheUsageLogPredicates(q, keyID)...).
		Where(cacheUsageLogFilters(q)...).
		Where(usagelog.HasRequestWith(append(cacheUsageRequestPredicates(q, keyID), request.CompletedAtNotNil())...))
	// Latest-N needs bounded records, including unknowns. Time windows aggregate
	// in SQL and never load unbounded detail rows into the host/plugin process.
	if q.LastRequests == 0 {
		count, err := requests.Clone().Count(ctx)
		if err != nil {
			return item, err
		}
		item.Count = int64(count)
		reported, err := logs.Clone().Where(usagelog.CacheReadStatusEQ(string(llm.CacheReadReported))).Count(ctx)
		if err != nil {
			return item, err
		}
		item.UnknownCount = item.Count - int64(reported)
		var sums []struct {
			Count        int64  `json:"count"`
			PromptTokens *int64 `json:"sum_prompt"`
			CachedTokens *int64 `json:"sum_cached"`
		}
		err = logs.Clone().Where(validCacheUsage()).Aggregate(ent.Count(),
			ent.As(ent.Sum(usagelog.FieldPromptTokens), "sum_prompt"),
			ent.As(ent.Sum(usagelog.FieldPromptCachedTokens), "sum_cached")).Scan(ctx, &sums)
		if err != nil {
			return item, fmt.Errorf("aggregate cache tokens: %w", err)
		}
		if len(sums) > 0 {
			item.ValidCount = sums[0].Count
			if sums[0].PromptTokens != nil {
				item.PromptTokens = *sums[0].PromptTokens
			}
			if sums[0].CachedTokens != nil {
				item.CachedTokens = *sums[0].CachedTokens
			}
		}
		item.InvalidCount = item.Count - item.UnknownCount - item.ValidCount
	}
	{
		rows, err := requests.Clone().Order(func(s *sql.Selector) {
			s.OrderExpr(sql.Expr("COALESCE(" + s.C(request.FieldCompletedAt) + ", " + s.C(request.FieldCreatedAt) + ") DESC"))
		}, request.ByID(sql.OrderDesc())).
			Limit(cacheUsageRecentLimit(q)).Select(request.FieldID, request.FieldCompletedAt, request.FieldCreatedAt).All(ctx)
		if err != nil {
			return item, err
		}
		usageByRequest := make(map[int]*ent.UsageLog, len(rows))
		if len(rows) > 0 {
			ids := make([]int, len(rows))
			for i, row := range rows {
				ids[i] = row.ID
			}
			// At most one final log per selected request, hence <= 1000 rows.
			usageRows, err := client.UsageLog.Query().Where(cacheUsageLogPredicates(q, keyID)...).
				Where(usagelog.RequestIDIn(ids...)).
				Select(usagelog.FieldID, usagelog.FieldRequestID, usagelog.FieldCacheReadStatus,
					usagelog.FieldPromptTokens, usagelog.FieldPromptCachedTokens).All(ctx)
			if err != nil {
				return item, err
			}
			for _, row := range usageRows {
				usageByRequest[row.RequestID] = row
			}
			item.LatestRequestID = rows[0].ID
			if usage := usageByRequest[rows[0].ID]; usage != nil {
				item.LatestUsageID = usage.ID
			}
		}
		trailing := true
		for _, row := range rows {
			if err := addCacheUsageSample(q, &item, cacheSample(row, usageByRequest[row.ID]), &trailing); err != nil {
				return item, err
			}
		}
		finishCacheKeyUsage(q, &item, len(rows), trailing)
	}
	return item, nil
}

func cacheUsageRecentLimit(q CacheUsageQuery) int {
	if q.LastRequests > 0 {
		return q.LastRequests
	}
	if q.IncludeSamples || q.LowThreshold != nil {
		return MaxCacheUsageRequests
	}
	return 1 // Always expose the newest evidence IDs.
}

func addCacheUsageSample(q CacheUsageQuery, item *CacheKeyUsage, sample CacheUsageSample, trailing *bool) error {
	if q.LastRequests > 0 {
		item.Count++
		switch {
		case sample.Status != llm.CacheReadReported:
			item.UnknownCount++
		case sample.CacheRate == nil:
			item.InvalidCount++
		default:
			item.ValidCount++
			if sample.PromptTokens > math.MaxInt64-item.PromptTokens || sample.CachedTokens > math.MaxInt64-item.CachedTokens {
				return fmt.Errorf("cache token aggregate exceeds int64")
			}
			item.PromptTokens += sample.PromptTokens
			item.CachedTokens += sample.CachedTokens
		}
	}
	if q.LowThreshold != nil && *trailing {
		if sample.CacheRate != nil && *sample.CacheRate < *q.LowThreshold {
			item.TrailingLowCount++
		} else {
			*trailing = false
		}
	}
	if q.IncludeSamples {
		item.Samples = append(item.Samples, sample)
	}
	return nil
}

func finishCacheKeyUsage(q CacheUsageQuery, item *CacheKeyUsage, sampleCount int, trailing bool) {
	item.SamplesTruncated = q.IncludeSamples && item.Count > int64(sampleCount)
	item.TrailingTruncated = q.LowThreshold != nil && trailing && item.Count > int64(sampleCount)
	if item.PromptTokens > 0 {
		rate := float64(item.CachedTokens) / float64(item.PromptTokens)
		item.CacheRate = &rate
	}
	item.EnoughSamples = item.ValidCount >= int64(q.MinSamples)
	item.SampleComplete = item.EnoughSamples && item.UnknownCount == 0 && item.InvalidCount == 0 &&
		(q.LastRequests == 0 || item.Count == int64(q.LastRequests))
}

func cacheSample(req *ent.Request, row *ent.UsageLog) CacheUsageSample {
	sample := CacheUsageSample{RequestID: req.ID, CreatedAt: req.CreatedAt, Status: llm.CacheReadUnknown}
	if req.CompletedAt == nil {
		return sample
	}
	sample.CreatedAt = *req.CompletedAt
	if row == nil {
		return sample
	}
	status := llm.CacheReadUnknown
	if row.CacheReadStatus == string(llm.CacheReadReported) {
		status = llm.CacheReadReported
	}
	sample.Status, sample.PromptTokens, sample.CachedTokens = status, row.PromptTokens, row.PromptCachedTokens
	if status == llm.CacheReadReported && row.PromptTokens > 0 && row.PromptCachedTokens >= 0 && row.PromptCachedTokens <= row.PromptTokens {
		rate := float64(row.PromptCachedTokens) / float64(row.PromptTokens)
		sample.CacheRate = &rate
	}
	return sample
}
