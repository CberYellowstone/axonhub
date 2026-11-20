// 职责: 使用项目和 Key 索引批量读取宿主缓存指标与最近样本。
// 关联文件:
//
//	internal/server/biz/plugin_usage_metrics.go
//	internal/server/plugin/automation_usage.go
//	internal/ent/schema/request.go
//	internal/ent/schema/usage_log.go
//
// 遵循的 Rules:
//
//	.agents/rules/biz-services.md
//
// 注意事项: 固定 SQL 只供已授权的 System bypass 操作使用；查询必须使用原事务客户端。
package biz

import (
	"context"
	stdsql "database/sql"
	"fmt"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/privacy"
	"github.com/looplj/axonhub/llm"
)

func canBatchCacheUsage(ctx context.Context, driverDialect string) bool {
	if driverDialect != dialect.Postgres && driverDialect != dialect.SQLite {
		return false // MySQL and TiDB continue to use the Ent query.
	}
	bypass, ok := authz.GetBypassInfo(ctx)
	if authz.RequireSystemPrincipal(ctx) != nil || !ok || !bypass.Principal.IsSystem() {
		return false
	}
	decision, ok := privacy.DecisionFromContext(ctx)
	return ok && decision == nil
}

func queryCacheUsageBatch(ctx context.Context, client *ent.Client, q CacheUsageQuery, keys []*ent.APIKey) ([]CacheKeyUsage, error) {
	// The key page was itself selected with Ent privacy, without credentials.
	driver := client.Driver()
	if !canBatchCacheUsage(ctx, driver.Dialect()) {
		return nil, privacy.Denyf("batch cache usage requires an authorized system context")
	}
	items := make([]CacheKeyUsage, len(keys))
	byKey := make(map[int]int, len(keys))
	keyIDs := make([]int, len(keys))
	for i, key := range keys {
		items[i] = CacheKeyUsage{KeyID: key.ID, KeyName: key.Name, ProjectID: q.ProjectID}
		byKey[key.ID], keyIDs[i] = i, key.ID
	}

	if q.LastRequests == 0 {
		if err := aggregateCacheUsageBatch(ctx, driver, q, keyIDs, items, byKey); err != nil {
			return nil, fmt.Errorf("aggregate cache usage window: %w", err)
		}
	}

	type sampleProgress struct {
		count    int
		trailing bool
	}
	progress := make([]sampleProgress, len(keys))
	query, args := buildCacheUsageBatchSQL(driver.Dialect(), q, keyIDs, false)
	var rows sql.Rows
	if err := driver.Query(ctx, query, args, &rows); err != nil {
		return nil, fmt.Errorf("query recent cache usage: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var keyID int
		var req ent.Request
		var completed stdsql.NullTime
		var usageID, prompt, cached stdsql.NullInt64
		var status stdsql.NullString
		if err := rows.Scan(&keyID, &req.ID, &req.CreatedAt, &completed, &usageID, &status, &prompt, &cached); err != nil {
			return nil, fmt.Errorf("decode recent cache usage: %w", err)
		}
		i, ok := byKey[keyID]
		if !ok {
			return nil, fmt.Errorf("unexpected cache usage key in samples")
		}
		item, state := &items[i], &progress[i]
		if state.count == 0 {
			item.LatestRequestID, item.LatestUsageID = req.ID, int(usageID.Int64)
			state.trailing = true
		}
		state.count++
		if completed.Valid {
			req.CompletedAt = &completed.Time
		}
		var usage *ent.UsageLog
		if usageID.Valid {
			usage = &ent.UsageLog{CacheReadStatus: status.String, PromptTokens: prompt.Int64, PromptCachedTokens: cached.Int64}
		}
		if err := addCacheUsageSample(q, item, cacheSample(&req, usage), &state.trailing); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read recent cache usage: %w", err)
	}
	for i := range items {
		finishCacheKeyUsage(q, &items[i], progress[i].count, progress[i].trailing)
	}
	return items, nil
}

func aggregateCacheUsageBatch(ctx context.Context, driver dialect.Driver, q CacheUsageQuery, keyIDs []int, items []CacheKeyUsage, byKey map[int]int) error {
	query, args := buildCacheUsageBatchSQL(driver.Dialect(), q, keyIDs, true)
	var rows sql.Rows
	if err := driver.Query(ctx, query, args, &rows); err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var keyID int
		var count, reported, valid, prompt, cached int64
		if err := rows.Scan(&keyID, &count, &reported, &valid, &prompt, &cached); err != nil {
			return fmt.Errorf("decode cache usage aggregate (int64): %w", err)
		}
		i, ok := byKey[keyID]
		if !ok {
			return fmt.Errorf("unexpected cache usage key in aggregate")
		}
		item := &items[i]
		item.Count, item.ValidCount = count, valid
		item.UnknownCount, item.InvalidCount = count-reported, reported-valid
		item.PromptTokens, item.CachedTokens = prompt, cached
	}
	return rows.Err()
}

const cacheUsageBatchColumns = `api_key_id, request_id, created_at, completed_at, usage_id, cache_read_status, prompt_tokens, prompt_cached_tokens`

const cacheUsageBatchOrder = `COALESCE(completed_at, created_at) DESC, request_id DESC`

func buildCacheUsageBatchSQL(driverDialect string, q CacheUsageQuery, keyIDs []int, aggregate bool) (string, []any) {
	var b sql.Builder
	b.SetDialect(driverDialect)
	limit := cacheUsageRecentLimit(q)
	switch {
	case aggregate:
		// Aggregate just the scoped candidates in SQL; do not send all window rows to Go.
		b.WriteString(`WITH eligible AS (`)
		appendCacheUsageBranch(&b, q, keyIDs, false, 0)
		b.WriteString(` UNION ALL `)
		appendCacheUsageBranch(&b, q, keyIDs, true, 0)
		b.WriteString(`), classified AS (
 SELECT api_key_id, prompt_tokens, prompt_cached_tokens,
        completed_at IS NOT NULL AND cache_read_status = `).Arg(string(llm.CacheReadReported)).WriteString(` AS reported
 FROM eligible
), validated AS (
 SELECT api_key_id, reported, prompt_tokens, prompt_cached_tokens,
        reported AND prompt_tokens > 0 AND prompt_cached_tokens >= 0 AND prompt_cached_tokens <= prompt_tokens AS valid
 FROM classified
)
SELECT api_key_id, COUNT(*), SUM(CASE WHEN reported THEN 1 ELSE 0 END), SUM(CASE WHEN valid THEN 1 ELSE 0 END),
       SUM(CASE WHEN valid THEN prompt_tokens ELSE 0 END), SUM(CASE WHEN valid THEN prompt_cached_tokens ELSE 0 END)
FROM validated GROUP BY api_key_id`)
	case driverDialect == dialect.Postgres:
		// Read at most N eligible rows from each timestamp branch per key before
		// merging them. The hot completed branch can stop its ordered index scan
		// early, even when LastRequests has no lower time bound.
		b.WriteString(`SELECT recent.* FROM (VALUES `)
		for i, id := range keyIDs {
			if i != 0 {
				b.Comma()
			}
			b.WriteString(`(CAST(`).Arg(id).WriteString(` AS bigint))`)
		}
		b.WriteString(`) AS k(key_id) CROSS JOIN LATERAL (SELECT * FROM ((`)
		appendCacheUsageBranch(&b, q, nil, false, limit)
		b.WriteString(`) UNION ALL (`)
		appendCacheUsageBranch(&b, q, nil, true, limit)
		b.WriteString(`)) AS candidates ORDER BY `).WriteString(cacheUsageBatchOrder).WriteString(` LIMIT `).Arg(limit)
		b.WriteString(`) AS recent ORDER BY api_key_id, `).WriteString(cacheUsageBatchOrder)
	default:
		// SQLite has no LATERAL. Restrict by key/time before ranking in SQL, then
		// return at most N samples per key to the caller.
		b.WriteString(`WITH eligible AS (`)
		appendCacheUsageBranch(&b, q, keyIDs, false, 0)
		b.WriteString(` UNION ALL `)
		appendCacheUsageBranch(&b, q, keyIDs, true, 0)
		b.WriteString(`), ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY api_key_id ORDER BY `).WriteString(cacheUsageBatchOrder)
		b.WriteString(`) AS sample_rank FROM eligible) SELECT `).WriteString(cacheUsageBatchColumns)
		b.WriteString(` FROM ranked WHERE sample_rank <= `).Arg(limit).WriteString(` ORDER BY api_key_id, sample_rank`)
	}
	return b.Query()
}

func appendCacheUsageBranch(b *sql.Builder, q CacheUsageQuery, keyIDs []int, legacy bool, limit int) {
	b.WriteString(`SELECT r.api_key_id, r.id AS request_id, r.created_at, r.completed_at,
       u.id AS usage_id, u.cache_read_status, u.prompt_tokens, u.prompt_cached_tokens
FROM requests r`)
	if legacy && b.Dialect() == dialect.SQLite {
		// SQLite may otherwise choose the project-wide created_at index for
		// a small key page. Both paths use existing schema indexes.
		if q.Window > 0 {
			b.WriteString(` INDEXED BY requests_by_api_key_id_created_at`)
		} else {
			b.WriteString(` INDEXED BY requests_by_project_key_completed`)
		}
	}
	// Choose the latest same-project log BEFORE checking its key or metadata.
	// A latest log with the wrong key is treated as missing, not replaced with an older one.
	const newest = ` FROM usage_logs cache_usage_latest
 WHERE cache_usage_latest.project_id = r.project_id AND cache_usage_latest.request_id = r.id
   AND cache_usage_latest.created_at <= `
	const newestOrder = ` ORDER BY cache_usage_latest.created_at DESC, cache_usage_latest.id DESC LIMIT 1`
	if b.Dialect() == dialect.Postgres {
		b.WriteString(` LEFT JOIN LATERAL (SELECT id, api_key_id, cache_read_status, prompt_tokens, prompt_cached_tokens, source, model_id, channel_id`)
		b.WriteString(newest).Arg(q.End).WriteString(newestOrder).WriteString(`) u ON u.api_key_id = r.api_key_id`)
	} else {
		b.WriteString(` LEFT JOIN usage_logs u ON u.id = (SELECT cache_usage_latest.id`)
		b.WriteString(newest).Arg(q.End).WriteString(newestOrder).WriteString(`) AND u.api_key_id = r.api_key_id`)
	}
	b.WriteString(` WHERE r.project_id = `).Arg(q.ProjectID).WriteString(` AND r.status = 'completed'`)
	if keyIDs == nil {
		b.WriteString(` AND r.api_key_id = k.key_id`)
	} else {
		b.WriteString(` AND r.api_key_id IN (`)
		appendCacheUsageArgs(b, keyIDs)
		b.WriteByte(')')
	}
	stamp := `r.completed_at`
	if legacy {
		b.WriteString(` AND r.completed_at IS NULL`)
		stamp = `r.created_at`
	}
	b.WriteString(` AND `).WriteString(stamp).WriteString(` <= `).Arg(q.End)
	if q.Window > 0 {
		b.WriteString(` AND `).WriteString(stamp).WriteString(` >= `).Arg(q.End.Add(-q.Window))
	}
	b.WriteString(` AND ((u.id IS NOT NULL`)
	appendCacheUsageMetadataFilters(b, q, `u.`)
	b.WriteString(`) OR (u.id IS NULL`)
	appendCacheUsageMetadataFilters(b, q, `r.`)
	b.WriteString(`))`)
	if limit > 0 {
		b.WriteString(` ORDER BY `).WriteString(stamp).WriteString(` DESC, r.id DESC LIMIT `).Arg(limit)
	}
}

func appendCacheUsageMetadataFilters(b *sql.Builder, q CacheUsageQuery, table string) {
	b.WriteString(` AND `).WriteString(table).WriteString(`source IN (`)
	for i, source := range q.Sources {
		if i != 0 {
			b.Comma()
		}
		b.Arg(string(source))
	}
	b.WriteByte(')')
	if len(q.ModelIDs) > 0 {
		b.WriteString(` AND `).WriteString(table).WriteString(`model_id IN (`)
		appendCacheUsageArgs(b, q.ModelIDs)
		b.WriteByte(')')
	}
	if len(q.ChannelIDs) > 0 {
		b.WriteString(` AND `).WriteString(table).WriteString(`channel_id IN (`)
		appendCacheUsageArgs(b, q.ChannelIDs)
		b.WriteByte(')')
	}
}

func appendCacheUsageArgs[T int | string](b *sql.Builder, values []T) {
	for i, v := range values {
		if i != 0 {
			b.Comma()
		}
		b.Arg(v)
	}
}
