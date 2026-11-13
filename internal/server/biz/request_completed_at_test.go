// 职责: 验证请求首次成功完成时间与状态原子落库且不随重试或内容更新变化。
// 关联文件:
//
//	internal/server/biz/request.go
//	internal/ent/schema/request.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//	.agents/rules/biz-services.md
//
// 注意事项:
//
//	completed_at 由数据库 UTC 时钟首次赋值；历史空值不在普通内容更新时补填。
package biz

import (
	"testing"
	"time"

	"entgo.io/ent/dialect"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/objects"
)

func TestRequestCompletion_FirstTimestampSurvivesUpdates(t *testing.T) {
	f := newCacheUsageFixture(t)
	svc := &RequestService{AbstractService: &AbstractService{db: f.client}}
	key := f.key(t, f.projectID)
	r, err := f.client.Request.Create().SetProjectID(f.projectID).SetAPIKeyID(key).SetModelID("model").
		SetStatus(request.StatusPending).SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
	require.NoError(t, err)
	require.Nil(t, r.CompletedAt)
	require.NoError(t, svc.saveRequestUpdate(f.ctx, r.ID, f.client.Request.Update().SetStatus(request.StatusProcessing)))
	r, err = f.client.Request.Get(f.ctx, r.ID)
	require.NoError(t, err)
	require.Nil(t, r.CompletedAt)
	before := time.Now().UTC().Add(-time.Second)
	require.NoError(t, svc.saveRequestUpdate(f.ctx, r.ID, f.client.Request.Update().SetStatus(request.StatusCompleted)))
	r, err = f.client.Request.Get(f.ctx, r.ID)
	require.NoError(t, err)
	require.Equal(t, request.StatusCompleted, r.Status)
	require.NotNil(t, r.CompletedAt)
	first := *r.CompletedAt
	require.False(t, first.Before(before))
	require.False(t, first.After(time.Now().UTC().Add(time.Second)))
	count, err := f.client.Request.Query().Where(request.IDEQ(r.ID), request.CompletedAtEQ(first)).Count(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count, "database timestamp must compare exactly to its bound Go value")
	page, err := QueryCacheUsage(f.ctx, f.client, CacheUsageQuery{ProjectID: f.projectID, KeyIDs: []int{key},
		End: first, Window: time.Hour})
	require.NoError(t, err)
	require.Equal(t, int64(1), page.Items[0].Count, "completion at inclusive End must not disappear")

	// Both a duplicate completion and a payload-only update preserve the marker.
	later := first.Add(time.Hour)
	require.NoError(t, svc.saveRequestUpdate(f.ctx, r.ID,
		f.client.Request.Update().SetStatus(request.StatusCompleted).SetUpdatedAt(later)))
	require.NoError(t, svc.saveRequestUpdate(f.ctx, r.ID,
		f.client.Request.Update().SetContentSaved(true).SetUpdatedAt(later.Add(time.Hour))))
	r, err = f.client.Request.Get(f.ctx, r.ID)
	require.NoError(t, err)
	require.True(t, first.Equal(*r.CompletedAt))
	require.True(t, r.ContentSaved)
	require.True(t, later.Add(time.Hour).Equal(r.UpdatedAt))
}

func TestRequestCompletion_PreserveExistingAndLegacyTimestamps(t *testing.T) {
	f := newCacheUsageFixture(t)
	svc := &RequestService{AbstractService: &AbstractService{db: f.client}}
	key := f.key(t, f.projectID)
	existing := f.completedRequest(t, key, time.Hour)
	require.NoError(t, svc.saveRequestUpdate(f.ctx, existing.ID, f.client.Request.Update().SetStatus(request.StatusCompleted)))
	r, err := f.client.Request.Get(f.ctx, existing.ID)
	require.NoError(t, err)
	require.True(t, existing.CompletedAt.Equal(*r.CompletedAt))

	legacy, err := f.client.Request.Create().SetProjectID(f.projectID).SetModelID("model").
		SetStatus(request.StatusCompleted).SetRequestBody(objects.JSONRawMessage(`{}`)).Save(f.ctx)
	require.NoError(t, err)
	require.NoError(t, svc.saveRequestUpdate(f.ctx, legacy.ID, f.client.Request.Update().SetContentSaved(true)))
	r, err = f.client.Request.Get(f.ctx, legacy.ID)
	require.NoError(t, err)
	require.Nil(t, r.CompletedAt)
}

func TestRequestCompletion_DatabaseClockSQL(t *testing.T) {
	for dialectName, expected := range map[string]string{
		dialect.Postgres: "clock_timestamp()",
		dialect.MySQL:    "UTC_TIMESTAMP(6)",
		dialect.SQLite:   "RTRIM(RTRIM(STRFTIME('%Y-%m-%d %H:%M:%f', 'now'), '0'), '.') || ' +0000 UTC'",
	} {
		t.Run(dialectName, func(t *testing.T) {
			require.Equal(t, expected, requestCompletionClockSQL(dialectName))
		})
	}
}
