// 职责: 在真实 SQLite 上验证无表数据库时钟和 modernc UTC 时间解析。
// 关联文件:
//
//	internal/server/biz/database_clock.go
//	internal/server/biz/api_key_suspension_test.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
//
// 注意事项: 时钟查询没有业务表依赖；测试不创建业务 schema，也不依赖应用时钟推断有效期。
package biz

import (
	"context"
	stdsql "database/sql"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	_ "github.com/looplj/axonhub/internal/pkg/sqlite"
)

func newDatabaseClockTestClient(t *testing.T) (*ent.Client, context.Context) {
	t.Helper()
	db, err := stdsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	client := ent.NewClient(ent.Driver(sql.OpenDB(dialect.SQLite, db)))
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return client, authz.NewSystemContext(ent.NewContext(t.Context(), client))
}

func TestAPIKeySuspensionDatabaseUTCClockEmptySchema(t *testing.T) {
	client, host := newDatabaseClockTestClient(t)
	observed, err := databaseUTCNow(host, client)
	require.NoError(t, err)
	require.Equal(t, time.UTC, observed.Location())
	require.WithinDuration(t, time.Now().UTC(), observed, 2*time.Second)
	_, err = databaseUTCNow(authz.NewUserContext(context.Background(), 1), client)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(host)
	cancel()
	_, err = databaseUTCNow(ctx, client)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAPIKeySuspensionSQLiteClockPreservesModerncFraction(t *testing.T) {
	client, host := newDatabaseClockTestClient(t)
	for _, text := range []string{"2026-11-13 12:34:56.789123456 +0000 UTC", "2026-11-13 12:34:56 +0000 UTC", "2026-11-13T12:34:56.789123456Z"} {
		t.Run(text, func(t *testing.T) {
			value := "'" + text + "'"
			expected := "julianday('2026-11-13 12:34:56.789123456')"
			if text == "2026-11-13 12:34:56 +0000 UTC" {
				expected = "julianday('2026-11-13 12:34:56')"
			}
			expression := databaseUTCTimestampSQL(dialect.SQLite, value)
			query, args := sql.Dialect(dialect.SQLite).Select(sql.As(expression+" - "+expected, "delta")).Query()
			rows := &sql.Rows{}
			require.NoError(t, client.Driver().Query(host, query, args, rows))
			defer rows.Close()
			var observed []struct {
				Delta float64 `json:"delta"`
			}
			require.NoError(t, sql.ScanSlice(rows, &observed))
			require.Len(t, observed, 1)
			require.Zero(t, observed[0].Delta, "removing UTC suffix must not discard fractional seconds")
		})
	}
}
