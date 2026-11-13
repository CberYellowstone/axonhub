// 职责: 为安全期限提供数据库 UTC 时钟及跨方言时间比较表达式。
// 关联文件:
//
//	internal/server/biz/api_key_suspension.go
//	internal/server/biz/api_key_suspension_test.go
//
// 遵循的 Rules:
//
//	.agents/rules/biz-services.md
//
// 注意事项: 只执行固定无表时钟查询；优先复用上下文事务，不能回退到应用实例时钟。
package biz

import (
	"context"
	"fmt"
	"math"
	"time"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
)

func databaseUTCEpochSQL(dialectName string) string {
	switch dialectName {
	case dialect.Postgres:
		return "EXTRACT(EPOCH FROM clock_timestamp())"
	case dialect.MySQL:
		// UNIX_TIMESTAMP(UTC_TIMESTAMP()) 会把 UTC 误按连接时区解释；
		// 用 UTC 日期差计算 epoch，避免连接时区影响安全期限。
		return "TIMESTAMPDIFF(MICROSECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP(6)) / 1000000.0"
	default:
		return "(julianday('now') - 2440587.5) * 86400.0"
	}
}

func databaseUTCNowSQL(dialectName string) string {
	switch dialectName {
	case dialect.Postgres:
		return "clock_timestamp()"
	case dialect.MySQL:
		return "UTC_TIMESTAMP(6)"
	default:
		return "julianday('now')"
	}
}

func databaseUTCTimestampSQL(dialectName, column string) string {
	if dialectName != dialect.SQLite {
		return column
	}
	// modernc 可能保存 time.String() 的 "...00.123456789 +0000 UTC"。
	// julianday 不识别该后缀；去后缀时保留小数秒，不能截到 19 字节而提前到期。
	end := "CASE WHEN instr(substr(" + column + ",20),' ') > 0 THEN 18 + instr(substr(" + column + ",20),' ') ELSE 19 END"
	return "COALESCE(julianday(" + column + "), julianday(substr(" + column + ",1," + end + ")))"
}

func databaseUTCFromEpoch(epoch float64) (time.Time, error) {
	if math.IsNaN(epoch) || math.IsInf(epoch, 0) || epoch < 0 || epoch > 253402300799 {
		return time.Time{}, fmt.Errorf("invalid database UTC clock")
	}
	seconds, fraction := math.Modf(epoch)
	return time.Unix(int64(seconds), int64(fraction*1e9)).UTC(), nil
}

func databaseUTCNow(ctx context.Context, client *ent.Client) (time.Time, error) {
	if err := authz.RequireSystemPrincipal(ctx); err != nil {
		return time.Time{}, err
	}
	if txClient := ent.FromContext(ctx); txClient != nil {
		client = txClient
	}
	query, args := sql.Dialect(client.Driver().Dialect()).Select(sql.As(databaseUTCEpochSQL(client.Driver().Dialect()), "epoch")).Query()
	rows := &sql.Rows{}
	if err := client.Driver().Query(ctx, query, args, rows); err != nil {
		return time.Time{}, fmt.Errorf("read database UTC clock: %w", err)
	}
	defer rows.Close()
	var observed []struct {
		Epoch float64 `json:"epoch"`
	}
	if err := sql.ScanSlice(rows, &observed); err != nil {
		return time.Time{}, fmt.Errorf("decode database UTC clock: %w", err)
	}
	if len(observed) != 1 {
		return time.Time{}, fmt.Errorf("database UTC clock must return one observation")
	}
	return databaseUTCFromEpoch(observed[0].Epoch)
}
