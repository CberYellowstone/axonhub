// 职责: 持久化插件 WebHook outbox、幂等身份和跨实例租约围栏。
// 关联文件:
//
//	internal/server/biz/plugin_notification.go
//
// 注意事项: 无外键且不软删除；租约与重试时间使用数据库时钟的 Unix 秒。
package schema

import (
	"entgo.io/contrib/entgql"
	"entgo.io/ent"
	"entgo.io/ent/privacy"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type PluginNotification struct{ ent.Schema }

func (PluginNotification) Mixin() []ent.Mixin { return []ent.Mixin{TimeMixin{}} }

func (PluginNotification) Fields() []ent.Field {
	return []ent.Field{
		field.Int("installation_id").Positive().Immutable(),
		field.String("idempotency_hash").MaxLen(64).NotEmpty().Immutable(),
		field.String("idempotency_key").MaxLen(256).NotEmpty().Immutable(),
		field.String("event_id").MaxLen(64).NotEmpty().Immutable(),
		field.String("target_name").MaxLen(128).NotEmpty().Immutable(),
		field.JSON("payload", map[string]any{}).Immutable(),
		field.Int("suspension_id").Optional().Nillable().Immutable(),
		field.JSON("evidence", map[string]any{}).Optional().Immutable(),
		field.String("status").Default("pending").MaxLen(16),
		field.Int("attempts").Default(0).NonNegative(),
		field.Int64("next_attempt_at").Default(0),
		field.String("lease_token").Default("").MaxLen(64),
		field.Int64("lease_until").Default(0),
		field.String("last_error").Default("").MaxLen(128),
	}
}

func (PluginNotification) Indexes() []ent.Index {
	return []ent.Index{
		// MySQL utf8mb4: integer + 64*4 bytes, below the 3072-byte limit.
		index.Fields("installation_id", "idempotency_hash").Unique().StorageKey("plugin_notifications_by_installation_idempotency"),
		// Due work is bounded by next_attempt_at; terminal rows use MaxInt64.
		index.Fields("next_attempt_at").StorageKey("plugin_notifications_by_next_attempt"),
	}
}

func (PluginNotification) Annotations() []schema.Annotation {
	return []schema.Annotation{entgql.Skip(entgql.SkipAll)}
}

func (PluginNotification) Policy() ent.Policy {
	return privacy.Policy{
		Query:    privacy.QueryPolicy{privacy.AlwaysDenyRule()},
		Mutation: privacy.MutationPolicy{privacy.AlwaysDenyRule()},
	}
}
