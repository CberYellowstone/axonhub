// 职责: 持久化插件周期任务、重试身份与跨实例租约围栏。
// 关联文件:
//
//	internal/server/plugin/scheduler.go
//	internal/objects/plugin.go
//
// 注意事项: 不建立外键；安装被停用或删除后执行器实时检查安装版本并拒绝旧租约。
package schema

import (
	"entgo.io/contrib/entgql"
	"entgo.io/ent"
	"entgo.io/ent/privacy"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type PluginSchedule struct{ ent.Schema }

func (PluginSchedule) Mixin() []ent.Mixin { return []ent.Mixin{TimeMixin{}} }

func (PluginSchedule) Fields() []ent.Field {
	return []ent.Field{
		field.Int("installation_id").Immutable().Positive(),
		field.String("key").Immutable().NotEmpty().MaxLen(32),
		field.Int("interval_seconds").Min(60).Max(86400),
		field.String("revision").MaxLen(64),
		field.Time("next_run_at"),
		field.String("run_id").Default("").MaxLen(64),
		field.Time("scheduled_at").Optional().Nillable(),
		field.Int("attempt").Default(0).NonNegative(),
		field.String("lease_owner").Default("").MaxLen(64),
		field.String("lease_token").Default("").MaxLen(64),
		field.Time("lease_until").Optional().Nillable(),
	}
}

func (PluginSchedule) Indexes() []ent.Index {
	return []ent.Index{
		// MySQL utf8mb4: integer + 32*4 bytes, safely below 3072 bytes.
		index.Fields("installation_id", "key").Unique().StorageKey("plugin_schedules_by_installation_key"),
		index.Fields("next_run_at").StorageKey("plugin_schedules_by_next_run"),
	}
}

func (PluginSchedule) Annotations() []schema.Annotation {
	return []schema.Annotation{entgql.Skip(entgql.SkipAll)}
}

func (PluginSchedule) Policy() ent.Policy {
	return privacy.Policy{
		Query:    privacy.QueryPolicy{privacy.AlwaysDenyRule()},
		Mutation: privacy.MutationPolicy{privacy.AlwaysDenyRule()},
	}
}
