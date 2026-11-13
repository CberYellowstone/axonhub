// 职责: 保存独立于 API Key 人工状态的插件暂停记录及解除审计。
// 关联文件:
//
//	internal/server/biz/api_key_suspension.go
//	internal/objects/key_suspension.go
//
// 遵循的 Rules:
//
//	.agents/rules/ent-graphql.md
//	.agents/rules/database-indexes.md
//
// 注意事项: 不关联级联删除；卸载插件或删除 Key 不应移除审计或重新启用其他暂停。
package schema

import (
	"entgo.io/contrib/entgql"
	"entgo.io/ent"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/scopes"
)

type APIKeySuspension struct {
	ent.Schema
}

func (APIKeySuspension) Mixin() []ent.Mixin { return []ent.Mixin{TimeMixin{}} }

func (APIKeySuspension) Fields() []ent.Field {
	return []ent.Field{
		field.Int("installation_id").Positive().Immutable(),
		field.Int("api_key_id").Positive().Immutable(),
		field.Int("project_id").Positive().Immutable(),
		field.String("plugin_id").MaxLen(objects.MaxPluginIDLength).Immutable(),
		// SHA-256 bounds the unique key to 256 utf8mb4 bytes plus one integer.
		field.String("idempotency_key_hash").MaxLen(64).MinLen(64).Immutable(),
		field.String("reason").MaxLen(512).NotEmpty().Immutable(),
		field.JSON("evidence", map[string]any{}).Immutable(),
		field.String("created_by").MaxLen(128).NotEmpty().Immutable(),
		field.Time("expires_at").Optional().Nillable().Immutable(),
		field.Time("released_at").Optional().Nillable(),
		field.String("released_by").Optional().Default("").MaxLen(128),
		field.String("release_reason").Optional().Default("").MaxLen(512),
	}
}

func (APIKeySuspension) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("installation_id", "idempotency_key_hash").Unique().StorageKey("api_key_suspensions_by_idempotency"),
		// Auth probes one key/project and only unreleased records; expiry is residual.
		index.Fields("api_key_id", "project_id", "released_at").StorageKey("api_key_suspensions_by_key_project_release"),
	}
}

func (APIKeySuspension) Annotations() []schema.Annotation {
	return []schema.Annotation{entgql.Skip(entgql.SkipAll)}
}

func (APIKeySuspension) Policy() ent.Policy { return scopes.Policy{} }
