// 职责: 定义插件 Key 暂停请求与有界审计证据。
// 关联文件:
//
//	internal/server/biz/api_key_suspension.go
//	internal/ent/schema/api_key_suspension.go
//	internal/server/biz/plugin_notification.go
//
// 注意事项: 暂停不修改 API Key 状态；永久暂停必须显式选择，且不能同时指定时长。
package objects

import "time"

const (
	MaxKeySuspensionReasonBytes      = 512
	MaxKeySuspensionEvidenceBytes    = 16 * 1024
	MaxKeySuspensionIdempotencyBytes = 256
	MaxKeySuspensionDuration         = 365 * 24 * time.Hour
)

type APIKeySuspensionInput struct {
	APIKeyID        int                      `json:"apiKeyId"`
	ProjectID       int                      `json:"projectId"`
	IdempotencyKey  string                   `json:"idempotencyKey"`
	Reason          string                   `json:"reason"`
	Evidence        map[string]any           `json:"evidence"`
	DurationSeconds int                      `json:"durationSeconds"`
	Permanent       bool                     `json:"permanent"`
	Notification    *PluginNotificationInput `json:"notification,omitempty"`
}

type APIKeySuspensionFilter struct {
	ProjectID  int  `json:"projectId"`
	APIKeyID   int  `json:"apiKeyId"`
	ActiveOnly bool `json:"activeOnly"`
	Limit      int  `json:"limit"`
	BeforeID   int  `json:"beforeId"`
}

// PluginNotificationInput contains no network address or transport credentials.
// When embedded in Apply, the host owns SuspensionID, Evidence and IdempotencyKey.
type PluginNotificationInput struct {
	TargetName     string         `json:"targetName"`
	Payload        map[string]any `json:"payload"`
	IdempotencyKey string         `json:"idempotencyKey"`
	SuspensionID   *int           `json:"suspensionId,omitempty"`
	Evidence       map[string]any `json:"evidence,omitempty"`
}

// APIKeySuspensionResult is the public host API shape; storage hashes and Ent
// metadata are not part of the plugin protocol.
type APIKeySuspensionResult struct {
	ID             int            `json:"id"`
	CreatedAt      time.Time      `json:"createdAt"`
	UpdatedAt      time.Time      `json:"updatedAt"`
	InstallationID int            `json:"installationId"`
	PluginID       string         `json:"pluginId"`
	APIKeyID       int            `json:"apiKeyId"`
	ProjectID      int            `json:"projectId"`
	Reason         string         `json:"reason"`
	Evidence       map[string]any `json:"evidence"`
	CreatedBy      string         `json:"createdBy"`
	ExpiresAt      *time.Time     `json:"expiresAt"`
	ReleasedAt     *time.Time     `json:"releasedAt"`
	ReleasedBy     string         `json:"releasedBy"`
	ReleaseReason  string         `json:"releaseReason"`
}
