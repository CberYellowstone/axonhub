// 职责: 为插件持久状态提供带不透明版本令牌的原子比较交换。
// 关联文件:
//
//	internal/server/plugin/kv.go
//	internal/ent/schema/plugin_storage.go
//	internal/server/plugin/hostapi.go
//
// 注意事项: nil 版本只创建缺失键；空字符串版本可比较旧行；删除重建产生新令牌，避免 ABA。
package plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/looplj/axonhub/internal/ent/pluginstorage"
	"github.com/looplj/axonhub/internal/ent/predicate"
)

type KVCASResult struct {
	Swapped bool     `json:"swapped"`
	Entry   *KVEntry `json:"entry"`
}

// KVCompareAndSet atomically checks the token and writes a new value. A failed
// comparison has no write effect. The returned token belongs to this write.
func (m *Manager) KVCompareAndSet(ctx context.Context, manifestID string, installationID int, namespace, key string, expectedRevision *string, value string, ttl time.Duration) (*KVCASResult, error) {
	namespace, err := validateKVLookup(installationID, namespace, key)
	if err != nil {
		return nil, err
	}
	if err := validateKVValue(value); err != nil {
		return nil, err
	}
	if expectedRevision != nil && len(*expectedRevision) > 36 {
		return nil, fmt.Errorf("plugin kv revision is invalid")
	}
	expiresAt, err := m.kvExpiry(ttl)
	if err != nil {
		return nil, err
	}
	return runKV(ctx, m, manifestID, installationID, true, func(hostCtx context.Context) (*KVCASResult, error) {
		var result *KVCASResult
		err := m.runInTx(hostCtx, func(txCtx context.Context) error {
			var err error
			result, err = m.compareAndSetKV(txCtx, installationID, namespace, key, expectedRevision, value, expiresAt)
			return err
		})
		return result, err
	})
}

func (m *Manager) compareAndSetKV(hostCtx context.Context, installationID int, namespace, key string, expectedRevision *string, value string, expiresAt *time.Time) (*KVCASResult, error) {
	client := m.client(hostCtx)
	now, token := m.now(), uuid.NewString()
	base := []predicate.PluginStorage{
		pluginstorage.InstallationID(installationID), pluginstorage.Namespace(namespace), pluginstorage.Key(key),
	}
	var swapped bool
	if expectedRevision == nil {
		// Conditional expiry cleanup cannot delete a concurrent renewal.
		if _, err := client.PluginStorage.Delete().Where(base...).Where(pluginstorage.ExpiresAtLTE(now)).Exec(hostCtx); err != nil {
			return nil, fmt.Errorf("expire plugin CAS key: %w", err)
		}
		err := client.PluginStorage.Create().SetInstallationID(installationID).
			SetNamespace(namespace).SetKey(key).SetValue(value).SetRevision(token).
			SetUpdatedAt(now).SetNillableExpiresAt(expiresAt).
			OnConflictColumns(pluginstorage.FieldInstallationID, pluginstorage.FieldNamespace, pluginstorage.FieldKey).
			Ignore().Exec(hostCtx)
		if err != nil {
			return nil, fmt.Errorf("create plugin CAS key: %w", err)
		}
		swapped, err = client.PluginStorage.Query().Where(base...).Where(pluginstorage.RevisionEQ(token)).Exist(hostCtx)
		if err != nil {
			return nil, fmt.Errorf("confirm plugin CAS creation: %w", err)
		}
	} else {
		u := client.PluginStorage.Update().Where(base...).
			Where(pluginstorage.RevisionEQ(*expectedRevision), pluginstorage.Or(pluginstorage.ExpiresAtIsNil(), pluginstorage.ExpiresAtGT(now))).
			SetValue(value).SetRevision(token).SetUpdatedAt(now)
		if expiresAt == nil {
			u.ClearExpiresAt()
		} else {
			u.SetExpiresAt(*expiresAt)
		}
		n, err := u.Save(hostCtx)
		if err != nil {
			return nil, fmt.Errorf("compare plugin CAS key: %w", err)
		}
		swapped = n == 1
	}
	if !swapped {
		return &KVCASResult{}, nil
	}
	return &KVCASResult{Swapped: true, Entry: &KVEntry{
		Namespace: namespace, Key: key, Value: value, Revision: token, UpdatedAt: now, ExpiresAt: expiresAt,
	}}, nil
}
