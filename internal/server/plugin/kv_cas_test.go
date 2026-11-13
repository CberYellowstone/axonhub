// 职责: 验证持久状态 CAS 的冲突、过期及删除重建行为。
// 关联文件:
//
//	internal/server/plugin/kv_cas.go
//	internal/server/plugin/kv.go
package plugin

import (
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestKVCASRevisionsAndABA(t *testing.T) {
	m := newTestManager(t, newPluginDB(t))
	ctx := pluginCtx()
	installed := installUI(t, m, uiZip(t, nil, nil))
	cas := func(expected *string, value string) *KVCASResult {
		r, err := m.KVCompareAndSet(ctx, installed.PluginID, installed.ID, "", "progress", expected, value, 0)
		require.NoError(t, err)
		return r
	}
	first := cas(nil, "one")
	require.True(t, first.Swapped)
	require.NotEmpty(t, first.Entry.Revision)
	require.False(t, cas(nil, "overwrite").Swapped)
	second := cas(lo.ToPtr(first.Entry.Revision), "two")
	require.True(t, second.Swapped)
	require.NotEqual(t, first.Entry.Revision, second.Entry.Revision)
	require.False(t, cas(lo.ToPtr(first.Entry.Revision), "stale").Swapped)

	// Ordinary writes must invalidate tokens issued by CAS.
	ordinary, err := m.KVSet(ctx, installed.PluginID, installed.ID, "", "progress", "ordinary", 0)
	require.NoError(t, err)
	require.NotEqual(t, second.Entry.Revision, ordinary.Revision)
	require.False(t, cas(lo.ToPtr(second.Entry.Revision), "stale").Swapped)
	deleted, err := m.KVDelete(ctx, installed.PluginID, installed.ID, "", "progress")
	require.NoError(t, err)
	require.True(t, deleted)
	third := cas(nil, "one")
	require.True(t, third.Swapped)
	require.NotEqual(t, ordinary.Revision, third.Entry.Revision)
	require.False(t, cas(lo.ToPtr(ordinary.Revision), "ABA").Swapped)
	got, err := m.KVGet(ctx, installed.PluginID, installed.ID, "", "progress")
	require.NoError(t, err)
	require.Equal(t, "one", got.Value)
}

func TestKVCASExpiredAndInstallationIsolation(t *testing.T) {
	m := newTestManager(t, newPluginDB(t))
	ctx := pluginCtx()
	installed := installUI(t, m, uiZip(t, nil, nil))
	now := time.Now()
	m.kvNow = func() time.Time { return now }
	first, err := m.KVCompareAndSet(ctx, installed.PluginID, installed.ID, "", "cursor", nil, "old", time.Minute)
	require.NoError(t, err)
	require.True(t, first.Swapped)
	now = now.Add(time.Minute)
	stale, err := m.KVCompareAndSet(ctx, installed.PluginID, installed.ID, "", "cursor", lo.ToPtr(first.Entry.Revision), "expired", 0)
	require.NoError(t, err)
	require.False(t, stale.Swapped)
	recreated, err := m.KVCompareAndSet(ctx, installed.PluginID, installed.ID, "", "cursor", nil, "new", 0)
	require.NoError(t, err)
	require.True(t, recreated.Swapped)
	require.NotEqual(t, first.Entry.Revision, recreated.Entry.Revision)
	other, err := m.KVCompareAndSet(ctx, "other.installation", installed.ID+1, "", "cursor", lo.ToPtr(recreated.Entry.Revision), "cross-install", 0)
	require.NoError(t, err)
	require.False(t, other.Swapped)
}
