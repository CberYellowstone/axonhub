// 职责: 将持久 KV 的版本读取及比较交换接口映射到 JavaScript。
// 关联文件:
//
//	internal/server/plugin/kv_cas.go
//	internal/server/plugin/hostapi.go
//	internal/server/plugin/kv.go
//
// 注意事项: expectedRevision 为 null 时只创建；get 保持返回字符串的既有契约。
package plugin

import (
	"encoding/json"

	"github.com/MoeForge/moejs"
)

func (h *hostCall) kvGetEntry(_ *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
	if err := h.enter(false); err != nil {
		return moejs.Undefined(), err
	}
	key, err := h.requireString(moejs.Arg(args, 0), "key")
	if err != nil {
		return moejs.Undefined(), err
	}
	opts, err := h.optionsAt(args, 1)
	if err != nil {
		return moejs.Undefined(), err
	}
	namespace, err := h.optionString(opts, "namespace")
	if err != nil {
		return moejs.Undefined(), err
	}
	entry, err := h.manager.KVGet(h.ctx, h.pluginID, h.installationID, namespace, key)
	if err != nil {
		return moejs.Undefined(), h.kvFail(err)
	}
	return h.hostJSONValue(entry)
}

func (h *hostCall) kvCompareAndSet(_ *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
	if err := h.enter(false); err != nil {
		return moejs.Undefined(), err
	}
	key, err := h.requireString(moejs.Arg(args, 0), "key")
	if err != nil {
		return moejs.Undefined(), err
	}
	var expected *string
	if v := moejs.Arg(args, 1); !v.IsNull() {
		text, err := h.requireString(v, "expectedRevision")
		if err != nil {
			return moejs.Undefined(), err
		}
		expected = &text
	}
	value, err := h.requireString(moejs.Arg(args, 2), "value")
	if err != nil {
		return moejs.Undefined(), err
	}
	opts, err := h.optionsAt(args, 3)
	if err != nil {
		return moejs.Undefined(), err
	}
	namespace, err := h.optionString(opts, "namespace")
	if err != nil {
		return moejs.Undefined(), err
	}
	ttl, err := h.optionTTL(opts)
	if err != nil {
		return moejs.Undefined(), err
	}
	if err := h.beginWrite(); err != nil {
		return moejs.Undefined(), err
	}
	result, err := h.manager.KVCompareAndSet(h.ctx, h.pluginID, h.installationID, namespace, key, expected, value, ttl)
	if err != nil {
		return moejs.Undefined(), h.kvFail(err)
	}
	return h.hostJSONValue(result)
}

func (h *hostCall) hostJSONValue(value any) (moejs.Value, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return moejs.Undefined(), h.fail(hostInternal, "could not encode host API result")
	}
	if len(raw) > 2<<20 {
		return moejs.Undefined(), h.fail(hostInvalidState, "host API result exceeds 2 MiB; narrow the query or page size")
	}
	return h.rt.ParseJSON(raw)
}
