// 职责: 向插件暴露受实时授权及调度围栏约束的指标、暂停和通知宿主接口。
// 关联文件:
//
//	internal/server/plugin/automation_usage.go
//	internal/server/biz/api_key_suspension.go
//	internal/server/biz/plugin_notification.go
//	internal/server/plugin/scheduler.go
//
// 注意事项: 安装身份由宿主固定；插件参数不能选择其他安装或取得通用数据库权限。
package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/MoeForge/moejs"
	"github.com/MoeForge/moejs/engine"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/privacy"
	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
)

type CacheUsageOptions struct {
	ProjectID      int       `json:"projectId"`
	KeyIDs         []int     `json:"keyIds,omitempty"`
	AllKeys        bool      `json:"allKeys,omitempty"`
	ExcludeKeyIDs  []int     `json:"excludeKeyIds,omitempty"`
	AfterKeyID     int       `json:"afterKeyId,omitempty"`
	Limit          int       `json:"limit,omitempty"`
	End            time.Time `json:"end,omitempty"`
	WindowSeconds  int       `json:"windowSeconds,omitempty"`
	LastRequests   int       `json:"lastRequests,omitempty"`
	MinSamples     int       `json:"minSamples,omitempty"`
	LowThreshold   *float64  `json:"lowThreshold,omitempty"`
	IncludeSamples bool      `json:"includeSamples,omitempty"`
	ModelIDs       []string  `json:"modelIds,omitempty"`
	ChannelIDs     []int     `json:"channelIds,omitempty"`
}

func (q CacheUsageOptions) Query() (biz.CacheUsageQuery, error) {
	if q.WindowSeconds < 0 || int64(q.WindowSeconds) > int64(biz.MaxCacheUsageWindow/time.Second) {
		return biz.CacheUsageQuery{}, fmt.Errorf("windowSeconds must be within 0..7776000")
	}
	return biz.CacheUsageQuery{
		ProjectID: q.ProjectID, KeyIDs: q.KeyIDs, AllKeys: q.AllKeys,
		ExcludeKeyIDs: q.ExcludeKeyIDs, AfterKeyID: q.AfterKeyID, Limit: q.Limit,
		End: q.End, Window: time.Duration(q.WindowSeconds) * time.Second,
		LastRequests: q.LastRequests, MinSamples: q.MinSamples,
		LowThreshold: q.LowThreshold, IncludeSamples: q.IncludeSamples,
		ModelIDs: q.ModelIDs, ChannelIDs: q.ChannelIDs,
	}, nil
}

func (h *hostCall) attachAutomationAPI(ctxObj *moejs.Object) error {
	usage := h.rt.Realm().NewObject()
	if err := defineHostFn(h.rt, usage, "cacheStats", 1, h.cacheStats); err != nil {
		return err
	}
	if err := defineValue(h.rt, ctxObj, "usage", engine.ObjectValue(usage)); err != nil {
		return err
	}
	if h.export != "onSchedule" && h.export != onEventExport {
		return nil
	}
	suspensions := h.rt.Realm().NewObject()
	for _, method := range []struct {
		name string
		fn   moejs.NativeFunc
	}{{"apply", h.suspensionApply}, {"release", h.suspensionRelease}, {"list", h.suspensionList}} {
		if err := defineHostFn(h.rt, suspensions, method.name, 1, method.fn); err != nil {
			return err
		}
	}
	if err := defineValue(h.rt, ctxObj, "keySuspensions", engine.ObjectValue(suspensions)); err != nil {
		return err
	}
	notifications := h.rt.Realm().NewObject()
	if err := defineHostFn(h.rt, notifications, "enqueue", 1, h.notificationEnqueue); err != nil {
		return err
	}
	return defineValue(h.rt, ctxObj, "notifications", engine.ObjectValue(notifications))
}

func (h *hostCall) decodeAutomation(value moejs.Value, target any) error {
	raw, err := h.readReturn(value)
	if err != nil || !isJSONObject(raw) || len(raw) > MaxConfigBytes {
		return h.fail(hostInvalidArgument, "options must be a JSON object of at most 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return h.fail(hostInvalidArgument, "invalid or unknown automation option")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return h.fail(hostInvalidArgument, "options must contain exactly one object")
	}
	return nil
}

func runAutomationHost[T any](h *hostCall, permission string, write bool, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	if err := h.enter(false); err != nil {
		return zero, err
	}
	if err := h.requirePermission(permission); err != nil {
		return zero, err
	}
	if write {
		if err := h.beginWrite(); err != nil {
			return zero, err
		}
		var result T
		err := h.manager.withScheduleWrite(h.ctx, func(txCtx context.Context) error {
			var err error
			result, err = authz.RunAsHost(txCtx, h.manager.client(txCtx), "plugin-automation-call", fn)
			return err
		})
		return result, err
	}
	// This fixed closure is supplied by host code, never JavaScript.
	return authz.RunAsHost(h.ctx, h.manager.client(h.ctx), "plugin-automation-call", fn)
}

func (h *hostCall) automationError(err error) error {
	if errors.Is(err, privacy.Deny) {
		return h.fail(hostPermissionDenied, "plugin automation access denied")
	}
	if ent.IsNotFound(err) {
		return h.fail(hostNotFound, "automation target not found")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return h.fail(hostCanceled, "plugin automation call canceled")
	}
	log.Warn(h.ctx, "plugin automation operation failed", log.String("plugin_id", h.pluginID), log.Cause(err))
	return h.fail(hostInvalidState, "plugin automation operation failed; check the host log")
}

func (h *hostCall) cacheStats(_ *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
	var options CacheUsageOptions
	if err := h.decodeAutomation(moejs.Arg(args, 0), &options); err != nil {
		return moejs.Undefined(), err
	}
	query, err := options.Query()
	if err != nil {
		return moejs.Undefined(), h.fail(hostInvalidArgument, err.Error())
	}
	result, err := runAutomationHost(h, objects.PluginPermissionUsageMetrics, false, func(hostCtx context.Context) (biz.CacheUsagePage, error) {
		return h.manager.CacheUsage(hostCtx, h.installationID, query)
	})
	if err != nil {
		return moejs.Undefined(), h.automationError(err)
	}
	return h.hostJSONValue(result)
}

func (h *hostCall) suspensionApply(_ *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
	var input objects.APIKeySuspensionInput
	if err := h.decodeAutomation(moejs.Arg(args, 0), &input); err != nil {
		return moejs.Undefined(), err
	}
	result, err := runAutomationHost(h, objects.PluginPermissionKeySuspensions, true, func(hostCtx context.Context) (*objects.APIKeySuspensionResult, error) {
		return biz.NewAPIKeySuspensionService(h.manager.client(hostCtx)).Apply(hostCtx, h.installationID, input)
	})
	if err != nil {
		return moejs.Undefined(), h.automationError(err)
	}
	return h.hostJSONValue(result)
}

func (h *hostCall) suspensionRelease(_ *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
	id, err := h.requireWhole(moejs.Arg(args, 0), "suspensionId")
	if err != nil {
		return moejs.Undefined(), err
	}
	result, err := runAutomationHost(h, objects.PluginPermissionKeySuspensions, true, func(hostCtx context.Context) (*objects.APIKeySuspensionResult, error) {
		return biz.NewAPIKeySuspensionService(h.manager.client(hostCtx)).ReleaseOwn(hostCtx, h.installationID, id)
	})
	if err != nil {
		return moejs.Undefined(), h.automationError(err)
	}
	return h.hostJSONValue(result)
}

func (h *hostCall) suspensionList(_ *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
	var filter objects.APIKeySuspensionFilter
	if err := h.decodeAutomation(moejs.Arg(args, 0), &filter); err != nil {
		return moejs.Undefined(), err
	}
	result, err := runAutomationHost(h, objects.PluginPermissionKeySuspensions, false, func(hostCtx context.Context) ([]*objects.APIKeySuspensionResult, error) {
		return biz.NewAPIKeySuspensionService(h.manager.client(hostCtx)).List(hostCtx, h.installationID, filter)
	})
	if err != nil {
		return moejs.Undefined(), h.automationError(err)
	}
	return h.hostJSONValue(result)
}

func (h *hostCall) notificationEnqueue(_ *moejs.Realm, _ moejs.Value, args []moejs.Value) (moejs.Value, error) {
	var input objects.PluginNotificationInput
	if err := h.decodeAutomation(moejs.Arg(args, 0), &input); err != nil {
		return moejs.Undefined(), err
	}
	row, err := runAutomationHost(h, permWebhook, true, func(hostCtx context.Context) (*ent.PluginNotification, error) {
		return biz.EnqueuePluginNotification(hostCtx, h.manager.client(hostCtx), h.installationID, input)
	})
	if err != nil {
		return moejs.Undefined(), h.automationError(err)
	}
	return h.hostJSONValue(map[string]any{"id": row.ID, "eventId": row.EventID, "status": row.Status})
}
