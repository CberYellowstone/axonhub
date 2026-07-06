package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
)

const specialRetryTypeEncryptedContentCleanupSameChannel = "encrypted_content_cleanup_same_channel"

var encryptedContentCleanupFieldNames = map[string]struct{}{
	"encrypted_content":   {},
	"encryptedContent":    {},
	"reasoning_signature": {},
	"reasoningSignature":  {},
}

func (p *PersistentOutboundTransformer) CanSpecialRetry(ctx context.Context, err error) bool {
	if p == nil || p.state == nil || p.state.SpecialRetryActive || err == nil {
		return false
	}

	policy := p.currentRetryPolicy(ctx)
	if policy == nil || !policy.Enabled || !policy.EncryptedContentCleanupRetryEnabled {
		return false
	}

	statusCode := ExtractStatusCodeFromError(err)
	if statusCode != http.StatusBadRequest {
		return false
	}

	apiFormat := p.currentProviderAPIFormat()
	model := p.currentProviderModel()

	if !isOpenAIResponsesFormat(apiFormat) {
		return false
	}

	if !isGPTModel(model) {
		return false
	}

	return isEncryptedContentCleanupStrongError(err) ||
		isBroadGPTResponsesBadRequestCleanupCandidate(statusCode, apiFormat, model)
}

func (p *PersistentOutboundTransformer) PrepareSpecialRetry(ctx context.Context, err error) error {
	if p == nil || p.state == nil {
		return nil
	}

	p.resetPassThroughStreamState()
	p.state.RequestExec = nil
	p.state.PassThroughApplied = false
	p.state.SpecialRetryActive = true
	p.state.SpecialRetryType = specialRetryTypeEncryptedContentCleanupSameChannel
	p.state.SpecialRetryTriggerStatus = ExtractStatusCodeFromError(err)
	p.state.SpecialRetryTriggerMessage = encryptedContentCleanupErrorText(err)
	p.state.SpecialRetryOriginalDefaultCleanupApplied = p.state.EncryptedContentCleanupDefaultApplied
	p.state.SpecialRetryCleanupBodyApplied = false

	return nil
}

func (p *PersistentOutboundTransformer) FinishSpecialRetry(ctx context.Context, originalErr error, specialErr error) {
	if p == nil || p.state == nil {
		return
	}

	if specialErr != nil {
		p.resetPassThroughStreamState()
		p.state.RequestExec = nil
		p.state.PassThroughApplied = false
	}

	p.finishPendingEncryptedContentCleanupAccounting(ctx, specialErr)
	p.markEncryptedContentCleanupStickyIfNeeded(ctx, specialErr)

	p.state.SpecialRetryActive = false
	p.state.SpecialRetryType = ""
	p.state.SpecialRetryTriggerStatus = 0
	p.state.SpecialRetryTriggerMessage = ""
	p.state.SpecialRetryOriginalDefaultCleanupApplied = false
	p.state.SpecialRetryCleanupBodyApplied = false
}

func (p *PersistentOutboundTransformer) finishPendingEncryptedContentCleanupAccounting(ctx context.Context, specialErr error) {
	if p == nil || p.state == nil {
		return
	}

	dropPending := p.isRecoveredByAppliedEncryptedContentCleanup(specialErr)
	if pending := p.state.PendingCleanupFailurePerf; pending != nil {
		if !dropPending && p.state.ChannelService != nil {
			p.state.ChannelService.AsyncRecordPerformance(ctx, pending)
		}
		p.state.PendingCleanupFailurePerf = nil
	}

	if pending := p.state.PendingCleanupCircuitBreaker; pending != nil {
		if !dropPending {
			pending.Record(ctx)
		}
		p.state.PendingCleanupCircuitBreaker = nil
	}
}

func (p *PersistentOutboundTransformer) markEncryptedContentCleanupStickyIfNeeded(ctx context.Context, specialErr error) {
	if p == nil || p.state == nil ||
		!p.isRecoveredByAppliedEncryptedContentCleanup(specialErr) ||
		p.state.EncryptedContentCleanupSticky == nil {
		return
	}

	policy := p.currentRetryPolicy(ctx)
	if policy == nil ||
		!policy.Enabled ||
		!policy.EncryptedContentCleanupRetryEnabled ||
		policy.EncryptedContentCleanupStickySeconds <= 0 {
		return
	}

	p.state.EncryptedContentCleanupSticky.Mark(
		ctx,
		time.Duration(policy.EncryptedContentCleanupStickySeconds)*time.Second,
	)
}

func (p *PersistentOutboundTransformer) isRecoveredByAppliedEncryptedContentCleanup(specialErr error) bool {
	return p != nil &&
		p.state != nil &&
		specialErr == nil &&
		p.state.SpecialRetryType == specialRetryTypeEncryptedContentCleanupSameChannel &&
		p.state.SpecialRetryTriggerStatus == http.StatusBadRequest &&
		!p.state.SpecialRetryOriginalDefaultCleanupApplied &&
		p.state.SpecialRetryCleanupBodyApplied
}

func (p *PersistentOutboundTransformer) currentRetryPolicy(ctx context.Context) *biz.RetryPolicy {
	if p.state.RetryPolicyProvider == nil {
		return nil
	}

	return p.state.RetryPolicyProvider.RetryPolicyOrDefault(ctx)
}

func (p *PersistentOutboundTransformer) currentProviderAPIFormat() string {
	if p.state.RawProviderRequest != nil && p.state.RawProviderRequest.APIFormat != "" {
		return p.state.RawProviderRequest.APIFormat
	}

	if p.state.CurrentCandidate != nil && p.state.CurrentCandidate.APIFormat != "" {
		return p.state.CurrentCandidate.APIFormat
	}

	if p.state.LlmRequest != nil {
		return string(p.state.LlmRequest.APIFormat)
	}

	return ""
}

func (p *PersistentOutboundTransformer) currentProviderModel() string {
	model := p.GetCurrentModelID()
	if model != "" {
		return model
	}

	if p.state.LlmRequest != nil {
		return p.state.LlmRequest.Model
	}

	return ""
}

func isOpenAIResponsesFormat(format string) bool {
	switch llm.APIFormat(format) {
	case llm.APIFormatOpenAIResponse, llm.APIFormatOpenAIResponseCompact:
		return true
	default:
		return false
	}
}

func isGPTModel(model string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "gpt-")
}

func isBroadGPTResponsesBadRequestCleanupCandidate(statusCode int, apiFormat string, model string) bool {
	return statusCode == http.StatusBadRequest &&
		isOpenAIResponsesFormat(apiFormat) &&
		isGPTModel(model)
}

func isEncryptedContentCleanupStrongError(err error) bool {
	text := strings.ToLower(encryptedContentCleanupErrorText(err))
	if strings.Contains(text, "invalid_encrypted_content") {
		return true
	}

	return strings.Contains(text, "encrypted content") &&
		(strings.Contains(text, "could not be verified") ||
			strings.Contains(text, "could not be decrypted or parsed"))
}

func encryptedContentCleanupErrorText(err error) string {
	if err == nil {
		return ""
	}

	var parts []string
	parts = append(parts, err.Error())

	var httpErr *httpclient.Error
	if errors.As(err, &httpErr) && len(httpErr.Body) > 0 {
		parts = append(parts, string(httpErr.Body))
	}

	var llmErr *llm.ResponseError
	if errors.As(err, &llmErr) {
		parts = append(parts, llmErr.Error(), llmErr.Detail.Message, llmErr.Detail.Code, llmErr.Detail.Type)
	}

	return strings.Join(parts, "\n")
}

func applyEncryptedContentCleanupRetryBody(outbound *PersistentOutboundTransformer) pipeline.Middleware {
	return pipeline.OnRawRequest("encrypted-content-cleanup-retry-body", func(ctx context.Context, request *httpclient.Request) (*httpclient.Request, error) {
		if outbound == nil || outbound.state == nil {
			return request, nil
		}

		if outbound.state.SpecialRetryActive &&
			outbound.state.SpecialRetryType == specialRetryTypeEncryptedContentCleanupSameChannel {
			cleaned, ok := cleanupEncryptedContentRequestBody(request)
			if !ok {
				return request, nil
			}

			applyCleanedRequestBody(request, cleaned)
			outbound.state.SpecialRetryCleanupBodyApplied = true

			return request, nil
		}

		outbound.state.EncryptedContentCleanupDefaultApplied = false
		if isEncryptedContentCleanupCompactRequest(outbound, request) {
			if outbound.state.EncryptedContentCleanupSticky != nil {
				outbound.state.EncryptedContentCleanupSticky.Clear(ctx)
			}

			return request, nil
		}

		if !outbound.shouldApplyEncryptedContentCleanupDefault(ctx) {
			return request, nil
		}

		cleaned, ok := cleanupEncryptedContentRequestBody(request)
		if !ok {
			return request, nil
		}

		applyCleanedRequestBody(request, cleaned)
		outbound.state.EncryptedContentCleanupDefaultApplied = true

		return request, nil
	})
}

func (p *PersistentOutboundTransformer) shouldApplyEncryptedContentCleanupDefault(ctx context.Context) bool {
	if p == nil || p.state == nil || p.state.EncryptedContentCleanupSticky == nil {
		return false
	}

	policy := p.currentRetryPolicy(ctx)
	if policy == nil ||
		!policy.Enabled ||
		!policy.EncryptedContentCleanupRetryEnabled ||
		policy.EncryptedContentCleanupStickySeconds <= 0 {
		return false
	}

	if !p.state.EncryptedContentCleanupSticky.Active(ctx) {
		return false
	}

	return isOpenAIResponsesFormat(p.currentProviderAPIFormat()) && isGPTModel(p.currentProviderModel())
}

func (p *PersistentOutboundTransformer) shouldDeferEncryptedContentCleanupFailure(ctx context.Context, err error) bool {
	if p == nil || p.state == nil || err == nil {
		return false
	}

	policy := p.currentRetryPolicy(ctx)
	if policy == nil || !policy.IgnoreCleanedUpEncryptedContentErrors {
		return false
	}

	return p.CanSpecialRetry(ctx, err)
}

func isEncryptedContentCleanupCompactRequest(outbound *PersistentOutboundTransformer, request *httpclient.Request) bool {
	if request != nil {
		if llm.RequestType(request.RequestType) == llm.RequestTypeCompact {
			return true
		}
		if llm.APIFormat(request.APIFormat) == llm.APIFormatOpenAIResponseCompact {
			return true
		}
	}

	return outbound != nil &&
		outbound.state != nil &&
		outbound.state.LlmRequest != nil &&
		outbound.state.LlmRequest.RequestType == llm.RequestTypeCompact
}

func applyCleanedRequestBody(request *httpclient.Request, cleaned []byte) {
	request.Body = cleaned
	request.JSONBody = cleaned
}

func cleanupEncryptedContentRequestBody(request *httpclient.Request) ([]byte, bool) {
	if request == nil {
		return nil, false
	}

	body := request.Body
	if len(body) == 0 {
		body = request.JSONBody
	}
	if len(body) == 0 {
		return nil, false
	}

	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, false
	}

	changed := removeEncryptedContentFields(value)
	changed = removeResponsesReasoningItems(value) || changed
	if !changed {
		return nil, false
	}

	cleaned, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}

	return cleaned, true
}

func removeEncryptedContentFields(value any) bool {
	changed := false
	switch typed := value.(type) {
	case map[string]any:
		for key := range encryptedContentCleanupFieldNames {
			if _, ok := typed[key]; ok {
				delete(typed, key)
				changed = true
			}
		}
		for _, child := range typed {
			changed = removeEncryptedContentFields(child) || changed
		}
	case []any:
		for _, child := range typed {
			changed = removeEncryptedContentFields(child) || changed
		}
	}

	return changed
}

func removeResponsesReasoningItems(value any) bool {
	obj, ok := value.(map[string]any)
	if !ok {
		return false
	}

	input, ok := obj["input"].([]any)
	if !ok {
		return false
	}

	changed := false
	filtered := make([]any, 0, len(input))
	for _, item := range input {
		if isResponsesReasoningItem(item) {
			changed = true
			continue
		}

		filtered = append(filtered, item)
	}
	if !changed {
		return false
	}

	obj["input"] = filtered

	return true
}

func isResponsesReasoningItem(value any) bool {
	item, ok := value.(map[string]any)
	if !ok {
		return false
	}

	itemType, _ := item["type"].(string)
	return strings.EqualFold(itemType, "reasoning")
}
