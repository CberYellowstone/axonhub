package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

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

	return nil
}

func (p *PersistentOutboundTransformer) FinishSpecialRetry(ctx context.Context, originalErr error, specialErr error) {
	if p == nil || p.state == nil {
		return
	}

	p.resetPassThroughStreamState()
	if specialErr != nil {
		p.state.RequestExec = nil
		p.state.PassThroughApplied = false
	}

	p.state.SpecialRetryActive = false
	p.state.SpecialRetryType = ""
	p.state.SpecialRetryTriggerStatus = 0
	p.state.SpecialRetryTriggerMessage = ""
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
		if outbound == nil || outbound.state == nil ||
			!outbound.state.SpecialRetryActive ||
			outbound.state.SpecialRetryType != specialRetryTypeEncryptedContentCleanupSameChannel {
			return request, nil
		}

		cleaned, ok := cleanupEncryptedContentRequestBody(request)
		if !ok {
			return request, nil
		}

		request.Body = cleaned
		request.JSONBody = cleaned

		return request, nil
	})
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

	removeEncryptedContentFields(value)
	removeEmptyResponsesReasoningItems(value)

	cleaned, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}

	return cleaned, true
}

func removeEncryptedContentFields(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for key := range encryptedContentCleanupFieldNames {
			delete(typed, key)
		}
		for _, child := range typed {
			removeEncryptedContentFields(child)
		}
	case []any:
		for _, child := range typed {
			removeEncryptedContentFields(child)
		}
	}
}

func removeEmptyResponsesReasoningItems(value any) {
	obj, ok := value.(map[string]any)
	if !ok {
		return
	}

	input, ok := obj["input"].([]any)
	if !ok {
		return
	}

	filtered := make([]any, 0, len(input))
	for _, item := range input {
		if isEmptyResponsesReasoningItem(item) {
			continue
		}

		filtered = append(filtered, item)
	}

	obj["input"] = filtered
}

func isEmptyResponsesReasoningItem(value any) bool {
	item, ok := value.(map[string]any)
	if !ok {
		return false
	}

	itemType, _ := item["type"].(string)
	if !strings.EqualFold(itemType, "reasoning") {
		return false
	}

	for _, key := range []string{"summary", "content", "reasoning_content", "text"} {
		if hasUsableReasoningValue(item[key]) {
			return false
		}
	}

	return true
}

func hasUsableReasoningValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		for _, item := range typed {
			if hasUsableReasoningValue(item) {
				return true
			}
		}

		return false
	case map[string]any:
		for key, item := range typed {
			if strings.EqualFold(key, "type") {
				continue
			}
			if hasUsableReasoningValue(item) {
				return true
			}
		}

		return false
	default:
		return true
	}
}
