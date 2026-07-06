package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer/shared"
)

func TestPersistentOutboundTransformer_CanSpecialRetry_EncryptedContentCleanup(t *testing.T) {
	ctx := context.Background()
	enabledPolicy := &biz.RetryPolicy{
		Enabled:                             true,
		EncryptedContentCleanupRetryEnabled: true,
	}

	tests := []struct {
		name          string
		policy        *biz.RetryPolicy
		apiFormat     string
		model         string
		statusCode    int
		body          string
		specialActive bool
		want          bool
	}{
		{
			name:       "strong encrypted content error",
			policy:     enabledPolicy,
			apiFormat:  string(llm.APIFormatOpenAIResponse),
			model:      "gpt-5.5",
			statusCode: http.StatusBadRequest,
			body:       `{"error":{"code":"invalid_encrypted_content","message":"The encrypted content could not be verified. Reason: Encrypted content could not be decrypted or parsed."}}`,
			want:       true,
		},
		{
			name:       "broad GPT responses HTTP 400",
			policy:     enabledPolicy,
			apiFormat:  string(llm.APIFormatOpenAIResponse),
			model:      "GPT-5.4",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"invalid request: failed to decode responses api request: invalid input: invalid request"}`,
			want:       true,
		},
		{
			name:       "non GPT model",
			policy:     enabledPolicy,
			apiFormat:  string(llm.APIFormatOpenAIResponse),
			model:      "claude-sonnet-4",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"invalid request"}`,
			want:       false,
		},
		{
			name:       "non responses format",
			policy:     enabledPolicy,
			apiFormat:  string(llm.APIFormatOpenAIChatCompletion),
			model:      "gpt-5.5",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"invalid request"}`,
			want:       false,
		},
		{
			name:       "rate limit keeps ordinary retry behavior",
			policy:     enabledPolicy,
			apiFormat:  string(llm.APIFormatOpenAIResponse),
			model:      "gpt-5.5",
			statusCode: http.StatusTooManyRequests,
			body:       `{"error":"rate limited"}`,
			want:       false,
		},
		{
			name: "retry policy disabled",
			policy: &biz.RetryPolicy{
				Enabled:                             false,
				EncryptedContentCleanupRetryEnabled: true,
			},
			apiFormat:  string(llm.APIFormatOpenAIResponse),
			model:      "gpt-5.5",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"invalid request"}`,
			want:       false,
		},
		{
			name: "cleanup retry switch disabled",
			policy: &biz.RetryPolicy{
				Enabled:                             true,
				EncryptedContentCleanupRetryEnabled: false,
			},
			apiFormat:  string(llm.APIFormatOpenAIResponse),
			model:      "gpt-5.5",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"invalid request"}`,
			want:       false,
		},
		{
			name:          "special attempt does not recurse",
			policy:        enabledPolicy,
			apiFormat:     string(llm.APIFormatOpenAIResponse),
			model:         "gpt-5.5",
			statusCode:    http.StatusBadRequest,
			body:          `{"error":"invalid request"}`,
			specialActive: true,
			want:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outbound := newEncryptedContentCleanupRetryTestOutbound(tt.policy, tt.apiFormat, tt.model, tt.specialActive)
			err := &httpclient.Error{
				StatusCode: tt.statusCode,
				Status:     http.StatusText(tt.statusCode),
				Body:       []byte(tt.body),
			}

			require.Equal(t, tt.want, outbound.CanSpecialRetry(ctx, err))
		})
	}
}

func TestApplyEncryptedContentCleanupRetryBody_CleansResponsesBody(t *testing.T) {
	ctx := context.Background()
	request := &httpclient.Request{
		Body: []byte(`{
			"model":"gpt-5.5",
			"include":["message.output_text.logprobs"],
			"reasoning":{"effort":"high"},
			"parallel_tool_calls":true,
			"tool_choice":"auto",
			"tools":[{"type":"function","name":"lookup","input_schema":{"type":"object","encrypted_content":"remove"}}],
			"input":[
				{"type":"reasoning","encrypted_content":"stale","reasoning_signature":"sig"},
				{"type":"reasoning","summary":[{"type":"summary_text","text":"kept"}],"encryptedContent":"stale"},
				{"type":"message","role":"user","content":[{"type":"input_text","text":"hi","reasoningSignature":"sig"}]},
				{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"},
				{"type":"function_call_output","call_id":"call_1","output":"ok"}
			]
		}`),
	}
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			SpecialRetryActive: true,
			SpecialRetryType:   specialRetryTypeEncryptedContentCleanupSameChannel,
		},
	}

	processed, err := applyEncryptedContentCleanupRetryBody(outbound).OnOutboundRawRequest(ctx, request)
	require.NoError(t, err)
	require.Equal(t, processed.Body, processed.JSONBody)
	require.NotContains(t, string(processed.Body), "encrypted_content")
	require.NotContains(t, string(processed.Body), "encryptedContent")
	require.NotContains(t, string(processed.Body), "reasoning_signature")
	require.NotContains(t, string(processed.Body), "reasoningSignature")
	require.False(t, gjson.GetBytes(processed.Body, "input.0.content.0.reasoningSignature").Exists())
	require.False(t, gjson.GetBytes(processed.Body, "tools.0.input_schema.encrypted_content").Exists())

	input := gjson.GetBytes(processed.Body, "input").Array()
	require.Len(t, input, 3)
	require.Equal(t, "message", input[0].Get("type").String())
	require.Equal(t, "hi", input[0].Get("content.0.text").String())
	require.Equal(t, "function_call", input[1].Get("type").String())
	require.Equal(t, "call_1", input[1].Get("call_id").String())
	require.Equal(t, "function_call_output", input[2].Get("type").String())
	require.Equal(t, "call_1", input[2].Get("call_id").String())
	require.Equal(t, "high", gjson.GetBytes(processed.Body, "reasoning.effort").String())
	require.Equal(t, "message.output_text.logprobs", gjson.GetBytes(processed.Body, "include.0").String())
	require.Equal(t, "auto", gjson.GetBytes(processed.Body, "tool_choice").String())
	require.True(t, gjson.GetBytes(processed.Body, "parallel_tool_calls").Bool())
	require.Equal(t, "function", gjson.GetBytes(processed.Body, "tools.0.type").String())
}

func TestApplyEncryptedContentCleanupRetryBody_NoChangesDoesNotMarkCleanupApplied(t *testing.T) {
	ctx := context.Background()
	body := []byte(`{"model":"gpt-5.5","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`)
	request := &httpclient.Request{Body: body}
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			SpecialRetryActive: true,
			SpecialRetryType:   specialRetryTypeEncryptedContentCleanupSameChannel,
		},
	}

	processed, err := applyEncryptedContentCleanupRetryBody(outbound).OnOutboundRawRequest(ctx, request)
	require.NoError(t, err)
	require.Equal(t, body, processed.Body)
	require.Nil(t, processed.JSONBody)
	require.False(t, outbound.state.SpecialRetryCleanupBodyApplied)
}

func TestEncryptedContentCleanupSticky_MarksAndAppliesDefaultCleanup(t *testing.T) {
	ctx := shared.WithSessionScope(shared.WithSessionID(context.Background(), "session-1"), "api-key:1")
	store := newEncryptedContentCleanupStickyStore()
	policy := &biz.RetryPolicy{
		Enabled:                              true,
		EncryptedContentCleanupRetryEnabled:  true,
		EncryptedContentCleanupStickySeconds: 60,
	}
	outbound := newEncryptedContentCleanupRetryTestOutbound(
		policy,
		string(llm.APIFormatOpenAIResponse),
		"gpt-5.5",
		false,
	)
	outbound.state.EncryptedContentCleanupSticky = store
	outbound.state.SpecialRetryActive = true
	outbound.state.SpecialRetryType = specialRetryTypeEncryptedContentCleanupSameChannel
	outbound.state.SpecialRetryTriggerStatus = http.StatusBadRequest
	outbound.state.SpecialRetryCleanupBodyApplied = true

	outbound.FinishSpecialRetry(ctx, errors.New("original"), nil)
	require.True(t, store.Active(ctx))

	request := &httpclient.Request{
		RequestType: llm.RequestTypeChat.String(),
		APIFormat:   string(llm.APIFormatOpenAIResponse),
		Body: []byte(`{
			"model":"gpt-5.5",
			"input":[
				{"type":"reasoning","encrypted_content":"stale"},
				{"type":"message","role":"user","content":[{"type":"input_text","text":"hi","reasoning_signature":"sig"}]}
			]
		}`),
	}

	processed, err := applyEncryptedContentCleanupRetryBody(outbound).OnOutboundRawRequest(ctx, request)
	require.NoError(t, err)
	require.True(t, outbound.state.EncryptedContentCleanupDefaultApplied)
	require.Equal(t, processed.Body, processed.JSONBody)
	require.NotContains(t, string(processed.Body), "encrypted_content")
	require.NotContains(t, string(processed.Body), "reasoning_signature")
	require.Len(t, gjson.GetBytes(processed.Body, "input").Array(), 1)
}

func TestEncryptedContentCleanupSticky_NotMarkedFromAlreadyDefaultCleanedAttempt(t *testing.T) {
	ctx := shared.WithSessionScope(shared.WithSessionID(context.Background(), "session-2"), "api-key:1")
	store := newEncryptedContentCleanupStickyStore()
	outbound := newEncryptedContentCleanupRetryTestOutbound(
		&biz.RetryPolicy{
			Enabled:                              true,
			EncryptedContentCleanupRetryEnabled:  true,
			EncryptedContentCleanupStickySeconds: 60,
		},
		string(llm.APIFormatOpenAIResponse),
		"gpt-5.5",
		false,
	)
	outbound.state.EncryptedContentCleanupSticky = store
	outbound.state.SpecialRetryActive = true
	outbound.state.SpecialRetryType = specialRetryTypeEncryptedContentCleanupSameChannel
	outbound.state.SpecialRetryTriggerStatus = http.StatusBadRequest
	outbound.state.SpecialRetryCleanupBodyApplied = true
	outbound.state.SpecialRetryOriginalDefaultCleanupApplied = true

	outbound.FinishSpecialRetry(ctx, errors.New("original"), nil)

	require.False(t, store.Active(ctx))
}

func TestEncryptedContentCleanupSticky_CompactClearsStickyWithoutCleaning(t *testing.T) {
	ctx := shared.WithSessionScope(shared.WithSessionID(context.Background(), "session-3"), "api-key:1")
	store := newEncryptedContentCleanupStickyStore()
	store.Mark(ctx, time.Minute)
	require.True(t, store.Active(ctx))

	outbound := newEncryptedContentCleanupRetryTestOutbound(
		&biz.RetryPolicy{
			Enabled:                              true,
			EncryptedContentCleanupRetryEnabled:  true,
			EncryptedContentCleanupStickySeconds: 60,
		},
		string(llm.APIFormatOpenAIResponseCompact),
		"gpt-5.5",
		false,
	)
	outbound.state.EncryptedContentCleanupSticky = store
	outbound.state.LlmRequest = &llm.Request{RequestType: llm.RequestTypeCompact}
	request := &httpclient.Request{
		RequestType: llm.RequestTypeCompact.String(),
		APIFormat:   string(llm.APIFormatOpenAIResponseCompact),
		Body:        []byte(`{"input":[{"type":"reasoning","encrypted_content":"keep"}]}`),
	}

	processed, err := applyEncryptedContentCleanupRetryBody(outbound).OnOutboundRawRequest(ctx, request)
	require.NoError(t, err)
	require.False(t, store.Active(ctx))
	require.Contains(t, string(processed.Body), "encrypted_content")
	require.False(t, outbound.state.EncryptedContentCleanupDefaultApplied)
}

func TestEncryptedContentCleanupSticky_ExpiresAfterTTL(t *testing.T) {
	ctx := shared.WithSessionScope(shared.WithSessionID(context.Background(), "session-ttl"), "api-key:1")
	now := time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)
	store := newEncryptedContentCleanupStickyStore()
	store.now = func() time.Time {
		return now
	}

	store.Mark(ctx, time.Second)
	require.True(t, store.Active(ctx))

	now = now.Add(time.Second)
	require.False(t, store.Active(ctx))
}

func TestPerformanceRecording_DefersCleanupRecoveredFailure(t *testing.T) {
	ctx := context.Background()
	policy := &biz.RetryPolicy{
		Enabled:                               true,
		EncryptedContentCleanupRetryEnabled:   true,
		IgnoreCleanedUpEncryptedContentErrors: true,
	}
	outbound := newEncryptedContentCleanupRetryTestOutbound(
		policy,
		string(llm.APIFormatOpenAIResponse),
		"gpt-5.5",
		false,
	)
	outbound.state.Perf = &biz.PerformanceRecord{ChannelID: 1}
	recorder := withPerformanceRecording(outbound)

	recorder.OnOutboundRawError(ctx, &httpclient.Error{
		StatusCode: http.StatusBadRequest,
		Status:     http.StatusText(http.StatusBadRequest),
		Body:       []byte(`{"error":"invalid request"}`),
	})

	require.NotNil(t, outbound.state.PendingCleanupFailurePerf)
	require.Equal(t, http.StatusBadRequest, outbound.state.PendingCleanupFailurePerf.ResponseStatusCode)

	outbound.state.SpecialRetryActive = true
	outbound.state.SpecialRetryType = specialRetryTypeEncryptedContentCleanupSameChannel
	outbound.state.SpecialRetryTriggerStatus = http.StatusBadRequest
	outbound.state.SpecialRetryCleanupBodyApplied = true
	outbound.FinishSpecialRetry(ctx, errors.New("original"), nil)
	require.Nil(t, outbound.state.PendingCleanupFailurePerf)
}

func TestPendingCleanupCircuitBreakerRecordedWhenSpecialRetrySucceedsWithoutCleanup(t *testing.T) {
	ctx := context.Background()
	cb := biz.NewModelCircuitBreaker()
	outbound := newEncryptedContentCleanupRetryTestOutbound(
		&biz.RetryPolicy{
			Enabled:                               true,
			EncryptedContentCleanupRetryEnabled:   true,
			IgnoreCleanedUpEncryptedContentErrors: true,
		},
		string(llm.APIFormatOpenAIResponse),
		"gpt-5.5",
		false,
	)
	outbound.state.PendingCleanupCircuitBreaker = &pendingCleanupCircuitBreakerError{
		modelCircuitBreaker: cb,
		channelID:           1,
		modelID:             "gpt-5.5",
	}
	outbound.state.SpecialRetryActive = true
	outbound.state.SpecialRetryType = specialRetryTypeEncryptedContentCleanupSameChannel
	outbound.state.SpecialRetryTriggerStatus = http.StatusBadRequest

	outbound.FinishSpecialRetry(ctx, errors.New("original"), nil)

	stats := cb.GetModelCircuitBreakerStats(ctx, 1, "gpt-5.5")
	require.Equal(t, 1, stats.ConsecutiveFailures)
	require.Nil(t, outbound.state.PendingCleanupCircuitBreaker)
}

func TestModelCircuitBreaker_DeferCleanupFailureRequiresChannelAndModel(t *testing.T) {
	ctx := context.Background()
	policy := &biz.RetryPolicy{
		Enabled:                               true,
		EncryptedContentCleanupRetryEnabled:   true,
		IgnoreCleanedUpEncryptedContentErrors: true,
	}
	outbound := newEncryptedContentCleanupRetryTestOutbound(
		policy,
		string(llm.APIFormatOpenAIResponse),
		"gpt-5.5",
		false,
	)
	outbound.state.CurrentCandidate.Channel = nil
	outbound.state.OriginalModel = "gpt-5.5"

	middleware := withModelCircuitBreaker(outbound, biz.NewModelCircuitBreaker(), biz.LoadBalancerStrategyCircuitBreaker)
	require.NotPanics(t, func() {
		middleware.OnOutboundRawError(ctx, &httpclient.Error{
			StatusCode: http.StatusBadRequest,
			Status:     http.StatusText(http.StatusBadRequest),
			Body:       []byte(`{"error":"invalid request"}`),
		})
	})
	require.Nil(t, outbound.state.PendingCleanupCircuitBreaker)
}

func TestPersistentOutboundTransformer_FinishSpecialRetryKeepsSuccessfulStreamOpen(t *testing.T) {
	ctx := context.Background()
	canceled := false
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			SpecialRetryActive:         true,
			SpecialRetryType:           specialRetryTypeEncryptedContentCleanupSameChannel,
			SpecialRetryTriggerStatus:  http.StatusBadRequest,
			SpecialRetryTriggerMessage: "invalid_encrypted_content",
			RawStreamCancel: func() {
				canceled = true
			},
			RawStreamCh: make(chan *httpclient.StreamEvent),
		},
	}

	outbound.FinishSpecialRetry(ctx, errors.New("original"), nil)

	require.False(t, canceled, "successful streaming special retry must remain readable by the handler")
	require.NotNil(t, outbound.state.RawStreamCancel)
	require.NotNil(t, outbound.state.RawStreamCh)
	require.False(t, outbound.state.SpecialRetryActive)
	require.Empty(t, outbound.state.SpecialRetryType)
	require.Zero(t, outbound.state.SpecialRetryTriggerStatus)
	require.Empty(t, outbound.state.SpecialRetryTriggerMessage)
}

func TestPersistentOutboundTransformer_FinishSpecialRetryCleansFailedStream(t *testing.T) {
	ctx := context.Background()
	canceled := false
	outbound := &PersistentOutboundTransformer{
		state: &PersistenceState{
			SpecialRetryActive: true,
			SpecialRetryType:   specialRetryTypeEncryptedContentCleanupSameChannel,
			RawStreamCancel: func() {
				canceled = true
			},
			RawStreamCh:        make(chan *httpclient.StreamEvent),
			RawStreamErrRef:    new(error),
			RequestExec:        &ent.RequestExecution{ID: 1},
			PassThroughApplied: true,
		},
	}

	outbound.FinishSpecialRetry(ctx, errors.New("original"), errors.New("special failed"))

	require.True(t, canceled)
	require.Nil(t, outbound.state.RawStreamCancel)
	require.Nil(t, outbound.state.RawStreamCh)
	require.Nil(t, outbound.state.RawStreamErrRef)
	require.Nil(t, outbound.state.RequestExec)
	require.False(t, outbound.state.PassThroughApplied)
	require.False(t, outbound.state.SpecialRetryActive)
	require.Empty(t, outbound.state.SpecialRetryType)
}

func newEncryptedContentCleanupRetryTestOutbound(
	policy *biz.RetryPolicy,
	apiFormat string,
	model string,
	specialActive bool,
) *PersistentOutboundTransformer {
	return &PersistentOutboundTransformer{
		state: &PersistenceState{
			RetryPolicyProvider: &mockSystemService{retryPolicy: policy},
			RawProviderRequest:  &httpclient.Request{APIFormat: apiFormat},
			CurrentCandidate: &ChannelModelsCandidate{
				Channel: &biz.Channel{Channel: &ent.Channel{ID: 1, Name: "test"}},
				Models: []biz.ChannelModelEntry{
					{RequestModel: model, ActualModel: model},
				},
				APIFormat: apiFormat,
			},
			CurrentModelIndex:  0,
			SpecialRetryActive: specialActive,
		},
	}
}
