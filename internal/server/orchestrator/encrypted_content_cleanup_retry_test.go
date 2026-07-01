package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
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
	require.False(t, gjson.GetBytes(processed.Body, "input.0.encrypted_content").Exists())
	require.False(t, gjson.GetBytes(processed.Body, "input.0.reasoning_signature").Exists())
	require.False(t, gjson.GetBytes(processed.Body, "input.0.encryptedContent").Exists())
	require.False(t, gjson.GetBytes(processed.Body, "input.1.content.0.reasoningSignature").Exists())
	require.False(t, gjson.GetBytes(processed.Body, "tools.0.input_schema.encrypted_content").Exists())

	input := gjson.GetBytes(processed.Body, "input").Array()
	require.Len(t, input, 4)
	require.Equal(t, "reasoning", input[0].Get("type").String())
	require.Equal(t, "kept", input[0].Get("summary.0.text").String())
	require.Equal(t, "function_call", input[2].Get("type").String())
	require.Equal(t, "call_1", input[2].Get("call_id").String())
	require.Equal(t, "function_call_output", input[3].Get("type").String())
	require.Equal(t, "call_1", input[3].Get("call_id").String())
	require.Equal(t, "high", gjson.GetBytes(processed.Body, "reasoning.effort").String())
	require.Equal(t, "message.output_text.logprobs", gjson.GetBytes(processed.Body, "include.0").String())
	require.Equal(t, "auto", gjson.GetBytes(processed.Body, "tool_choice").String())
	require.True(t, gjson.GetBytes(processed.Body, "parallel_tool_calls").Bool())
	require.Equal(t, "function", gjson.GetBytes(processed.Body, "tools.0.type").String())
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
