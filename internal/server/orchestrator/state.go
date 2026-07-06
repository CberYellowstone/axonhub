package orchestrator

import (
	"context"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
)

// PersistenceState holds shared state with channel management and retry capabilities.
// TODO: move the dependencies out of the state to make it a real state.
type PersistenceState struct {
	APIKey *ent.APIKey

	RequestService                *biz.RequestService
	UsageLogService               *biz.UsageLogService
	ChannelService                *biz.ChannelService
	PromptProvider                PromptProvider
	PromptProtecter               PromptProtecter
	RetryPolicyProvider           RetryPolicyProvider
	CandidateSelector             CandidateSelector
	LoadBalancer                  *LoadBalancer
	EncryptedContentCleanupSticky *encryptedContentCleanupStickyStore

	// Request state
	ModelMapper *ModelMapper
	// Proxy config, will be used to override channel's default proxy config.
	Proxy *httpclient.ProxyConfig

	// OriginalModel is the model after API key profile mapping, used for channel selection
	OriginalModel string
	RawRequest    *httpclient.Request
	LlmRequest    *llm.Request

	// OriginalRequestStream stores the client's original stream intent before any
	// candidate-specific forcing to provider-side streaming happens.
	OriginalRequestStream *bool

	// Persistence state
	Request     *ent.Request
	RequestExec *ent.RequestExecution

	// ChannelModelsCandidates is the primary state for channel selection
	ChannelModelsCandidates []*ChannelModelsCandidate
	// Candidate state - current candidate index of ChannelModelsCandidates
	CurrentCandidateIndex int
	// CurrentCandidate is the currently selected candidate of ChannelModelsCandidates
	CurrentCandidate *ChannelModelsCandidate
	// CurrentModelIndex is the current model index in CurrentCandidate.Models
	CurrentModelIndex int

	// Perf is the performance record for the current request.
	Perf *biz.PerformanceRecord

	// StreamCompleted tracks whether the stream has response successfully completed.
	// This is used to distinguish between a stream that was canceled mid-way
	// versus a stream that completed successfully but the client disconnected
	// immediately after receiving the last chunk.
	StreamCompleted bool

	// RawProviderResponse stores the raw provider response for non-stream response pass-through.
	RawProviderResponse *httpclient.Response

	// RawProviderRequest stores the actual outbound provider request for pass-through checks.
	RawProviderRequest *httpclient.Request

	// RawStreamCh receives raw provider stream events for stream response pass-through.
	RawStreamCh chan *httpclient.StreamEvent

	// RawStreamErrRef points to the current attempt's local error variable used by the
	// captureRawProviderStream fan-out goroutine. Using a per-attempt pointer (instead of
	// a single shared field) prevents data races when retries spawn a new goroutine before
	// the previous one has exited.
	RawStreamErrRef *error

	// RawStreamCancel cancels the current attempt's fan-out goroutine started by
	// captureRawProviderStream. Must be called in PrepareForRetry and NextChannel so the
	// abandoned goroutine exits promptly and releases its upstream HTTP connection.
	RawStreamCancel context.CancelFunc

	// PassThroughApplied records whether the inbound request body was substituted during pass-through.
	PassThroughApplied bool

	// SpecialRetryActive marks the current attempt as a special retry attempt.
	SpecialRetryActive bool
	// SpecialRetryType describes the active special retry strategy.
	SpecialRetryType string
	// SpecialRetryTriggerStatus records the status that triggered the special retry.
	SpecialRetryTriggerStatus int
	// SpecialRetryTriggerMessage records the error text that triggered the special retry.
	SpecialRetryTriggerMessage string
	// EncryptedContentCleanupDefaultApplied records whether sticky default cleanup
	// changed the first outbound body for this attempt.
	EncryptedContentCleanupDefaultApplied bool
	// SpecialRetryOriginalDefaultCleanupApplied snapshots the original attempt state.
	SpecialRetryOriginalDefaultCleanupApplied bool
	// SpecialRetryCleanupBodyApplied records whether the special retry body was changed.
	SpecialRetryCleanupBodyApplied bool
	// PendingCleanupFailurePerf defers the original failure performance record until
	// the cleanup special retry outcome is known.
	PendingCleanupFailurePerf *biz.PerformanceRecord
	// PendingCleanupCircuitBreaker defers the original model circuit-breaker error
	// until the cleanup special retry outcome is known.
	PendingCleanupCircuitBreaker *pendingCleanupCircuitBreakerError
}
