package orchestrator

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/looplj/axonhub/llm/transformer/shared"
)

type encryptedContentCleanupSessionKey struct {
	scope string
	id    string
}

type encryptedContentCleanupStickyStore struct {
	mu       sync.Mutex
	sessions map[encryptedContentCleanupSessionKey]time.Time
	now      func() time.Time
}

func newEncryptedContentCleanupStickyStore() *encryptedContentCleanupStickyStore {
	return &encryptedContentCleanupStickyStore{
		sessions: make(map[encryptedContentCleanupSessionKey]time.Time),
		now:      time.Now,
	}
}

func encryptedContentCleanupSessionKeyFromContext(ctx context.Context) (encryptedContentCleanupSessionKey, bool) {
	sessionID, ok := shared.GetSessionID(ctx)
	if !ok || strings.TrimSpace(sessionID) == "" {
		return encryptedContentCleanupSessionKey{}, false
	}

	scope, ok := shared.GetSessionScope(ctx)
	if !ok || strings.TrimSpace(scope) == "" {
		return encryptedContentCleanupSessionKey{}, false
	}

	return encryptedContentCleanupSessionKey{
		scope: strings.TrimSpace(scope),
		id:    strings.TrimSpace(sessionID),
	}, true
}

func (s *encryptedContentCleanupStickyStore) Mark(ctx context.Context, ttl time.Duration) {
	if s == nil || ttl <= 0 {
		return
	}

	key, ok := encryptedContentCleanupSessionKeyFromContext(ctx)
	if !ok {
		return
	}

	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupExpiredLocked(now)
	s.sessions[key] = now.Add(ttl)
}

func (s *encryptedContentCleanupStickyStore) Active(ctx context.Context) bool {
	if s == nil {
		return false
	}

	key, ok := encryptedContentCleanupSessionKeyFromContext(ctx)
	if !ok {
		return false
	}

	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	expiresAt, ok := s.sessions[key]
	if !ok {
		return false
	}

	if !expiresAt.After(now) {
		delete(s.sessions, key)
		return false
	}

	return true
}

func (s *encryptedContentCleanupStickyStore) Clear(ctx context.Context) {
	if s == nil {
		return
	}

	key, ok := encryptedContentCleanupSessionKeyFromContext(ctx)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sessions, key)
}

func (s *encryptedContentCleanupStickyStore) cleanupExpiredLocked(now time.Time) {
	for key, expiresAt := range s.sessions {
		if !expiresAt.After(now) {
			delete(s.sessions, key)
		}
	}
}
