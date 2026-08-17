package biz

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/scheduler"
	"github.com/looplj/axonhub/llm/httpclient"
)

const (
	catalogSourceUpstream = "upstream"
	catalogSourceFallback = "fallback"
	catalogFetchTimeout   = 20 * time.Second
)

// Bundled snapshots. scripts/sync/sync-model-developers.js copies providers.json here.
//
//go:embed catalogdata/providers.json
var catalogFallbackJSON []byte

//go:embed catalogdata/models.json
var catalogExtraModelsJSON []byte

// CatalogSnapshot is the catalog payload served to the admin UI.
type CatalogSnapshot struct {
	Data      objects.JSONRawMessage `json:"data"`
	FetchedAt *time.Time             `json:"fetched_at,omitempty"`
	Source    string                 `json:"source"`
	Filtered  bool                   `json:"filtered"`
}

type catalogCache struct {
	raw       catalogFile
	fetchedAt time.Time
	source    string
}

// CatalogService fetches the public model catalog, applies AxonHub developer
// filters, and serves the result through GraphQL.
type CatalogService struct {
	system     *SystemService
	httpClient *httpclient.HttpClient
	scheduler  *scheduler.Scheduler

	mu    sync.RWMutex
	cache *catalogCache
}

func NewCatalogService(system *SystemService, httpClient *httpclient.HttpClient) *CatalogService {
	return &CatalogService{
		system:     system,
		httpClient: httpClient,
	}
}

func (s *CatalogService) RegisterScheduledTasks(ctx context.Context, sched *scheduler.Scheduler) error {
	s.scheduler = sched
	settings := s.system.CatalogSettingsOrDefault(ctx)

	if err := sched.Register(ctx, s.taskSpec(settings), s.refreshPeriodically); err != nil {
		return err
	}

	go func() {
		if _, err := s.Refresh(context.Background()); err != nil {
			log.Warn(context.Background(), "initial catalog refresh failed", log.Cause(err))
		}
	}()

	return nil
}

func (s *CatalogService) Reschedule(ctx context.Context, sched *scheduler.Scheduler) {
	if sched == nil {
		return
	}

	settings := s.system.CatalogSettingsOrDefault(ctx)
	if err := sched.Reschedule(ctx, "providers-catalog-refresh", s.taskSpec(settings)); err != nil {
		log.Warn(ctx, "failed to reschedule catalog refresh", log.Cause(err))
	}
}

func (s *CatalogService) taskSpec(settings CatalogSettings) scheduler.TaskSpec {
	return scheduler.TaskSpec{
		Name:        "providers-catalog-refresh",
		Description: "Refresh built-in model catalog from upstream",
		FixRate:     time.Duration(settings.RefreshSeconds) * time.Second,
	}
}

func (s *CatalogService) refreshPeriodically(ctx context.Context) {
	if _, err := s.Refresh(ctx); err != nil {
		log.Warn(ctx, "scheduled catalog refresh failed", log.Cause(err))
	}
}

func (s *CatalogService) Snapshot(ctx context.Context, filtered bool) (CatalogSnapshot, error) {
	return s.snapshotFromCache(s.ensureCache(ctx), filtered)
}

func (s *CatalogService) Refresh(ctx context.Context) (CatalogSnapshot, error) {
	settings := s.system.CatalogSettingsOrDefault(ctx)
	raw, err := s.fetchUpstream(ctx, settings.EffectiveUpstreamURL())
	if err != nil {
		return CatalogSnapshot{}, err
	}

	s.storeCache(raw, catalogSourceUpstream)

	return s.snapshotFromCache(s.cached(), true)
}

func (s *CatalogService) ensureCache(ctx context.Context) *catalogCache {
	if cached := s.cached(); cached != nil {
		settings := s.system.CatalogSettingsOrDefault(ctx)
		if time.Since(cached.fetchedAt) < time.Duration(settings.RefreshSeconds)*time.Second {
			return cached
		}
	}

	settings := s.system.CatalogSettingsOrDefault(ctx)

	fetchCtx, cancel := context.WithTimeout(ctx, catalogFetchTimeout)
	defer cancel()

	raw, err := s.fetchUpstream(fetchCtx, settings.EffectiveUpstreamURL())
	if err != nil {
		log.Warn(ctx, "catalog fetch failed, using fallback", log.Cause(err))
		if cached := s.cached(); cached != nil {
			return cached
		}

		return s.fallbackCache()
	}

	s.storeCache(raw, catalogSourceUpstream)

	return s.cached()
}

func (s *CatalogService) fetchUpstream(ctx context.Context, upstreamURL string) (catalogFile, error) {
	if s.httpClient == nil {
		return catalogFile{}, fmt.Errorf("catalog http client is not configured")
	}

	resp, err := s.httpClient.Do(ctx, &httpclient.Request{
		Method: http.MethodGet,
		URL:    upstreamURL,
		Headers: http.Header{
			"Accept": []string{"application/json"},
		},
	})
	if err != nil {
		return catalogFile{}, fmt.Errorf("fetch catalog: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return catalogFile{}, fmt.Errorf("fetch catalog: unexpected status %d", resp.StatusCode)
	}

	var data catalogFile
	if err := json.Unmarshal(resp.Body, &data); err != nil {
		return catalogFile{}, fmt.Errorf("parse catalog: %w", err)
	}

	if data.Providers == nil {
		return catalogFile{}, fmt.Errorf("catalog is missing providers")
	}

	return data, nil
}

func (s *CatalogService) snapshotFromCache(cache *catalogCache, filtered bool) (CatalogSnapshot, error) {
	data := cache.raw
	if filtered {
		prepared := filterCatalogProviders(data, DefaultDeveloperIDs)
		if err := mergeExtraModels(&prepared, catalogExtraModelsJSON); err != nil {
			return CatalogSnapshot{}, fmt.Errorf("merge extra catalog models: %w", err)
		}
		sortCatalogModels(&prepared)
		data = prepared
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return CatalogSnapshot{}, err
	}

	fetchedAt := cache.fetchedAt
	source := cache.source
	if source == "" {
		source = catalogSourceFallback
	}

	return CatalogSnapshot{
		Data:      objects.JSONRawMessage(payload),
		FetchedAt: &fetchedAt,
		Source:    source,
		Filtered:  filtered,
	}, nil
}

func (s *CatalogService) storeCache(raw catalogFile, source string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache = &catalogCache{
		raw:       raw,
		fetchedAt: time.Now().UTC(),
		source:    source,
	}
}

func (s *CatalogService) cached() *catalogCache {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.cache
}

func (s *CatalogService) fallbackCache() *catalogCache {
	var data catalogFile
	if err := json.Unmarshal(catalogFallbackJSON, &data); err != nil || data.Providers == nil {
		data = catalogFile{Providers: map[string]catalogProvider{}}
	}

	now := time.Now().UTC()

	return &catalogCache{
		raw:       data,
		fetchedAt: now,
		source:    catalogSourceFallback,
	}
}

func (s *CatalogService) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache = nil
}
