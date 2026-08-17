package biz

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/llm/httpclient"
)

func TestCatalogService_FallsBackAndRefreshes(t *testing.T) {
	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=1")
	t.Cleanup(func() { _ = client.Close() })

	ctx := authz.WithTestBypass(context.Background())
	ctx = ent.NewContext(ctx, client)

	system := NewSystemService(SystemServiceParams{
		CacheConfig: xcache.Config{Mode: xcache.ModeMemory},
		Ent:         client,
	})
	svc := NewCatalogService(system, nil)

	snapshot := svc.fallbackCache()
	require.Equal(t, catalogSourceFallback, snapshot.source)
	require.NotEmpty(t, snapshot.raw.Providers)

	payload, err := svc.snapshotFromCache(snapshot, true)
	require.NoError(t, err)
	require.Equal(t, catalogSourceFallback, payload.Source)
	require.True(t, payload.Filtered)
	require.Contains(t, string(payload.Data), `"providers"`)

	upstream := catalogFile{Providers: map[string]catalogProvider{
		"openai": {ID: "openai", Models: []map[string]any{{"id": "gpt-hot"}}},
	}}
	body, err := json.Marshal(upstream)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	require.NoError(t, system.SetCatalogSettings(ctx, CatalogSettings{
		UpstreamURL:    server.URL,
		RefreshSeconds: 60,
	}))

	svc.httpClient = httpclient.NewHttpClientWithClient(server.Client())
	refreshed, err := svc.Refresh(ctx)
	require.NoError(t, err)
	require.Equal(t, catalogSourceUpstream, refreshed.Source)
	require.Contains(t, string(refreshed.Data), "gpt-hot")
	require.WithinDuration(t, time.Now().UTC(), *refreshed.FetchedAt, 5*time.Second)
}
