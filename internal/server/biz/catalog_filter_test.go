package biz

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterCatalogProviders(t *testing.T) {
	t.Parallel()

	input := catalogFile{Providers: map[string]catalogProvider{
		"openai": {
			ID: "openai",
			Models: []map[string]any{
				{"id": "gpt-5.6-luna", "release_date": "2026-03-01"},
			},
		},
		"unknown-lab": {
			ID: "unknown-lab",
			Models: []map[string]any{
				{"id": "secret-model"},
			},
		},
		"llama": {
			ID: "llama",
			Models: []map[string]any{
				{"id": "llama-4"},
				{"id": "other-7b"},
			},
		},
		"nvidia": {
			ID: "nvidia",
			Models: []map[string]any{
				{"id": "nvidia/nemotron"},
				{"id": "meta/llama-hosted"},
			},
		},
		"some-host": {
			ID: "some-host",
			Models: []map[string]any{
				{"id": "kwaipilot/kat-coder-pro", "family": "kat-coder"},
			},
		},
		"thinkingmachines": {
			ID: "thinkingmachines",
			Models: []map[string]any{
				{"id": "tinker-only"},
			},
		},
	}}

	filtered := filterCatalogProviders(input, DefaultDeveloperIDs)
	require.Contains(t, filtered.Providers, "openai")
	require.NotContains(t, filtered.Providers, "unknown-lab")
	require.Equal(t, "meta", filtered.Providers["meta"].ID)
	require.Equal(t, "llama-4", filtered.Providers["meta"].Models[0]["id"])
	require.Len(t, filtered.Providers["nvidia"].Models, 1)
	require.Equal(t, "nvidia/nemotron", filtered.Providers["nvidia"].Models[0]["id"])
	require.Equal(t, "kat-coder-pro", filtered.Providers["kwaipilot"].Models[0]["id"])
	require.NotContains(t, filtered.Providers, "thinkingmachines")

	extra := []byte(`{"thinkingmachines":[{"id":"tm-canonical","release_date":"2026-01-01"}]}`)
	require.NoError(t, mergeExtraModels(&filtered, extra))
	require.Equal(t, "tm-canonical", filtered.Providers["thinkingmachines"].Models[0]["id"])
}

func TestValidateCatalogSettings(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateCatalogSettings(CatalogSettings{}))
	require.Error(t, validateCatalogSettings(CatalogSettings{UpstreamURL: "ftp://x"}))
	require.Error(t, validateCatalogSettings(CatalogSettings{UpstreamURL: "not-a-url"}))
	require.NoError(t, validateCatalogSettings(CatalogSettings{UpstreamURL: DefaultCatalogUpstreamURL}))
}

func TestCatalogFallbackJSONParses(t *testing.T) {
	t.Parallel()

	var data catalogFile
	require.NoError(t, json.Unmarshal(catalogFallbackJSON, &data))
	require.NotEmpty(t, data.Providers)
}
