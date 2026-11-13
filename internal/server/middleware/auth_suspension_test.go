// 职责: 验证每条 API Key HTTP 鉴权路径的暂停、重试及故障响应。
// 关联文件:
//
//	internal/server/middleware/auth.go
//	internal/server/biz/api_key_suspension.go
//
// 遵循的 Rules:
//
//	.agents/rules/go-general.md
package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
)

func TestAPIKeyGateHTTPResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	until := time.Now().Add(90 * time.Second)
	for _, tc := range []struct {
		name   string
		err    error
		status int
		retry  bool
	}{
		{"temporary", fmt.Errorf("wrapped: %w", &biz.APIKeySuspendedError{Until: &until}), http.StatusTooManyRequests, true},
		{"permanent", &biz.APIKeySuspendedError{}, http.StatusForbidden, false},
		{"SQL failure", fmt.Errorf("%w: secret database detail", biz.ErrAPIKeyGateUnavailable), http.StatusServiceUnavailable, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := gin.New()
			router.GET("/", func(c *gin.Context) { require.True(t, abortAPIKeyGateError(c, tc.err)) })
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
			require.Equal(t, tc.status, recorder.Code)
			if tc.retry {
				seconds, err := strconv.Atoi(recorder.Header().Get("Retry-After"))
				require.NoError(t, err)
				require.InDelta(t, 90, seconds, 1)
			} else {
				require.Empty(t, recorder.Header().Get("Retry-After"))
			}
			require.NotContains(t, recorder.Body.String(), "secret database detail")
		})
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	require.False(t, abortAPIKeyGateError(ctx, errors.New("unrelated")))
}

func TestAllAPIKeyAuthPathsEnforceSuspensionWithWarmCache(t *testing.T) {
	gin.SetMode(gin.TestMode)
	f := setupAdminGraphqlAuth(t)
	setup := authz.WithTestBypass(ent.NewContext(context.Background(), f.client))
	key := f.client.APIKey.Query().Where(apikey.KeyEQ(f.saKey)).OnlyX(setup)
	manifest := objects.PluginManifest{SchemaVersion: 1, SchemaVersionSet: true, ID: "test.middleware-guard", Name: "Middleware Guard",
		Version: "1.0.0", AxonhubVersion: "*", PluginAPIVersion: 1, PluginAPIVersionSet: true,
		Permissions: []string{objects.PluginPermissionKeySuspensions}}
	require.NoError(t, manifest.Validate())
	installed := f.client.Plugin.Create().SetPluginID(manifest.ID).SetManifest(manifest).SetEnabled(true).
		SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions}).
		SetAutomationScope(objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: key.ProjectID, AllKeys: true}}}).SaveX(setup)
	_, err := f.auth.AuthenticateAPIKey(ent.NewContext(context.Background(), f.client), f.saKey)
	require.NoError(t, err)
	f.auth.AllowNoAuth = true
	_, err = biz.NewAPIKeySuspensionService(f.client).Apply(authz.NewSystemContext(ent.NewContext(context.Background(), f.client)), installed.ID,
		objects.APIKeySuspensionInput{APIKeyID: key.ID, ProjectID: key.ProjectID, IdempotencyKey: "http-warm", Reason: "private incident details", Evidence: map[string]any{"secret": "do-not-return"}, DurationSeconds: 120})
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		handler gin.HandlerFunc
		query   bool
	}{
		{"standard", WithAPIKeyAuth(f.auth), false},
		{"strict", WithStrictAPIKeyConfig(f.auth, nil), false},
		{"openapi", WithOpenAPIAuth(f.auth), false},
		{"admin", WithAdminGraphqlAuth(f.auth), false},
		{"gemini-header", WithGeminiKeyAuth(f.auth), false},
		{"gemini-query", WithGeminiKeyAuth(f.auth), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := gin.New()
			router.Use(WithEntClient(f.client))
			router.GET("/", tc.handler, func(c *gin.Context) { t.Error("suspended request reached handler"); c.Status(http.StatusOK) })
			url := "/"
			if tc.query {
				url += "?key=" + f.saKey
			}
			req := httptest.NewRequest(http.MethodGet, url, nil)
			if !tc.query {
				req.Header.Set("Authorization", "Bearer "+f.saKey)
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			require.Equal(t, http.StatusTooManyRequests, recorder.Code)
			require.NotEmpty(t, recorder.Header().Get("Retry-After"))
			require.NotContains(t, recorder.Body.String(), "private incident details")
			require.NotContains(t, recorder.Body.String(), "do-not-return")
		})
	}
}
