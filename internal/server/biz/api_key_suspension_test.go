// 职责: 验证插件暂停的授权边界、并发幂等、生命周期与无缓存鉴权。
// 关联文件:
//
//	internal/server/biz/api_key_suspension.go
//	internal/server/biz/plugin_automation_access.go
//	internal/server/biz/auth.go
//	internal/server/biz/database_clock.go
//
// 遵循的 Rules:
//
//	.agents/rules/biz-services.md
//	.agents/rules/ent-graphql.md
package biz

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/apikeysuspension"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/pkg/xcache"
	"github.com/looplj/axonhub/llm/httpclient"
)

type suspensionFixture struct {
	db        *ent.Client
	svc       *APIKeySuspensionService
	setup     context.Context
	host      context.Context
	owner     context.Context
	key       *ent.APIKey
	installed *ent.Plugin
}

func suspensionTestManifest(id string) objects.PluginManifest {
	return objects.PluginManifest{SchemaVersion: 1, SchemaVersionSet: true, ID: id, Name: id,
		Version: "1.0.0", AxonhubVersion: "*", PluginAPIVersion: 1, PluginAPIVersionSet: true,
		Permissions: []string{objects.PluginPermissionKeySuspensions, objects.PluginPermissionUsageMetrics, "webhook"}}
}

func newSuspensionFixture(t *testing.T) suspensionFixture {
	t.Helper()
	db := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=0")
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	setup := authz.WithTestBypass(ent.NewContext(context.Background(), db))
	project := db.Project.Create().SetName("suspension-project").SaveX(setup)
	key := db.APIKey.Create().SetName("key").SetKey("ah-suspension-key").SetProjectID(project.ID).SetType(apikey.TypeServiceAccount).SaveX(setup)
	manifest := suspensionTestManifest("test.cache-guard")
	require.NoError(t, manifest.Validate())
	installed := db.Plugin.Create().SetPluginID(manifest.ID).SetManifest(manifest).SetEnabled(true).
		SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions, objects.PluginPermissionUsageMetrics}).
		SetAutomationScope(objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: project.ID, AllKeys: true}}}).SaveX(setup)
	u := db.User.Create().SetEmail("suspension-owner@example.com").SetPassword("test").SetIsOwner(true).SaveX(setup)
	owner := contexts.WithUser(authz.NewUserContext(ent.NewContext(context.Background(), db), u.ID), u)
	owner = contexts.WithProjectID(owner, project.ID)
	return suspensionFixture{db: db, svc: NewAPIKeySuspensionService(db), setup: setup,
		host: authz.NewSystemContext(ent.NewContext(context.Background(), db)), owner: owner, key: key, installed: installed}
}

func (f suspensionFixture) input(idempotency string) objects.APIKeySuspensionInput {
	return objects.APIKeySuspensionInput{APIKeyID: f.key.ID, ProjectID: f.key.ProjectID, IdempotencyKey: idempotency,
		Reason: "cache hit ratio below threshold", Evidence: map[string]any{"windowSeconds": 300, "ratio": 0.1}, DurationSeconds: 60}
}

func TestAPIKeySuspensionAuthorizationAndLiveGrants(t *testing.T) {
	f := newSuspensionFixture(t)
	for name, ctx := range map[string]context.Context{"anonymous": context.Background(), "direct owner": f.owner, "API key": authz.NewAPIKeyContext(context.Background(), f.key.ID, f.key.ProjectID)} {
		t.Run(name, func(t *testing.T) { _, err := f.svc.Apply(ctx, f.installed.ID, f.input(name)); require.Error(t, err) })
	}
	delegated, err := authz.DelegatePlugin(f.owner, f.installed.ID, []string{objects.PluginPermissionKeySuspensions})
	require.NoError(t, err)
	_, err = f.svc.Apply(delegated, f.installed.ID, f.input("delegated"))
	require.NoError(t, err)
	_, err = f.svc.Apply(delegated, f.installed.ID+1, f.input("wrong-installation"))
	require.Error(t, err)
	noGrant, err := authz.DelegatePlugin(f.owner, f.installed.ID, nil)
	require.NoError(t, err)
	_, err = f.svc.Apply(noGrant, f.installed.ID, f.input("no-delegation-grant"))
	require.Error(t, err)

	f.db.Plugin.UpdateOneID(f.installed.ID).SetGrantedPermissions(nil).ExecX(f.setup)
	_, err = f.svc.Apply(f.host, f.installed.ID, f.input("revoked"))
	require.Error(t, err)
	_, err = f.svc.Apply(delegated, f.installed.ID, f.input("snapshot-revoked"))
	require.Error(t, err, "delegation snapshot cannot restore revoked live permission")
	f.db.Plugin.UpdateOneID(f.installed.ID).SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions}).SetEnabled(false).ExecX(f.setup)
	_, err = f.svc.Apply(f.host, f.installed.ID, f.input("disabled"))
	require.Error(t, err)
	f.db.Plugin.UpdateOneID(f.installed.ID).SetEnabled(true).SetDeletedAt(1).ExecX(f.setup)
	_, err = f.svc.Apply(f.host, f.installed.ID, f.input("deleted"))
	require.Error(t, err)
}

func TestAPIKeySuspensionScopeAndCurrentTarget(t *testing.T) {
	f := newSuspensionFixture(t)
	other := f.db.Project.Create().SetName("other-project").SaveX(f.setup)
	otherKey := f.db.APIKey.Create().SetName("foreign").SetKey("foreign").SetProjectID(other.ID).SaveX(f.setup)
	for name, input := range map[string]objects.APIKeySuspensionInput{
		"other project":  {APIKeyID: otherKey.ID, ProjectID: other.ID, IdempotencyKey: "foreign", Reason: "reason", DurationSeconds: 1},
		"forged project": {APIKeyID: otherKey.ID, ProjectID: f.key.ProjectID, IdempotencyKey: "forged", Reason: "reason", DurationSeconds: 1},
		"unknown key":    {APIKeyID: 999999, ProjectID: f.key.ProjectID, IdempotencyKey: "missing", Reason: "reason", DurationSeconds: 1},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := f.svc.Apply(f.host, f.installed.ID, input)
			require.Error(t, err)
			require.Nil(t, result)
		})
	}
	for _, keyType := range []apikey.Type{apikey.TypePersonal, apikey.TypeNoauth} {
		key := f.db.APIKey.Create().SetName(string(keyType)).SetKey(string(keyType)).SetProjectID(f.key.ProjectID).SetType(keyType).SaveX(f.setup)
		input := f.input(string(keyType))
		input.APIKeyID = key.ID
		_, err := f.svc.Apply(f.host, f.installed.ID, input)
		require.Error(t, err)
	}
	created, err := f.svc.Apply(f.host, f.installed.ID, f.input("before-exclusion"))
	require.NoError(t, err)
	f.db.Plugin.UpdateOneID(f.installed.ID).SetAutomationScope(objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: f.key.ProjectID, AllKeys: true, ExcludedAPIKeyIDs: []int{f.key.ID}}}}).ExecX(f.setup)
	_, err = f.svc.Apply(f.host, f.installed.ID, f.input("excluded"))
	require.Error(t, err)
	result, err := f.svc.ReleaseOwn(f.host, f.installed.ID, created.ID)
	require.Error(t, err)
	require.Nil(t, result)
	listed, err := f.svc.List(f.host, f.installed.ID, objects.APIKeySuspensionFilter{})
	require.NoError(t, err)
	require.Empty(t, listed)
	var suspended *APIKeySuspendedError
	require.ErrorAs(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID), &suspended, "removing scope does not silently release a hold")
	require.NoError(t, f.svc.Check(f.host, f.key.ID, other.ID), "hold is bound to its recorded project")
}

func TestAPIKeySuspensionIdempotencyAndReleaseCAS(t *testing.T) {
	f := newSuspensionFixture(t)
	input := f.input("same-evidence")
	first, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	replay, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	require.Equal(t, first.ID, replay.ID)
	require.True(t, first.ExpiresAt.Equal(*replay.ExpiresAt))
	other := f.db.APIKey.Create().SetName("other").SetKey("other").SetProjectID(f.key.ProjectID).SaveX(f.setup)
	conflict := input
	conflict.APIKeyID = other.ID
	_, err = f.svc.Apply(f.host, f.installed.ID, conflict)
	require.ErrorIs(t, err, ErrSuspensionIdempotencyConflict)
	released, err := f.svc.ReleaseOwn(f.host, f.installed.ID, first.ID)
	require.NoError(t, err)
	require.NotNil(t, released.ReleasedAt)
	require.NotEmpty(t, released.ReleasedBy)
	adminReplay, err := f.svc.AdminRelease(f.owner, first.ID, "different actor cannot replace original audit")
	require.NoError(t, err)
	require.Equal(t, released.ReleasedBy, adminReplay.ReleasedBy)
	require.True(t, released.ReleasedAt.Equal(*adminReplay.ReleasedAt))
	input.Permanent = true
	input.DurationSeconds = 0
	replay, err = f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	require.NotNil(t, replay.ReleasedAt, "replaying a permanent request cannot re-activate a released record")
	require.NoError(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID))
}

func TestAPIKeySuspensionRejectsMovedCurrentKey(t *testing.T) {
	f := newSuspensionFixture(t)
	hold, err := f.svc.Apply(f.host, f.installed.ID, f.input("before-move"))
	require.NoError(t, err)
	other := f.db.Project.Create().SetName("moved-project").SaveX(f.setup)
	// The normal schema makes project_id immutable. Simulate an external move to
	// prove that an old installation ceiling and audit row cannot authorize it.
	f.db.APIKey.UpdateOneID(f.key.ID).SetUpdatedAt(time.Now()).Modify(func(u *sql.UpdateBuilder) { u.Set(apikey.FieldProjectID, other.ID) }).SaveX(f.setup)
	_, err = f.svc.Apply(f.host, f.installed.ID, f.input("after-move"))
	require.Error(t, err)
	_, err = f.svc.ReleaseOwn(f.host, f.installed.ID, hold.ID)
	require.Error(t, err)
	listed, err := f.svc.List(f.host, f.installed.ID, objects.APIKeySuspensionFilter{})
	require.NoError(t, err)
	require.Empty(t, listed)
	_, err = f.svc.AdminRelease(f.owner, hold.ID, "owner independently releases historical hold")
	require.NoError(t, err)
}

func TestAPIKeySuspensionExpiredReplayAndSnapshot(t *testing.T) {
	f := newSuspensionFixture(t)
	input := f.input("expired-evidence")
	hash := sha256.Sum256([]byte(input.IdempotencyKey))
	expired := f.db.APIKeySuspension.Create().SetInstallationID(f.installed.ID).SetPluginID(f.installed.PluginID).
		SetAPIKeyID(f.key.ID).SetProjectID(f.key.ProjectID).SetIdempotencyKeyHash(hex.EncodeToString(hash[:])).
		SetReason(input.Reason).SetEvidence(input.Evidence).SetCreatedBy("system:test").SetExpiresAt(time.Now().UTC().Add(-time.Minute)).SaveX(f.setup)
	require.NoError(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID))
	replayed, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	require.Equal(t, expired.ID, replayed.ID)
	require.True(t, expired.ExpiresAt.Equal(*replayed.ExpiresAt))
	require.NoError(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID))
	input = f.input("snapshot")
	input.Evidence["nested"] = map[string]any{"value": "original"}
	created, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	input.Evidence["nested"].(map[string]any)["value"] = "changed"
	reloaded := f.db.APIKeySuspension.GetX(f.setup, created.ID)
	require.Equal(t, "original", reloaded.Evidence["nested"].(map[string]any)["value"])
}

func TestAPIKeySuspensionMultipleHoldsAndUninstall(t *testing.T) {
	f := newSuspensionFixture(t)
	short, err := f.svc.Apply(f.host, f.installed.ID, f.input("short"))
	require.NoError(t, err)
	input := f.input("long")
	input.DurationSeconds = 600
	long, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	var suspended *APIKeySuspendedError
	require.ErrorAs(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID), &suspended)
	require.True(t, long.ExpiresAt.Equal(*suspended.Until))
	require.InDelta(t, 600, suspended.RetryAfter(time.Now()), 1)
	input = f.input("permanent")
	input.Permanent = true
	input.DurationSeconds = 0
	permanent, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	require.ErrorAs(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID), &suspended)
	require.Nil(t, suspended.Until)
	_, err = f.svc.ReleaseOwn(f.host, f.installed.ID, long.ID)
	require.NoError(t, err)
	_, err = f.svc.ReleaseOwn(f.host, f.installed.ID, short.ID)
	require.NoError(t, err)
	require.ErrorAs(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID), &suspended)
	f.db.Plugin.UpdateOneID(f.installed.ID).SetDeletedAt(1).SetEnabled(false).ExecX(f.setup)
	require.ErrorAs(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID), &suspended, "uninstall must preserve holds")
	_, err = f.svc.ReleaseOwn(f.host, f.installed.ID, permanent.ID)
	require.Error(t, err)
	_, err = f.svc.AdminRelease(f.owner, permanent.ID, "operator resolved the incident")
	require.NoError(t, err)
	require.NoError(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID))
	require.Equal(t, apikey.StatusEnabled, f.db.APIKey.GetX(f.setup, f.key.ID).Status)
}

func TestAPIKeySuspensionCrossInstallationAndAdminProject(t *testing.T) {
	f := newSuspensionFixture(t)
	hold, err := f.svc.Apply(f.host, f.installed.ID, f.input("hold"))
	require.NoError(t, err)
	other := f.db.Plugin.Create().SetPluginID("test.other-installation").SetManifest(suspensionTestManifest("test.other-installation")).SetEnabled(true).SetGrantedPermissions(f.installed.GrantedPermissions).SetAutomationScope(f.installed.AutomationScope).SaveX(f.setup)
	result, err := f.svc.ReleaseOwn(f.host, other.ID, hold.ID)
	require.Error(t, err)
	require.Nil(t, result)
	listed, err := f.svc.List(f.host, other.ID, objects.APIKeySuspensionFilter{})
	require.NoError(t, err)
	require.Empty(t, listed)
	delegated, err := authz.DelegatePlugin(f.owner, f.installed.ID, []string{objects.PluginPermissionKeySuspensions, "write_api_keys"})
	require.NoError(t, err)
	_, err = f.svc.AdminRelease(delegated, hold.ID, "plugin cannot act as admin")
	require.Error(t, err)
	u := f.db.User.Create().SetEmail("scoped-admin@example.com").SetPassword("test").SaveX(f.setup)
	otherProject := f.db.Project.Create().SetName("admin-other-project").SaveX(f.setup)
	membership := f.db.UserProject.Create().SetUserID(u.ID).SetProjectID(otherProject.ID).SetScopes([]string{"write_api_keys", "read_api_keys"}).SaveX(f.setup)
	u.Edges.ProjectUsers = []*ent.UserProject{membership}
	admin := contexts.WithProjectID(contexts.WithUser(authz.NewUserContext(context.Background(), u.ID), u), otherProject.ID)
	result, err = f.svc.AdminRelease(admin, hold.ID, "wrong project")
	require.Error(t, err)
	require.Nil(t, result)
	_, err = f.svc.AdminList(admin, objects.APIKeySuspensionFilter{ProjectID: f.key.ProjectID})
	require.Error(t, err)
	targetMembership := f.db.UserProject.Create().SetUserID(u.ID).SetProjectID(f.key.ProjectID).SetScopes([]string{"write_api_keys", "read_api_keys"}).SaveX(f.setup)
	u.Edges.ProjectUsers = append(u.Edges.ProjectUsers, targetMembership)
	admin = contexts.WithProjectID(contexts.WithUser(authz.NewUserContext(context.Background(), u.ID), u), f.key.ProjectID)
	listed, err = f.svc.AdminList(admin, objects.APIKeySuspensionFilter{ProjectID: f.key.ProjectID})
	require.NoError(t, err)
	require.Len(t, listed, 1)
	released, err := f.svc.AdminRelease(admin, hold.ID, "scoped project operator resolved incident")
	require.NoError(t, err)
	require.NotNil(t, released.ReleasedAt)
	require.Equal(t, fmt.Sprintf("user:%d", u.ID), released.ReleasedBy)
}

func TestAPIKeySuspensionConcurrentIdempotency(t *testing.T) {
	f := newSuspensionFixture(t)
	const workers = 6
	var wg sync.WaitGroup
	results := make(chan int, workers)
	errorsOut := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Logf("suspension worker panic: %v", r)
					errorsOut <- fmt.Errorf("suspension worker panic: %v", r)
				}
			}()
			hold, err := f.svc.Apply(f.host, f.installed.ID, f.input("concurrent"))
			if err != nil {
				errorsOut <- err
				return
			}
			results <- hold.ID
		}()
	}
	wg.Wait()
	close(results)
	close(errorsOut)
	for err := range errorsOut {
		require.NoError(t, err)
	}
	id := 0
	count := 0
	for result := range results {
		if id == 0 {
			id = result
		}
		require.Equal(t, id, result)
		count++
	}
	require.Equal(t, workers, count)
	require.Equal(t, 1, f.db.APIKeySuspension.Query().CountX(f.setup))
}

func TestAPIKeySuspensionRejectsUnboundedOrAmbiguousInput(t *testing.T) {
	f := newSuspensionFixture(t)
	for name, mutate := range map[string]func(*objects.APIKeySuspensionInput){
		"no duration":             func(i *objects.APIKeySuspensionInput) { i.DurationSeconds = 0 },
		"permanent with duration": func(i *objects.APIKeySuspensionInput) { i.Permanent = true },
		"negative duration":       func(i *objects.APIKeySuspensionInput) { i.DurationSeconds = -1 },
		"long reason":             func(i *objects.APIKeySuspensionInput) { i.Reason = strings.Repeat("x", 513) },
		"invalid reason UTF8":     func(i *objects.APIKeySuspensionInput) { i.Reason = string([]byte{0xff}) },
		"long evidence": func(i *objects.APIKeySuspensionInput) {
			i.Evidence = map[string]any{"data": strings.Repeat("x", objects.MaxKeySuspensionEvidenceBytes)}
		},
		"non JSON evidence": func(i *objects.APIKeySuspensionInput) { i.Evidence = map[string]any{"data": func() {}} },
		"long idempotency":  func(i *objects.APIKeySuspensionInput) { i.IdempotencyKey = strings.Repeat("x", 257) },
	} {
		t.Run(name, func(t *testing.T) {
			input := f.input(name)
			mutate(&input)
			_, err := f.svc.Apply(f.host, f.installed.ID, input)
			require.Error(t, err)
		})
	}
	require.Zero(t, f.db.APIKeySuspension.Query().CountX(f.setup))
}

func TestAuthServiceSuspensionGateOnWarmCacheAndManualDisable(t *testing.T) {
	auth, db, cleanup := setupTestAuthService(t, xcache.Config{Mode: xcache.ModeMemory})
	defer cleanup()
	defer db.Close()
	setup := authz.WithTestBypass(ent.NewContext(context.Background(), db))
	project := db.Project.Create().SetName("warm-cache-project").SaveX(setup)
	key := db.APIKey.Create().SetName("warm-cache").SetKey("warm-cache-key").SetProjectID(project.ID).SaveX(setup)
	installed := db.Plugin.Create().SetPluginID("test.warm-cache-plugin").SetManifest(suspensionTestManifest("test.warm-cache-plugin")).SetEnabled(true).SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions}).SetAutomationScope(objects.PluginAutomationScope{Projects: []objects.PluginProjectAccess{{ProjectID: project.ID, AllKeys: true}}}).SaveX(setup)
	host := authz.NewSystemContext(ent.NewContext(context.Background(), db))
	requestCtx := ent.NewContext(context.Background(), db)
	svc := NewAPIKeySuspensionService(db)
	_, err := auth.AuthenticateAPIKey(requestCtx, key.Key)
	require.NoError(t, err)
	hold, err := svc.Apply(host, installed.ID, objects.APIKeySuspensionInput{APIKeyID: key.ID, ProjectID: key.ProjectID, IdempotencyKey: "warm", Reason: "reason", DurationSeconds: 60})
	require.NoError(t, err)
	var suspended *APIKeySuspendedError
	_, err = auth.AuthenticateAPIKey(requestCtx, key.Key)
	require.ErrorAs(t, err, &suspended)
	db.APIKey.UpdateOneID(key.ID).SetStatus(apikey.StatusDisabled).ExecX(setup)
	_, err = svc.ReleaseOwn(host, installed.ID, hold.ID)
	require.NoError(t, err)
	_, err = auth.AuthenticateAPIKey(requestCtx, key.Key)
	require.ErrorIs(t, err, ErrInvalidAPIKey)
	require.Equal(t, apikey.StatusDisabled, db.APIKey.GetX(setup, key.ID).Status)
	db.APIKey.UpdateOneID(key.ID).SetStatus(apikey.StatusEnabled).SetKey("rotated-value").ExecX(setup)
	_, err = auth.AuthenticateAPIKey(requestCtx, key.Key)
	require.ErrorIs(t, err, ErrInvalidAPIKey, "warm cache must not accept a rotated credential")
}

func TestAPIKeySuspensionGateFailsClosed(t *testing.T) {
	f := newSuspensionFixture(t)
	require.Error(t, f.svc.Check(f.owner, f.key.ID, f.key.ProjectID))
	// A cancelled gate cannot be interpreted as a successful or invalid credential.
	ctx, cancel := context.WithCancel(f.host)
	cancel()
	err := f.svc.Check(ctx, f.key.ID, f.key.ProjectID)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrInvalidAPIKey))
	require.ErrorIs(t, err, ErrAPIKeyGateUnavailable)
	require.NoError(t, f.db.Close())
	require.ErrorIs(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID), ErrAPIKeyGateUnavailable)
}

func TestAPIKeySuspensionNotificationValidationIsAtomic(t *testing.T) {
	f := newSuspensionFixture(t)
	input := f.input("atomic-notification")
	input.Notification = &PluginNotificationInput{TargetName: "missing", Payload: map[string]any{"message": "paused"}}
	_, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.Error(t, err, "missing webhook grant must roll back the hold")
	require.Zero(t, f.db.APIKeySuspension.Query().CountX(f.setup))
	require.Zero(t, f.db.PluginNotification.Query().CountX(f.setup))
	f.db.Plugin.UpdateOneID(f.installed.ID).SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions, "webhook"}).ExecX(f.setup)
	_, err = f.svc.Apply(f.host, f.installed.ID, input)
	require.ErrorIs(t, err, ErrWebhookTargetNotFound)
	require.Zero(t, f.db.APIKeySuspension.Query().CountX(f.setup))
	require.Zero(t, f.db.PluginNotification.Query().CountX(f.setup))
	delegated, err := authz.DelegatePlugin(f.owner, f.installed.ID, []string{objects.PluginPermissionKeySuspensions})
	require.NoError(t, err)
	_, err = f.svc.Apply(delegated, f.installed.ID, input)
	require.Error(t, err, "host fixed writes must not erase the delegated webhook ceiling")
	require.Zero(t, f.db.APIKeySuspension.Query().CountX(f.setup))
}

func TestAPIKeySuspensionReusesClientOnlyFencedTransaction(t *testing.T) {
	f := newSuspensionFixture(t)
	newTestSystemServiceWithWebhookConfig(t, f.db, WebhookNotifierConfig{Targets: []WebhookTarget{{Name: "ops", Enabled: true, URL: "https://example.invalid", Body: "{{.Data | json}}"}}})
	f.db.Plugin.UpdateOneID(f.installed.ID).SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions, "webhook"}).ExecX(f.setup)
	tx, err := f.db.Tx(f.host)
	require.NoError(t, err)
	defer tx.Rollback()
	// Deliberately omit ent.NewTxContext: the scheduler's fence owns this tx.
	txCtx := ent.NewContext(f.host, tx.Client())
	input := f.input("client-only-transaction")
	input.Notification = &PluginNotificationInput{TargetName: "ops", Payload: map[string]any{"message": "paused"}}
	hold, err := f.svc.Apply(txCtx, f.installed.ID, input)
	require.NoError(t, err)
	_, err = f.svc.ReleaseOwn(txCtx, f.installed.ID, hold.ID)
	require.NoError(t, err)
	require.Equal(t, 1, tx.Client().APIKeySuspension.Query().CountX(authz.WithSystemBypass(txCtx, "test-fenced-suspension")))
	require.Equal(t, 1, tx.Client().PluginNotification.Query().CountX(authz.WithSystemBypass(txCtx, "test-fenced-notification")))
	require.NoError(t, tx.Rollback())
	require.Zero(t, f.db.APIKeySuspension.Query().CountX(f.setup))
	require.Zero(t, f.db.PluginNotification.Query().CountX(f.setup))
}

func TestAPIKeySuspensionNotificationReplayAndDeliveryFailure(t *testing.T) {
	f := newSuspensionFixture(t)
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sends.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	system := newTestSystemServiceWithWebhookConfig(t, f.db, WebhookNotifierConfig{Targets: []WebhookTarget{{Name: "ops", Enabled: true, URL: server.URL, Body: "{{.Data | json}}"}}})
	f.db.Plugin.UpdateOneID(f.installed.ID).SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions, "webhook"}).ExecX(f.setup)
	input := f.input("notify-once")
	forgedID := 99999
	input.Notification = &PluginNotificationInput{TargetName: "ops", Payload: map[string]any{"message": "paused"}, IdempotencyKey: "caller-key", SuspensionID: &forgedID, Evidence: map[string]any{"forged": true}}
	hold, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	require.Zero(t, sends.Load(), "Apply must never send HTTP inside its transaction")
	notification := f.db.PluginNotification.Query().OnlyX(f.setup)
	require.NotNil(t, notification.SuspensionID)
	require.Equal(t, hold.ID, *notification.SuspensionID)
	require.Equal(t, hold.Evidence, notification.Evidence)
	require.Equal(t, "api-key-suspension:"+fmt.Sprint(hold.ID), notification.IdempotencyKey)
	count, err := DeliverPluginNotifications(f.host, f.db, NewWebhookNotifier(system, httpclient.NewHttpClient()), 10)
	require.NoError(t, err)
	require.Zero(t, count)
	require.EqualValues(t, 1, sends.Load())
	var suspended *APIKeySuspendedError
	require.ErrorAs(t, f.svc.Check(f.host, f.key.ID, f.key.ProjectID), &suspended, "delivery failure cannot undo a committed pause")
	f.db.Plugin.UpdateOneID(f.installed.ID).SetGrantedPermissions([]string{objects.PluginPermissionKeySuspensions}).ExecX(f.setup)
	input.Notification.TargetName = "now-missing"
	replayed, err := f.svc.Apply(f.host, f.installed.ID, input)
	require.NoError(t, err)
	require.Equal(t, hold.ID, replayed.ID)
	require.Equal(t, 1, f.db.PluginNotification.Query().CountX(f.setup), "replay must not enqueue after target or grant revocation")
	encoded, err := json.Marshal(hold)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"installationId"`)
	require.Contains(t, string(encoded), `"apiKeyId"`)
	require.NotContains(t, string(encoded), "idempotency_key_hash")
}

// The SQLite engine still evaluates each statement's clock. Only the database
// clock expression is shifted; the application's wall clock remains unchanged.
type suspensionClockShiftDriver struct {
	dialect.Driver
	offsetSeconds int64
}

func (d *suspensionClockShiftDriver) shifted(query string) string {
	return strings.ReplaceAll(query, "julianday('now')", fmt.Sprintf("julianday('now', '%+d seconds')", d.offsetSeconds))
}

func (d *suspensionClockShiftDriver) Query(ctx context.Context, query string, args, value any) error {
	return d.Driver.Query(ctx, d.shifted(query), args, value)
}

func (d *suspensionClockShiftDriver) Tx(ctx context.Context) (dialect.Tx, error) {
	tx, err := d.Driver.Tx(ctx)
	if err != nil {
		return nil, err
	}
	return &suspensionClockShiftTx{Tx: tx, clock: d}, nil
}

type suspensionClockShiftTx struct {
	dialect.Tx
	clock *suspensionClockShiftDriver
}

func (tx *suspensionClockShiftTx) Query(ctx context.Context, query string, args, value any) error {
	return tx.Tx.Query(ctx, tx.clock.shifted(query), args, value)
}

func TestAPIKeySuspensionDatabaseClockControlsAuditExpiryAndRetry(t *testing.T) {
	f := newSuspensionFixture(t)
	clock := &suspensionClockShiftDriver{Driver: f.db.Driver(), offsetSeconds: 48 * 60 * 60}
	client := ent.NewClient(ent.Driver(clock))
	// The proxy shares the fixture's driver; only the fixture owns its close.
	host := ent.NewContext(f.host, client)
	owner := ent.NewContext(f.owner, client)
	svc := NewAPIKeySuspensionService(f.db)
	before, err := databaseUTCNow(host, client)
	require.NoError(t, err)
	hold, err := svc.Apply(host, f.installed.ID, f.input("database-clock"))
	require.NoError(t, err)
	after, err := databaseUTCNow(host, client)
	require.NoError(t, err)
	require.False(t, hold.CreatedAt.Before(before))
	require.False(t, hold.CreatedAt.After(after))
	require.True(t, hold.CreatedAt.Equal(hold.UpdatedAt))
	require.Equal(t, time.Minute, hold.ExpiresAt.Sub(hold.CreatedAt))
	require.Greater(t, hold.CreatedAt.Sub(time.Now().UTC()), 47*time.Hour)
	_, offset := hold.CreatedAt.Zone()
	require.Zero(t, offset)
	var suspended *APIKeySuspendedError
	require.ErrorAs(t, svc.Check(host, f.key.ID, f.key.ProjectID), &suspended)
	require.False(t, suspended.observedAt.IsZero())
	for _, appNow := range []time.Time{time.Now().Add(-10 * 24 * time.Hour), time.Now().Add(10 * 24 * time.Hour)} {
		require.InDelta(t, 60, suspended.RetryAfter(appNow), 1, "Retry-After must ignore instance clock skew")
	}
	// Advance only the SQL clock past the TTL. No sleep, app clock change,
	// expiry mutation, or timer can explain the transition to inactive.
	clock.offsetSeconds += 120
	require.NoError(t, svc.Check(host, f.key.ID, f.key.ProjectID))
	filter := objects.APIKeySuspensionFilter{ProjectID: f.key.ProjectID, ActiveOnly: true}
	listed, err := svc.List(host, f.installed.ID, filter)
	require.NoError(t, err)
	require.Empty(t, listed)
	listed, err = svc.AdminList(owner, filter)
	require.NoError(t, err)
	require.Empty(t, listed)
	listed, err = f.svc.List(f.host, f.installed.ID, filter)
	require.NoError(t, err)
	require.Len(t, listed, 1, "unshifted database clock still sees a future expiry")
	beforeRelease, err := databaseUTCNow(host, client)
	require.NoError(t, err)
	released, err := svc.ReleaseOwn(host, f.installed.ID, hold.ID)
	require.NoError(t, err)
	afterRelease, err := databaseUTCNow(host, client)
	require.NoError(t, err)
	require.NotNil(t, released.ReleasedAt)
	require.False(t, released.ReleasedAt.Before(beforeRelease))
	require.False(t, released.ReleasedAt.After(afterRelease))
	require.True(t, released.UpdatedAt.Equal(*released.ReleasedAt))
	_, err = databaseUTCNow(f.owner, f.db)
	require.Error(t, err, "raw database clock is a host-only operation")
}

func TestAPIKeySuspensionClockSQLAcrossDialects(t *testing.T) {
	for _, tc := range []struct{ dialect, clock string }{
		{dialect.Postgres, "clock_timestamp()"}, {dialect.MySQL, "UTC_TIMESTAMP(6)"}, {dialect.SQLite, "julianday('now')"},
	} {
		t.Run(tc.dialect, func(t *testing.T) {
			s := sql.Dialect(tc.dialect).Select(apikeysuspension.FieldID).From(sql.Table(apikeysuspension.Table))
			activeSuspensions()(s)
			query, args := s.Query()
			require.Contains(t, query, tc.clock)
			require.Empty(t, args, "expiry comparison cannot bind an application-clock timestamp")
			require.Contains(t, databaseUTCEpochSQL(tc.dialect), tc.clock)
			if tc.dialect == dialect.MySQL {
				require.Contains(t, databaseUTCEpochSQL(tc.dialect), "TIMESTAMPDIFF")
				require.NotContains(t, databaseUTCEpochSQL(tc.dialect), "UNIX_TIMESTAMP", "UTC must not be reinterpreted in the connection timezone")
			}
			if tc.dialect == dialect.SQLite {
				require.Contains(t, query, "COALESCE")
				require.Contains(t, query, "instr(substr(")
			}
		})
	}
	for _, epoch := range []float64{math.NaN(), math.Inf(1), -1} {
		_, err := databaseUTCFromEpoch(epoch)
		require.Error(t, err)
	}
}
