package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/augustoamaro/webhook-relay/internal/queue"
	"github.com/augustoamaro/webhook-relay/internal/store"
	"github.com/redis/go-redis/v9"
)

const adminKey = "admin-test-key"

func testServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	dbURL, rURL := os.Getenv("RELAY_TEST_DATABASE_URL"), os.Getenv("RELAY_TEST_REDIS_URL")
	if dbURL == "" || rURL == "" {
		t.Skip("RELAY_TEST_DATABASE_URL / RELAY_TEST_REDIS_URL not set")
	}
	ctx := context.Background()
	s, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	_ = s.Migrate(ctx)
	_ = s.Truncate(ctx)
	opt, _ := redis.ParseURL(rURL)
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	_ = rdb.FlushAll(ctx).Err()
	q, _ := queue.New(ctx, rdb)
	srv := httptest.NewServer(NewServer(s, q, adminKey, 65536).Handler())
	t.Cleanup(srv.Close)
	return srv, s
}

func doJSON(t *testing.T, method, url, bearer string, body any) (*http.Response, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, url, &buf)
	req.Header.Set("content-type", "application/json")
	if bearer != "" {
		req.Header.Set("authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	return resp, out
}

func TestAdminAuthRequired(t *testing.T) {
	srv, _ := testServer(t)
	resp, _ := doJSON(t, "POST", srv.URL+"/api/v1/applications", "", map[string]any{"name": "x"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestCreateAppEndpointAndIngest(t *testing.T) {
	srv, _ := testServer(t)
	resp, app := doJSON(t, "POST", srv.URL+"/api/v1/applications", adminKey, map[string]any{"name": "acme"})
	if resp.StatusCode != 201 || app["api_key"] == "" {
		t.Fatalf("create app: %d %v", resp.StatusCode, app)
	}
	appID, appKey := app["id"].(string), app["api_key"].(string)

	resp, ep := doJSON(t, "POST", srv.URL+"/api/v1/applications/"+appID+"/endpoints", adminKey,
		map[string]any{"url": "https://example.com/hook", "event_types": []string{}})
	if resp.StatusCode != 201 || ep["secret"] == "" {
		t.Fatalf("create endpoint: %d %v", resp.StatusCode, ep)
	}

	resp, msg := doJSON(t, "POST", srv.URL+"/api/v1/applications/"+appID+"/messages", appKey,
		map[string]any{"event_type": "order.created", "payload": map[string]any{"n": 1}})
	if resp.StatusCode != 202 || msg["id"] == "" {
		t.Fatalf("ingest: %d %v", resp.StatusCode, msg)
	}

	// Wrong key on ingest → 401
	resp, _ = doJSON(t, "POST", srv.URL+"/api/v1/applications/"+appID+"/messages", "whr_bogus",
		map[string]any{"event_type": "x", "payload": map[string]any{}})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bogus key status: %d", resp.StatusCode)
	}
}

func TestIngestValidation(t *testing.T) {
	srv, _ := testServer(t)
	_, app := doJSON(t, "POST", srv.URL+"/api/v1/applications", adminKey, map[string]any{"name": "acme"})
	appID, appKey := app["id"].(string), app["api_key"].(string)
	resp, _ := doJSON(t, "POST", srv.URL+"/api/v1/applications/"+appID+"/messages", appKey,
		map[string]any{"payload": map[string]any{}}) // missing event_type
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}
