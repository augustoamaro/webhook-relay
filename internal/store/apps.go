package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"

	"github.com/augustoamaro/webhook-relay/internal/domain"
	"github.com/augustoamaro/webhook-relay/internal/signer"
)

func newAPIKey() (key string, hash []byte, err error) {
	buf := make([]byte, 32)
	if _, err = rand.Read(buf); err != nil {
		return "", nil, err
	}
	key = "whr_" + base64.RawURLEncoding.EncodeToString(buf)
	sum := sha256.Sum256([]byte(key))
	return key, sum[:], nil
}

// CreateApplication inserts a new application and returns the app and its API
// key — shown exactly once. The key is stored only as a SHA-256 hash.
func (s *Store) CreateApplication(ctx context.Context, name string) (domain.Application, string, error) {
	key, hash, err := newAPIKey()
	if err != nil {
		return domain.Application{}, "", err
	}
	app := domain.Application{ID: domain.NewID("app"), Name: name}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO applications (id, name, api_key_hash) VALUES ($1, $2, $3) RETURNING created_at`,
		app.ID, app.Name, hash).Scan(&app.CreatedAt)
	return app, key, err
}

// ApplicationByKey looks up an application by presenting the raw API key.
// The key is hashed on the fly; no plaintext is stored.
func (s *Store) ApplicationByKey(ctx context.Context, key string) (domain.Application, error) {
	sum := sha256.Sum256([]byte(key))
	var app domain.Application
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, created_at FROM applications WHERE api_key_hash = $1`,
		sum[:]).Scan(&app.ID, &app.Name, &app.CreatedAt)
	if err != nil {
		return domain.Application{}, fmt.Errorf("unknown api key: %w", err)
	}
	return app, nil
}

// CreateEndpoint adds a new endpoint under the given application. A fresh
// Standard Webhooks secret is generated and stored in plaintext (it must be
// readable to sign deliveries).
func (s *Store) CreateEndpoint(ctx context.Context, appID, url string, eventTypes []string) (domain.Endpoint, error) {
	secret, err := signer.NewSecret(rand.Read)
	if err != nil {
		return domain.Endpoint{}, err
	}
	if eventTypes == nil {
		eventTypes = []string{}
	}
	ep := domain.Endpoint{
		ID:            domain.NewID("ep"),
		ApplicationID: appID,
		URL:           url,
		Secret:        secret,
		EventTypes:    eventTypes,
		Status:        "enabled",
	}
	err = s.pool.QueryRow(ctx,
		`INSERT INTO endpoints (id, application_id, url, secret, event_types)
		 VALUES ($1, $2, $3, $4, $5) RETURNING created_at`,
		ep.ID, ep.ApplicationID, ep.URL, ep.Secret, ep.EventTypes).Scan(&ep.CreatedAt)
	return ep, err
}

// SetEndpointStatus updates the status of an endpoint and resets the
// consecutive_failures counter (re-enabling clears the circuit-breaker count).
func (s *Store) SetEndpointStatus(ctx context.Context, endpointID, status string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE endpoints SET status = $2, consecutive_failures = 0 WHERE id = $1`,
		endpointID, status)
	if err == nil && tag.RowsAffected() == 0 {
		return fmt.Errorf("endpoint %s not found", endpointID)
	}
	return err
}

// ListEndpoints returns all endpoints for the given application, ordered by
// creation time.
func (s *Store) ListEndpoints(ctx context.Context, appID string) ([]domain.Endpoint, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, application_id, url, secret, event_types, status, consecutive_failures, created_at
		 FROM endpoints WHERE application_id = $1 ORDER BY created_at`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Endpoint
	for rows.Next() {
		var ep domain.Endpoint
		if err := rows.Scan(&ep.ID, &ep.ApplicationID, &ep.URL, &ep.Secret,
			&ep.EventTypes, &ep.Status, &ep.ConsecutiveFailures, &ep.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, ep)
	}
	return out, rows.Err()
}
