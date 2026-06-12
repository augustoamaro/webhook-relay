// Package store owns all SQL. Postgres is the system's source of truth.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store is the data-access layer; all SQL lives here, backed by a pgxpool.
type Store struct {
	pool *pgxpool.Pool
}

// Open dials Postgres, pings the pool, and returns a ready Store.
func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close drains and closes the connection pool.
func (s *Store) Close() { s.pool.Close() }

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// migrationLockKey is an arbitrary fixed key for the advisory lock that
// serializes concurrent Migrate() callers (api/worker/sweeper all migrate on boot).
const migrationLockKey int64 = 0x7762_7265_6C61_79 // "wbrelay"

// Migrate runs all pending goose migrations under a pg_advisory_lock so that
// concurrent replicas starting together don't race.
func (s *Store) Migrate(ctx context.Context) error {
	// Serialize concurrent migrators (multiple replicas start together).
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey) }()

	goose.SetBaseFS(migrationsFS)
	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}
	db := stdlib.OpenDBFromPool(s.pool)
	defer func() { _ = db.Close() }()
	return goose.UpContext(ctx, db, "migrations")
}

// Truncate empties all tables. Test helper for packages outside store.
func (s *Store) Truncate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx,
		`TRUNCATE applications, endpoints, messages, deliveries, delivery_attempts CASCADE`)
	return err
}

// DeliveryState returns status and attempt_count (used by tests and reconcile).
func (s *Store) DeliveryState(ctx context.Context, id string) (string, int, error) {
	var status string
	var attempts int
	err := s.pool.QueryRow(ctx,
		`SELECT status, attempt_count FROM deliveries WHERE id = $1`, id).Scan(&status, &attempts)
	return status, attempts, err
}
