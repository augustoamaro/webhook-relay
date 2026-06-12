package store

import (
	"context"
	"encoding/json"
	"time"
)

// IncEndpointFailures increments consecutive_failures for an endpoint and returns the new count.
func (s *Store) IncEndpointFailures(ctx context.Context, endpointID string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`UPDATE endpoints SET consecutive_failures = consecutive_failures + 1
		 WHERE id = $1 RETURNING consecutive_failures`, endpointID).Scan(&n)
	return n, err
}

// ResetEndpointFailures clears the consecutive failure counter after a successful delivery.
func (s *Store) ResetEndpointFailures(ctx context.Context, endpointID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE endpoints SET consecutive_failures = 0 WHERE id = $1`, endpointID)
	return err
}

// TripBreaker disables the endpoint and dead-letters its in-flight deliveries.
func (s *Store) TripBreaker(ctx context.Context, endpointID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx,
		`UPDATE endpoints SET status = 'disabled' WHERE id = $1`, endpointID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE deliveries SET status = 'dead', dead_reason = 'endpoint_disabled', claimed_at = NULL
		 WHERE endpoint_id = $1 AND status IN ('pending','failed','delivering')`, endpointID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DeliverySummary is a row returned by ListDeliveries for the API response.
type DeliverySummary struct {
	ID             string
	MessageID      string
	EndpointID     string
	Status         string
	AttemptCount   int
	NextAttemptAt  time.Time
	LastStatusCode int
	LastError      string
	DeadReason     string
	CreatedAt      time.Time
}

// ListDeliveries pages an application's deliveries by status (newest first).
// Returns the page and the total count for that status.
func (s *Store) ListDeliveries(ctx context.Context, appID, status string, limit int) ([]DeliverySummary, int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.message_id, d.endpoint_id, d.status, d.attempt_count, d.next_attempt_at,
		       coalesce(d.last_status_code, 0), coalesce(d.last_error, ''), coalesce(d.dead_reason, ''),
		       d.created_at, count(*) OVER ()
		FROM deliveries d JOIN messages m ON m.id = d.message_id
		WHERE m.application_id = $1 AND d.status = $2
		ORDER BY d.created_at DESC
		LIMIT $3`, appID, status, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []DeliverySummary
	var total int
	for rows.Next() {
		var d DeliverySummary
		if err := rows.Scan(&d.ID, &d.MessageID, &d.EndpointID, &d.Status, &d.AttemptCount,
			&d.NextAttemptAt, &d.LastStatusCode, &d.LastError, &d.DeadReason, &d.CreatedAt, &total); err != nil {
			return nil, 0, err
		}
		out = append(out, d)
	}
	return out, total, rows.Err()
}

// Redrive resets matching dead deliveries to pending with a fresh attempt
// budget (the attempts ledger is preserved). Filters combine with AND;
// zero values mean "no filter".
func (s *Store) Redrive(ctx context.Context, appID string, deliveryIDs []string, endpointID string, since, until time.Time) (int, error) {
	if deliveryIDs == nil {
		deliveryIDs = []string{}
	}
	var sinceArg, untilArg any
	if !since.IsZero() {
		sinceArg = since
	}
	if !until.IsZero() {
		untilArg = until
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE deliveries d
		SET status = 'pending', next_attempt_at = now(), attempt_count = 0,
		    dead_reason = NULL, last_enqueued_at = NULL, claimed_at = NULL
		FROM messages m
		WHERE m.id = d.message_id AND m.application_id = $1 AND d.status = 'dead'
		  AND (cardinality($2::text[]) = 0 OR d.id = ANY($2))
		  AND ($3 = '' OR d.endpoint_id = $3)
		  AND ($4::timestamptz IS NULL OR d.created_at >= $4)
		  AND ($5::timestamptz IS NULL OR d.created_at <= $5)`,
		appID, deliveryIDs, endpointID, sinceArg, untilArg)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

// MessageStatus returns a message with its deliveries (for GET /messages/{id}).
func (s *Store) MessageStatus(ctx context.Context, appID, messageID string) (map[string]any, error) {
	var eventType string
	var payload []byte
	var createdAt time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT event_type, payload, created_at FROM messages WHERE id = $1 AND application_id = $2`,
		messageID, appID).Scan(&eventType, &payload, &createdAt)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, endpoint_id, status, attempt_count, next_attempt_at,
		       coalesce(last_status_code, 0), coalesce(last_error, ''), coalesce(dead_reason, '')
		FROM deliveries WHERE message_id = $1 ORDER BY created_at`, messageID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deliveries := []map[string]any{}
	for rows.Next() {
		var id, epID, status, lastErr, deadReason string
		var attempts, code int
		var next time.Time
		if err := rows.Scan(&id, &epID, &status, &attempts, &next, &code, &lastErr, &deadReason); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, map[string]any{
			"id": id, "endpoint_id": epID, "status": status, "attempt_count": attempts,
			"next_attempt_at": next, "last_status_code": code, "last_error": lastErr, "dead_reason": deadReason,
		})
	}
	return map[string]any{
		"id": messageID, "event_type": eventType, "payload": json.RawMessage(payload),
		"created_at": createdAt, "deliveries": deliveries,
	}, rows.Err()
}
