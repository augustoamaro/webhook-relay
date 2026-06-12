package store

import (
	"context"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/domain"
)

// ClaimedDelivery carries everything a worker needs for one attempt.
type ClaimedDelivery struct {
	ID           string
	AttemptCount int
	Traceparent  string
	MessageID    string
	EventType    string
	Payload      []byte
	MsgCreatedAt time.Time
	EndpointID   string
	URL          string
	Secret       string
}

// ClaimDelivery atomically claims a due delivery. nil, nil = lost the race /
// not due / already terminal. The lease (LeaseSeconds) makes claims from
// crashed workers expire.
func (s *Store) ClaimDelivery(ctx context.Context, id string) (*ClaimedDelivery, error) {
	row := s.pool.QueryRow(ctx, `
		WITH c AS (
		  UPDATE deliveries SET status = 'delivering', claimed_at = now()
		  WHERE id = $1
		    AND ((status IN ('pending','failed') AND next_attempt_at <= now())
		      OR (status = 'delivering' AND claimed_at < now() - make_interval(secs => $2)))
		  RETURNING id, message_id, endpoint_id, attempt_count, coalesce(traceparent, '') AS tp
		)
		SELECT c.id, c.attempt_count, c.tp, m.id, m.event_type, m.payload, m.created_at,
		       e.id, e.url, e.secret
		FROM c
		JOIN messages m ON m.id = c.message_id
		JOIN endpoints e ON e.id = c.endpoint_id`,
		id, domain.LeaseSeconds)
	var cd ClaimedDelivery
	err := row.Scan(&cd.ID, &cd.AttemptCount, &cd.Traceparent,
		&cd.MessageID, &cd.EventType, &cd.Payload, &cd.MsgCreatedAt,
		&cd.EndpointID, &cd.URL, &cd.Secret)
	if isNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cd, nil
}

// FinishSuccess marks a delivery succeeded and records the terminal status code.
func (s *Store) FinishSuccess(ctx context.Context, id string, statusCode int) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'succeeded', succeeded_at = now(), attempt_count = attempt_count + 1,
		    claimed_at = NULL, last_status_code = $2, last_error = NULL
		WHERE id = $1 AND status = 'delivering'`, id, statusCode)
	return err
}

// FinishFailure records a failed attempt: either schedules the retry after
// `delay`, or (dead=true) sends the delivery to the DLQ.
func (s *Store) FinishFailure(ctx context.Context, id string, delay time.Duration, dead bool, statusCode int, errMsg, deadReason string) error {
	status := domain.StatusFailed
	if dead {
		status = domain.StatusDead
	}
	var code any
	if statusCode > 0 {
		code = statusCode
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE deliveries
		SET status = $2, attempt_count = attempt_count + 1,
		    next_attempt_at = now() + make_interval(secs => $3),
		    claimed_at = NULL, last_status_code = $4, last_error = $5,
		    dead_reason = NULLIF($6, '')
		WHERE id = $1 AND status = 'delivering'`,
		id, string(status), delay.Seconds(), code, errMsg, deadReason)
	return err
}

// DueDelivery is a minimal row returned by DueDeliveries for the sweeper.
type DueDelivery struct {
	ID          string
	Traceparent string
}

// DueDeliveries is the sweeper feed: due (or lease-expired) deliveries not
// enqueued in the last 30s. SKIP LOCKED keeps concurrent sweepers safe;
// the 30s throttle keeps a backlog from being re-enqueued every tick
// (claims absorb any duplicates that do slip through).
func (s *Store) DueDeliveries(ctx context.Context, limit int) ([]DueDelivery, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE deliveries SET last_enqueued_at = now()
		WHERE id IN (
		  SELECT id FROM deliveries
		  WHERE ((status IN ('pending','failed') AND next_attempt_at <= now())
		     OR (status = 'delivering' AND claimed_at < now() - make_interval(secs => $1)))
		    AND (last_enqueued_at IS NULL OR last_enqueued_at < now() - interval '30 seconds')
		  ORDER BY next_attempt_at
		  LIMIT $2
		  FOR UPDATE SKIP LOCKED
		)
		RETURNING id, coalesce(traceparent, '')`,
		domain.LeaseSeconds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DueDelivery
	for rows.Next() {
		var d DueDelivery
		if err := rows.Scan(&d.ID, &d.Traceparent); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// RecordAttempt appends to the per-attempt ledger.
func (s *Store) RecordAttempt(ctx context.Context, deliveryID string, duration time.Duration, statusCode int, errMsg, snippet string) error {
	var code any
	if statusCode > 0 {
		code = statusCode
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO delivery_attempts (id, delivery_id, duration_ms, status_code, error, response_snippet)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''))`,
		domain.NewID("att"), deliveryID, duration.Milliseconds(), code, errMsg, snippet)
	return err
}
