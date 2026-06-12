package store

import (
	"context"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/domain"
)

// IngestResult is returned by IngestMessage; Duplicate is true when the idempotency key already existed.
type IngestResult struct {
	Message     domain.Message
	DeliveryIDs []string
	Duplicate   bool
}

// IngestMessage writes the message and fans out one delivery per matching
// enabled endpoint — one transaction, so the deliveries ARE the outbox.
// A duplicate Idempotency-Key returns the original message and no new work.
func (s *Store) IngestMessage(ctx context.Context, appID, eventType string, payload []byte, idemKey, traceparent string) (IngestResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestResult{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	msg := domain.Message{
		ID: domain.NewID("msg"), ApplicationID: appID,
		EventType: eventType, Payload: payload, IdempotencyKey: idemKey,
	}
	var key any
	if idemKey != "" {
		key = idemKey
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO messages (id, application_id, event_type, payload, idempotency_key)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (application_id, idempotency_key) WHERE idempotency_key IS NOT NULL
		 DO NOTHING
		 RETURNING created_at`,
		msg.ID, appID, eventType, payload, key).Scan(&msg.CreatedAt)
	switch {
	case err == nil:
		// inserted; fall through to fan-out
	case isNoRows(err):
		// Conflict: fetch the original and return it untouched.
		var orig domain.Message
		err = tx.QueryRow(ctx,
			`SELECT id, application_id, event_type, payload, created_at
			 FROM messages WHERE application_id = $1 AND idempotency_key = $2`,
			appID, idemKey).Scan(&orig.ID, &orig.ApplicationID, &orig.EventType, &orig.Payload, &orig.CreatedAt)
		if err != nil {
			return IngestResult{}, err
		}
		orig.IdempotencyKey = idemKey
		return IngestResult{Message: orig, Duplicate: true}, tx.Commit(ctx)
	default:
		return IngestResult{}, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id FROM endpoints
		 WHERE application_id = $1 AND status = 'enabled'
		   AND (event_types = '{}' OR $2 = ANY(event_types))`,
		appID, eventType)
	if err != nil {
		return IngestResult{}, err
	}
	var endpointIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return IngestResult{}, err
		}
		endpointIDs = append(endpointIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return IngestResult{}, err
	}

	deliveryIDs := make([]string, 0, len(endpointIDs))
	for _, epID := range endpointIDs {
		dlvID := domain.NewID("dlv")
		_, err := tx.Exec(ctx,
			`INSERT INTO deliveries (id, message_id, endpoint_id, traceparent, next_attempt_at)
			 VALUES ($1, $2, $3, NULLIF($4, ''), $5)`,
			dlvID, msg.ID, epID, traceparent, time.Now())
		if err != nil {
			return IngestResult{}, err
		}
		deliveryIDs = append(deliveryIDs, dlvID)
	}
	return IngestResult{Message: msg, DeliveryIDs: deliveryIDs}, tx.Commit(ctx)
}
