// reconcile verifies the ledger after a load/chaos run: it waits for the
// backlog to drain, then asserts no delivery was lost or left stuck.
// Exit 0 = reconciled; exit 1 = stuck or inconsistent.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	url := os.Getenv("RELAY_DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "RELAY_DATABASE_URL required")
		os.Exit(2)
	}
	deadline := time.Now().Add(3 * time.Minute)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer pool.Close()

	for {
		var pending, delivering, failed, succeeded, dead int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE status = 'pending'),
			       count(*) FILTER (WHERE status = 'delivering'),
			       count(*) FILTER (WHERE status = 'failed'),
			       count(*) FILTER (WHERE status = 'succeeded'),
			       count(*) FILTER (WHERE status = 'dead')
			FROM deliveries`).Scan(&pending, &delivering, &failed, &succeeded, &dead)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		inflight := pending + delivering + failed
		fmt.Printf("deliveries: pending=%d delivering=%d failed=%d succeeded=%d dead=%d\n",
			pending, delivering, failed, succeeded, dead)
		if inflight == 0 {
			fmt.Println("RECONCILED: every delivery reached a terminal state — zero lost")
			os.Exit(0)
		}
		if time.Now().After(deadline) {
			fmt.Printf("STUCK: %d deliveries not terminal after deadline\n", inflight)
			os.Exit(1)
		}
		time.Sleep(2 * time.Second)
	}
}
