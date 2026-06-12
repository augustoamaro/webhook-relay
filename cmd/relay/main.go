// relay runs the webhook delivery service: api, worker, sweep, or all-in-one.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/augustoamaro/webhook-relay/internal/api"
	"github.com/augustoamaro/webhook-relay/internal/config"
	"github.com/augustoamaro/webhook-relay/internal/dispatch"
	"github.com/augustoamaro/webhook-relay/internal/domain"
	"github.com/augustoamaro/webhook-relay/internal/netguard"
	"github.com/augustoamaro/webhook-relay/internal/obs"
	"github.com/augustoamaro/webhook-relay/internal/queue"
	"github.com/augustoamaro/webhook-relay/internal/store"
	"github.com/augustoamaro/webhook-relay/internal/sweep"
	"github.com/redis/go-redis/v9"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	mode := "all"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if err := run(mode); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(mode string) error {
	cfg, err := config.FromEnv(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownObs, metrics, err := obs.Setup(ctx, "webhook-relay-"+mode)
	if err != nil {
		return err
	}
	defer func() { _ = shutdownObs(context.Background()) }()

	s, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		return err
	}

	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(opt)
	defer func() { _ = rdb.Close() }()
	q, err := queue.New(ctx, rdb)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	startWorker := func(name string) {
		wg.Add(2)
		w := dispatch.New(s, netguard.NewClient(time.Duration(cfg.HTTPTimeoutMS)*time.Millisecond, cfg.AllowPrivateDestinations), metrics)
		go func() {
			defer wg.Done()
			q.Consume(ctx, name, func(ctx context.Context, it queue.Item) { w.Handle(ctx, it.DeliveryID) })
		}()
		go func() {
			defer wg.Done()
			q.Reclaim(ctx, name+"-reclaim", domain.LeaseSeconds*time.Second, 15*time.Second,
				func(ctx context.Context, it queue.Item) { w.Handle(ctx, it.DeliveryID) })
		}()
	}
	startSweep := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sweep.Run(ctx, s, q, time.Duration(cfg.SweepIntervalMS)*time.Millisecond, 500)
		}()
	}
	startAPI := func() error {
		srv := &http.Server{
			Addr:              cfg.ListenAddr,
			Handler:           obs.HTTPMiddleware(api.NewServer(s, q, cfg.AdminAPIKey, cfg.MaxPayloadBytes).Handler()),
			ReadHeaderTimeout: 5 * time.Second,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutCtx)
		}()
		slog.Info("api listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}

	hostname, _ := os.Hostname()
	switch mode {
	case "api":
		if err := startAPI(); err != nil {
			return err
		}
	case "worker":
		startWorker("worker-" + hostname)
	case "sweep":
		startSweep()
	case "all":
		startWorker("worker-" + hostname)
		startSweep()
		if err := startAPI(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown mode %q (want api|worker|sweep|all)", mode)
	}
	wg.Wait()
	return nil
}
