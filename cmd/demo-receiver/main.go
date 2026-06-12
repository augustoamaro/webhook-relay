// demo-receiver is a deliberately flaky webhook receiver for demos and load
// tests: configurable failure rate and latency, Standard Webhooks verification.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	port := envOr("PORT", "9090")
	failRate, _ := strconv.ParseFloat(envOr("FAIL_RATE", "0.2"), 64)
	maxDelayMS, _ := strconv.Atoi(envOr("MAX_DELAY_MS", "200"))
	secret := os.Getenv("WEBHOOK_SECRET") // optional: verify signatures

	var received, failed atomic.Int64
	http.HandleFunc("POST /hook", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if maxDelayMS > 0 {
			time.Sleep(time.Duration(rand.IntN(maxDelayMS)) * time.Millisecond)
		}
		if secret != "" && !verify(secret, r.Header, body) {
			slog.Error("bad signature", "id", r.Header.Get("webhook-id"))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if rand.Float64() < failRate {
			failed.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		received.Add(1)
		slog.Info("received", "id", r.Header.Get("webhook-id"))
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"received":%d,"failed":%d}`, received.Load(), failed.Load())
	})
	slog.Info("demo-receiver listening", "port", port, "fail_rate", failRate)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func verify(secret string, h http.Header, body []byte) bool {
	raw, ok := strings.CutPrefix(secret, "whsec_")
	if !ok {
		return false
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, key)
	fmt.Fprintf(mac, "%s.%s.", h.Get("webhook-id"), h.Get("webhook-timestamp"))
	mac.Write(body)
	want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	for _, sig := range strings.Split(h.Get("webhook-signature"), " ") {
		if hmac.Equal([]byte(sig), []byte(want)) {
			return true
		}
	}
	return false
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
