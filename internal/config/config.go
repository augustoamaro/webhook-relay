// Package config loads and validates the relay's runtime configuration.
package config

import (
	"fmt"
	"strconv"
)

// Config holds the runtime configuration for the relay service.
type Config struct {
	DatabaseURL              string
	RedisURL                 string
	AdminAPIKey              string
	AllowPrivateDestinations bool
	HTTPTimeoutMS            int
	MaxPayloadBytes          int64
	SweepIntervalMS          int
	ListenAddr               string
}

// FromEnv builds config from a lookup func (os.Getenv in production).
func FromEnv(get func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:              get("RELAY_DATABASE_URL"),
		RedisURL:                 get("RELAY_REDIS_URL"),
		AdminAPIKey:              get("RELAY_ADMIN_API_KEY"),
		AllowPrivateDestinations: get("RELAY_ALLOW_PRIVATE_DESTINATIONS") == "true",
		HTTPTimeoutMS:            intOr(get("RELAY_HTTP_TIMEOUT_MS"), 10000),
		MaxPayloadBytes:          int64(intOr(get("RELAY_MAX_PAYLOAD_BYTES"), 65536)),
		SweepIntervalMS:          intOr(get("RELAY_SWEEP_INTERVAL_MS"), 1000),
		ListenAddr:               or(get("RELAY_LISTEN_ADDR"), ":8080"),
	}
	if cfg.DatabaseURL == "" || cfg.RedisURL == "" || cfg.AdminAPIKey == "" {
		return Config{}, fmt.Errorf("RELAY_DATABASE_URL, RELAY_REDIS_URL and RELAY_ADMIN_API_KEY are required")
	}
	return cfg, nil
}

func intOr(s string, def int) int {
	if s == "" {
		return def
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
