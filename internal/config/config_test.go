package config

import "testing"

func TestFromEnvDefaultsAndOverrides(t *testing.T) {
	env := map[string]string{
		"RELAY_DATABASE_URL":  "postgres://x",
		"RELAY_REDIS_URL":     "redis://y",
		"RELAY_ADMIN_API_KEY": "k",
	}
	cfg, err := FromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPTimeoutMS != 10000 || cfg.MaxPayloadBytes != 65536 || cfg.SweepIntervalMS != 1000 {
		t.Fatalf("defaults: %+v", cfg)
	}
	if cfg.AllowPrivateDestinations {
		t.Fatal("private destinations must default to blocked")
	}

	env["RELAY_ALLOW_PRIVATE_DESTINATIONS"] = "true"
	env["RELAY_HTTP_TIMEOUT_MS"] = "2500"
	cfg, _ = FromEnv(func(k string) string { return env[k] })
	if !cfg.AllowPrivateDestinations || cfg.HTTPTimeoutMS != 2500 {
		t.Fatalf("overrides: %+v", cfg)
	}
}

func TestFromEnvRequiresCoreVars(t *testing.T) {
	if _, err := FromEnv(func(string) string { return "" }); err == nil {
		t.Fatal("expected error when required vars are missing")
	}
}
