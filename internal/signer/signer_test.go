package signer

import (
	"testing"
	"time"
)

func TestSignKnownVector(t *testing.T) {
	// Documented example vector (Svix "verifying webhooks manually" docs).
	secret := "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	msgID := "msg_p5jXN8AQM9LWM0D4loKWxJek"
	ts := time.Unix(1614265330, 0)
	payload := []byte(`{"test": 2432232314}`)

	sig, err := Sign(secret, msgID, ts, payload)
	if err != nil {
		t.Fatal(err)
	}
	want := "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
	if sig != want {
		t.Fatalf("got %s want %s", sig, want)
	}
}

func TestSignRejectsBadSecret(t *testing.T) {
	if _, err := Sign("not-a-whsec", "id", time.Now(), nil); err == nil {
		t.Fatal("expected error for malformed secret")
	}
}

func TestHeaders(t *testing.T) {
	h := Headers("msg_1", time.Unix(1700000000, 0), "v1,abc")
	if h["webhook-id"] != "msg_1" || h["webhook-timestamp"] != "1700000000" || h["webhook-signature"] != "v1,abc" {
		t.Fatalf("unexpected headers: %v", h)
	}
}
