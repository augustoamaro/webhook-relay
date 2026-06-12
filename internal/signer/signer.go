// Package signer implements the Standard Webhooks signature scheme
// (https://www.standardwebhooks.com): HMAC-SHA256 over "{id}.{timestamp}.{payload}".
package signer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const secretPrefix = "whsec_"

// Sign returns the versioned signature header value, e.g. "v1,<base64>".
func Sign(secret, msgID string, ts time.Time, payload []byte) (string, error) {
	raw, ok := strings.CutPrefix(secret, secretPrefix)
	if !ok {
		return "", fmt.Errorf("secret must start with %q", secretPrefix)
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", fmt.Errorf("decode secret: %w", err)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = fmt.Fprintf(mac, "%s.%d.", msgID, ts.Unix())
	mac.Write(payload)
	return "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

// Headers returns the three Standard Webhooks headers for a delivery.
func Headers(msgID string, ts time.Time, signature string) map[string]string {
	return map[string]string{
		"webhook-id":        msgID,
		"webhook-timestamp": strconv.FormatInt(ts.Unix(), 10),
		"webhook-signature": signature,
	}
}

// NewSecret generates a fresh endpoint secret ("whsec_" + base64 of 24 random bytes).
// Defined here so secret format knowledge stays in one package.
func NewSecret(random func([]byte) (int, error)) (string, error) {
	buf := make([]byte, 24)
	if _, err := random(buf); err != nil {
		return "", err
	}
	return secretPrefix + base64.StdEncoding.EncodeToString(buf), nil
}
