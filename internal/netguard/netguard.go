// Package netguard prevents SSRF: outbound webhook deliveries must not reach
// private, loopback, or link-local destinations. The check runs in the
// dialer's Control hook, i.e. against the IP actually being dialed — immune
// to DNS rebinding between validation and connection.
package netguard

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"
)

func checkAddr(address string) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("parse dial address %q: %w", address, err)
	}
	ip := ap.Addr().Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return fmt.Errorf("destination %s is not a public address", ip)
	}
	return nil
}

// NewClient returns an HTTP client for webhook delivery: bounded timeout,
// no redirect following, and (unless allowPrivate) a dial-time guard that
// rejects non-public destinations.
func NewClient(timeout time.Duration, allowPrivate bool) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if !allowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			return checkAddr(address)
		}
	}
	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     30 * time.Second,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse // never follow redirects
		},
	}
}
