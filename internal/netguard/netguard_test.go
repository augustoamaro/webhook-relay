package netguard

import "testing"

func TestCheckAddr(t *testing.T) {
	bad := []string{
		"127.0.0.1:443", "10.1.2.3:80", "192.168.1.1:443", "172.16.0.9:8080",
		"169.254.169.254:80", "[::1]:443", "0.0.0.0:80",
	}
	for _, a := range bad {
		if err := checkAddr(a); err == nil {
			t.Errorf("%s: expected rejection", a)
		}
	}
	good := []string{"93.184.216.34:443", "[2606:2800:220:1:248:1893:25c8:1946]:443"}
	for _, a := range good {
		if err := checkAddr(a); err != nil {
			t.Errorf("%s: unexpected rejection: %v", a, err)
		}
	}
}
