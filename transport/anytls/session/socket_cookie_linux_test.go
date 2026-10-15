//go:build linux

package session

import (
	utls "github.com/metacubex/utls"
	"net"
	"testing"
)

func TestStageEvidenceSocketCookieTLSAndUnavailable(t *testing.T) {
	_, client, _ := realTLSPair(t, utls.VersionTLS13, true)
	a, b := SocketCookie(client), SocketCookie(client.NetConn())
	if a == nil || b == nil || *a != *b {
		t.Fatal("TLS socket cookie mismatch")
	}
	p, q := net.Pipe()
	defer p.Close()
	defer q.Close()
	if SocketCookie(p) != nil {
		t.Fatal("unsupported transport must be null")
	}
	client.Close()
	if SocketCookie(client) != nil {
		t.Fatal("closed socket must be null")
	}
	t.Log("real verified TLS wrapper cookie matches TCP; unsupported and closed sockets are null; private cookie suppressed")
}
