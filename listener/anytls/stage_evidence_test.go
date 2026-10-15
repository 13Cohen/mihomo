package anytls

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	LC "github.com/metacubex/mihomo/listener/config"
	"github.com/metacubex/mihomo/log"
)

func TestStageEvidenceAuthenticatedListenerGate(t *testing.T) {
	cert, key := acceptanceCertificate(t)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(cert)) {
		t.Fatal("certificate")
	}
	sub := log.Subscribe()
	defer log.UnSubscribe(sub)
	makeListener := func(enabled bool) *Listener {
		l, err := New(LC.AnyTLSServer{Listen: "127.0.0.1:0", Users: map[string]string{"fixture": "synthetic-stage-password"}, Certificate: cert, PrivateKey: key, StageEvidence: enabled}, &acceptanceTunnel{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		return l
	}
	off, on := makeListener(false), makeListener(true)
	dial := func(l *Listener, password string) *tls.Conn {
		raw, err := net.DialTimeout("tcp", l.AddrList()[0].String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conn := tls.Client(raw, &tls.Config{RootCAs: roots, ServerName: "anytls-fixture.test", MinVersion: tls.VersionTLS13})
		if err = conn.Handshake(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		hash := sha256.Sum256([]byte(password))
		settings := []byte("v=2")
		frame := make([]byte, 7+len(settings))
		frame[0] = 4
		binary.BigEndian.PutUint16(frame[5:], uint16(len(settings)))
		copy(frame[7:], settings)
		auth := append(append(hash[:], 0, 0), frame...)
		if _, err = conn.Write(auth); err != nil {
			t.Fatal(err)
		}
		return conn
	}
	defaultConn := dial(off, "synthetic-stage-password")
	go io.Copy(io.Discard, defaultConn)
	bad := dial(on, "wrong-synthetic-password")
	bad.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := bad.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatal("wrong credential not rejected")
	}
	bad.Close()
	good := dial(on, "synthetic-stage-password")
	go io.Copy(io.Discard, good)
	deadline := time.After(12 * time.Second)
	for {
		select {
		case event := <-sub:
			const prefix = "[AnyTLS EVIDENCE] "
			if !strings.HasPrefix(event.Payload, prefix) {
				continue
			}
			var b struct {
				Kind     string `json:"kind"`
				Live     int    `json:"live_authenticated_sessions"`
				Tracked  int    `json:"tracked_live_sessions"`
				Sessions []struct {
					Cookie *string `json:"socket_cookie"`
					Known  bool    `json:"inflight_known"`
				} `json:"sessions"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(event.Payload, prefix)), &b); err != nil {
				t.Fatal(err)
			}
			if b.Kind != "diagnostic_not_failure" || b.Live != 1 || b.Tracked != 1 || len(b.Sessions) != 1 || b.Sessions[0].Cookie == nil || !b.Sessions[0].Known {
				t.Fatal("only authenticated explicitly enabled listener may register")
			}
			for _, secret := range []string{"synthetic-stage-password", "wrong-synthetic-password", "fixture", "127.0.0.1"} {
				if strings.Contains(event.Payload, secret) {
					t.Fatal("sensitive metadata in stage event")
				}
			}
			t.Log("default-off session and rejected authentication excluded; enabled authenticated TLS session has private cookie and fixed evidence fields")
			return
		case <-deadline:
			t.Fatal("bounded exporter did not emit")
		}
	}
}
