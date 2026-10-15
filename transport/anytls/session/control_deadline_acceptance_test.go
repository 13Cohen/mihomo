package session

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/transport/anytls/padding"
	utls "github.com/metacubex/utls"
)

type acceptedFrame struct {
	command byte
	stream  uint32
	payload []byte
}

type frameResult struct {
	frames []acceptedFrame
	err    error
}

func readAcceptedFrames(reader io.Reader, count int) frameResult {
	var result frameResult
	for i := 0; i < count; i++ {
		var header rawHeader
		if _, result.err = io.ReadFull(reader, header[:]); result.err != nil {
			return result
		}
		frame := acceptedFrame{command: header.Cmd(), stream: header.StreamID(), payload: make([]byte, int(header.Length()))}
		if _, result.err = io.ReadFull(reader, frame.payload); result.err != nil {
			return result
		}
		result.frames = append(result.frames, frame)
	}
	return result
}

func awaitWrite(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("write did not finish")
	}
}

// The delay is below TLS. TLS writes real records over a real loopback TCP
// connection, while the reader continues making progress. It is a deterministic
// deadline regression fixture, not an Internet throughput benchmark.
type pacedTLSWire struct {
	net.Conn
	enabled atomic.Bool
	entered chan struct{}
	stop    chan struct{}
	once    sync.Once
}

func (c *pacedTLSWire) Write(data []byte) (int, error) {
	if !c.enabled.Load() {
		return c.Conn.Write(data)
	}
	select {
	case c.entered <- struct{}{}:
	default:
	}
	total := 0
	for len(data) > 0 {
		next := len(data)
		if next > 512 {
			next = 512
		}
		n, err := c.Conn.Write(data[:next])
		total += n
		data = data[n:]
		if err != nil {
			return total, err
		}
		if n != next {
			return total, io.ErrShortWrite
		}
		if len(data) > 0 {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-timer.C:
			case <-c.stop:
				timer.Stop()
				return total, net.ErrClosed
			}
		}
	}
	return total, nil
}

func (c *pacedTLSWire) Close() error {
	c.once.Do(func() { close(c.stop) })
	return c.Conn.Close()
}

func realTLSPair(t *testing.T, version uint16, serverWriter bool) (*utls.Conn, *utls.Conn, *pacedTLSWire) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "anytls-fixture.invalid"},
		DNSNames: []string{"anytls-fixture.invalid"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	left, err := net.DialTCP("tcp4", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { left.Close() })
	right, err := listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { right.Close() })
	listener.Close()
	// Small buffers also make the opt-in real dead-peer test reproducible.
	for _, conn := range []*net.TCPConn{left, right} {
		if err := conn.SetWriteBuffer(4096); err != nil {
			t.Fatal(err)
		}
		if err := conn.SetReadBuffer(4096); err != nil {
			t.Fatal(err)
		}
	}
	var clientWire, serverWire net.Conn = left, right
	wire := &pacedTLSWire{Conn: left, entered: make(chan struct{}, 1), stop: make(chan struct{})}
	if serverWriter {
		wire.Conn = right
		serverWire = wire
	} else {
		clientWire = wire
	}
	t.Cleanup(func() { wire.Close() })
	client := utls.Client(clientWire, &utls.Config{RootCAs: roots, ServerName: "anytls-fixture.invalid", MinVersion: version, MaxVersion: version})
	server := utls.Server(serverWire, &utls.Config{Certificates: []utls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: version, MaxVersion: version})
	left.SetDeadline(time.Now().Add(10 * time.Second))
	right.SetDeadline(time.Now().Add(10 * time.Second))
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Handshake() }()
	if err := client.Handshake(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	left.SetDeadline(time.Time{})
	right.SetDeadline(time.Time{})
	if len(client.ConnectionState().VerifiedChains) == 0 {
		t.Fatal("TLS peer certificate was not verified")
	}
	if serverWriter {
		return server, client, wire
	}
	return client, server, wire
}

func TestTLSConcurrentDATAFINPreservesPaddingAndPayload(t *testing.T) {
	for _, version := range []uint16{utls.VersionTLS12, utls.VersionTLS13} {
		t.Run(fmt.Sprintf("tls_%x", version), func(t *testing.T) {
			writer, reader, wire := realTLSPair(t, version, false)
			var factory atomic.Pointer[padding.PaddingFactory]
			if !padding.UpdatePaddingScheme([]byte("stop=3\n1=33000-33000\n2=128-128"), &factory) {
				t.Fatal("padding scheme")
			}
			s := NewClientSession(writer, &factory)
			t.Cleanup(func() { s.Close() })
			// Exercise the same initial buffering branch without starting recvLoop.
			s.buffering = true
			settings := newFrame(cmdSettings, 0)
			settings.data = []byte("v=2")
			if _, err := s.writeControlFrame(settings); err != nil {
				t.Fatal(err)
			}
			s.buffering = false
			payload := bytes.Repeat([]byte{0, 1, 2, 3, 4, 5, 6, 7}, 4096)
			last := bytes.Repeat([]byte{0xa5}, 257)
			readDone := make(chan frameResult, 1)
			go func() { readDone <- readAcceptedFrames(reader, 6) }()
			wire.enabled.Store(true)
			dataDone, controlDone := make(chan error, 1), make(chan error, 1)
			started := time.Now()
			go func() { _, err := s.writeDataFrame(1, payload); dataDone <- err }()
			select {
			case <-wire.entered:
			case <-time.After(time.Second):
				t.Fatal("DATA did not enter TLS writer")
			}
			go func() { _, err := s.writeControlFrame(newFrame(cmdFIN, 3)); controlDone <- err }()
			awaitWrite(t, dataDone)
			awaitWrite(t, controlDone)
			if _, err := s.writeDataFrame(5, last); err != nil {
				t.Fatal(err)
			}
			var result frameResult
			select {
			case result = <-readDone:
			case <-time.After(3 * time.Second):
				t.Fatal("reader did not finish all frames")
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			want := []acceptedFrame{{cmdSettings, 0, settings.data}, {cmdPSH, 1, payload}, {cmdWaste, 0, make([]byte, 208)}, {cmdFIN, 3, nil}, {cmdWaste, 0, make([]byte, 114)}, {cmdPSH, 5, last}}
			if len(result.frames) != len(want) {
				t.Fatalf("frames=%d", len(result.frames))
			}
			for i, frame := range result.frames {
				if frame.command != want[i].command || frame.stream != want[i].stream || !bytes.Equal(frame.payload, want[i].payload) {
					t.Fatalf("frame %d differs: cmd=%d sid=%d len=%d", i, frame.command, frame.stream, len(frame.payload))
				}
			}
			if s.sendPadding {
				t.Fatal("padding did not stop at configured boundary")
			}
			t.Logf("verified TLS, six exact frames, initial buffering, padding stop and concurrent DATA/FIN: %.3fs", time.Since(started).Seconds())
		})
	}
}

func TestTLSServerConcurrentDATAFINIntegrity(t *testing.T) {
	for _, version := range []uint16{utls.VersionTLS12, utls.VersionTLS13} {
		t.Run(fmt.Sprintf("tls_%x", version), func(t *testing.T) {
			writer, reader, wire := realTLSPair(t, version, true)
			s := NewServerSession(writer, nil, nil)
			t.Cleanup(func() { s.Close() })
			payload := bytes.Repeat([]byte{0, 9, 8, 7, 6, 5, 4, 3}, 4096)
			readDone := make(chan frameResult, 1)
			go func() { readDone <- readAcceptedFrames(reader, 2) }()
			wire.enabled.Store(true)
			dataDone, controlDone := make(chan error, 1), make(chan error, 1)
			go func() { _, err := s.writeDataFrame(11, payload); dataDone <- err }()
			select {
			case <-wire.entered:
			case <-time.After(time.Second):
				t.Fatal("server DATA did not enter writer")
			}
			go func() { _, err := s.writeControlFrame(newFrame(cmdFIN, 13)); controlDone <- err }()
			awaitWrite(t, dataDone)
			awaitWrite(t, controlDone)
			var result frameResult
			select {
			case result = <-readDone:
			case <-time.After(3 * time.Second):
				t.Fatal("server frames incomplete")
			}
			if result.err != nil {
				t.Fatal(result.err)
			}
			data, fin := result.frames[0], result.frames[1]
			if data.command != cmdPSH || data.stream != 11 || !bytes.Equal(data.payload, payload) {
				t.Fatal("server DATA differs")
			}
			if fin.command != cmdFIN || fin.stream != 13 || len(fin.payload) != 0 {
				t.Fatal("server FIN differs")
			}
		})
	}
}

func TestConcurrentStreamsHaveCompleteFrames(t *testing.T) {
	left, right := net.Pipe()
	s := NewServerSession(left, nil, nil)
	t.Cleanup(func() { s.Close(); right.Close() })
	const streams = 16
	readDone := make(chan frameResult, 1)
	go func() { readDone <- readAcceptedFrames(right, streams*2) }()
	done := make(chan error, streams)
	for i := 1; i <= streams; i++ {
		sid := uint32(i)
		go func() {
			_, err := s.writeDataFrame(sid, bytes.Repeat([]byte{byte(sid)}, 2048))
			if err == nil {
				_, err = s.writeControlFrame(newFrame(cmdFIN, sid))
			}
			done <- err
		}()
	}
	for i := 0; i < streams; i++ {
		awaitWrite(t, done)
	}
	result := <-readDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	seen := map[uint32]int{}
	for _, frame := range result.frames {
		if frame.stream == 0 || frame.stream > streams {
			t.Fatal("unexpected stream")
		}
		switch frame.command {
		case cmdPSH:
			if seen[frame.stream] != 0 || !bytes.Equal(frame.payload, bytes.Repeat([]byte{byte(frame.stream)}, 2048)) {
				t.Fatal("DATA corruption or duplicate")
			}
			seen[frame.stream] = 1
		case cmdFIN:
			if seen[frame.stream] != 1 || len(frame.payload) != 0 {
				t.Fatal("FIN before DATA or corrupt payload")
			}
			seen[frame.stream] = 2
		default:
			t.Fatal("unexpected command")
		}
	}
	if len(seen) != streams {
		t.Fatal("missing stream")
	}
	for _, state := range seen {
		if state != 2 {
			t.Fatal("missing FIN")
		}
	}
}

func TestSessionCloseUnblocksQueuedWriters(t *testing.T) {
	left, right := net.Pipe()
	c := &evidenceConn{Conn: left, entered: make(chan struct{}, 1)}
	s := NewServerSession(c, nil, nil)
	t.Cleanup(func() { s.Close(); right.Close() })
	done := make(chan error, 2)
	go func() { _, err := s.writeDataFrame(1, make([]byte, 32768)); done <- err }()
	<-c.entered
	go func() { _, err := s.writeControlFrame(newFrame(cmdFIN, 3)); done <- err }()
	var closers sync.WaitGroup
	for i := 0; i < 8; i++ {
		closers.Add(1)
		go func() { defer closers.Done(); s.Close() }()
	}
	closed := make(chan struct{})
	go func() { closers.Wait(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close stuck")
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("canceled write succeeded")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("queued writer stuck after Close")
		}
	}
	if !s.IsClosed() {
		t.Fatal("session still open")
	}
}

// Exercise the exported stream API as well as Session's internal writer lock.
// Baseline and candidate must both be run under -race: pre-existing stream-state
// races must not be mistaken for regressions introduced by deadline ownership.
func TestConcurrentStreamAPIClose(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		left, right := net.Pipe()
		s := NewServerSession(left, nil, nil)
		stream := newStream(1, s)
		s.streams[1] = stream
		readDone := make(chan struct{})
		go func() { io.Copy(io.Discard, right); close(readDone) }()
		start := make(chan struct{})
		var workers sync.WaitGroup
		for i := 0; i < 8; i++ {
			workers.Add(1)
			go func() { defer workers.Done(); <-start; stream.Write(make([]byte, 512)) }()
		}
		workers.Add(1)
		go func() { defer workers.Done(); <-start; stream.Close() }()
		close(start)
		finished := make(chan struct{})
		go func() { workers.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			s.Close()
			right.Close()
			t.Fatal("stream API cancellation stuck")
		}
		s.Close()
		right.Close()
		<-readDone
	}
}

type partialFailureConn struct {
	net.Conn
	first atomic.Bool
}

func (c *partialFailureConn) Write(p []byte) (int, error) {
	if c.first.CompareAndSwap(false, true) {
		return len(p) / 2, io.ErrUnexpectedEOF
	}
	return c.Conn.Write(p)
}

func TestPartialDATAFailureClosesSession(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	s := NewServerSession(&partialFailureConn{Conn: left}, nil, nil)
	defer s.Close()
	if _, err := s.writeDataFrame(1, []byte("partial frame")); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error=%v", err)
	}
	if !s.IsClosed() {
		t.Fatal("partial DATA failure left session open")
	}
	if _, err := s.writeDataFrame(3, []byte("must not reuse")); err == nil {
		t.Fatal("reused a partial-frame connection")
	}
}

func TestRealTLSDeadPeerWriteBudget(t *testing.T) {
	if os.Getenv("PCS_ANYTLS_REAL_DEADLINE") != "1" {
		t.Skip("opt-in: real 60-second TCP/TLS deadline")
	}
	writer, _, wire := realTLSPair(t, utls.VersionTLS13, true)
	s := NewServerSession(writer, nil, nil)
	t.Cleanup(func() { wire.Close(); s.Close() })
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		// The peer never consumes application records. Bounds prevent unbounded
		// fixture traffic even if a platform ignores the requested socket sizes.
		for i := 0; i < 256; i++ {
			if _, err := s.writeDataFrame(1, make([]byte, 32768)); err != nil {
				done <- err
				return
			}
		}
		done <- errors.New("8 MiB failed to fill the deliberately small socket buffers")
	}()
	select {
	case err := <-done:
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("expected real socket timeout: %v", err)
		}
		elapsed := time.Since(started)
		if elapsed < 59*time.Second || elapsed > 68*time.Second {
			t.Fatalf("unexpected real deadline duration: %v", elapsed)
		}
		if !s.IsClosed() {
			t.Fatal("dead peer left session open")
		}
		t.Logf("real TCP/TLS dead-peer exit after %.3fs", elapsed.Seconds())
	case <-time.After(70 * time.Second):
		t.Fatal("dead peer exceeded bounded cleanup time")
	}
}
