package session

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/transport/anytls/padding"
)

func lifecyclePadding(t *testing.T) *atomic.Pointer[padding.PaddingFactory] {
	t.Helper()
	p := new(atomic.Pointer[padding.PaddingFactory])
	if !padding.UpdatePaddingScheme(padding.DefaultPaddingScheme, p) {
		t.Fatal("padding")
	}
	return p
}

func TestClientCloseDuringDialDoesNotPublishSession(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	left, right := net.Pipe()
	t.Cleanup(func() { left.Close(); right.Close() })
	c := NewClient(context.Background(), func(context.Context) (net.Conn, error) {
		close(entered)
		<-release
		return left, nil
	}, lifecyclePadding(t), time.Minute, time.Minute, 0)
	t.Cleanup(func() { c.Close() })
	type result struct {
		conn net.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() { conn, err := c.CreateStream(context.Background()); done <- result{conn, err} }()
	<-entered
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case r := <-done:
		c.sessionsLock.Lock()
		retained := len(c.sessions)
		c.sessionsLock.Unlock()
		if r.conn != nil {
			left.Close()
			c.Close()
		}
		if r.err == nil || r.conn != nil || retained != 0 {
			t.Fatalf("closed client published late dial: returned_stream=%t err=%v retained_sessions=%d", r.conn != nil, r.err, retained)
		}
	case <-time.After(time.Second):
		t.Fatal("dial completion stuck after Close")
	}
}

// Repeated real client-pool reuse exercises the public API while its actual
// idle cleanup runs concurrently. The peer parses real Session frames and
// verifies all bytes by echoing them. This fixture does not include TLS/auth.
func TestClientPoolConcurrentReuseAndIdleCleanup(t *testing.T) {
	p := lifecyclePadding(t)
	var accepted atomic.Int64
	var peersMu sync.Mutex
	var peers []*Session
	dial := func(context.Context) (net.Conn, error) {
		left, right := net.Pipe()
		peer := NewServerSession(right, func(s *Stream) {
			defer s.Close()
			if err := s.HandshakeSuccess(); err != nil {
				return
			}
			io.Copy(s, s)
		}, p)
		peersMu.Lock()
		peers = append(peers, peer)
		peersMu.Unlock()
		accepted.Add(1)
		go peer.Run()
		return left, nil
	}
	c := NewClient(context.Background(), dial, p, time.Minute, time.Minute, 0)
	t.Cleanup(func() {
		c.Close()
		peersMu.Lock()
		defer peersMu.Unlock()
		for _, peer := range peers {
			peer.Close()
		}
	})
	stop := make(chan struct{})
	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		for {
			select {
			case <-stop:
				return
			default:
				c.idleCleanupExpTime(time.Now().Add(-time.Hour))
				time.Sleep(time.Millisecond)
			}
		}
	}()
	const workers, rounds = 4, 12
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				conn, err := c.CreateStream(context.Background())
				if err != nil {
					errs <- err
					return
				}
				payload := []byte{byte(w), byte(r), 0, 255, 73, 129}
				conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				if _, err = conn.Write(payload); err != nil {
					conn.Close()
					errs <- err
					return
				}
				got := make([]byte, len(payload))
				_, err = io.ReadFull(conn, got)
				if err == nil {
					for i := range got {
						if got[i] != payload[i] {
							err = io.ErrUnexpectedEOF
							break
						}
					}
				}
				conn.Close()
				if err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	<-cleanupDone
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if accepted.Load() >= workers*rounds {
		t.Fatal("no connection reuse")
	}
	t.Logf("completed_streams=%d physical_sessions=%d", workers*rounds, accepted.Load())
}
