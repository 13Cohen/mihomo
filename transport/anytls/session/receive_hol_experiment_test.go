package session

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/transport/anytls/padding"
)

// recvFixtureWire presents an already complete ordered byte stream to recvLoop.
// A gate can separately simulate a DATA frame whose remaining wire bytes have
// not arrived. It isolates application dispatch from TLS/TCP loss and latency;
// it is not a transport benchmark or a replacement for real-client acceptance.
type recvFixtureWire struct {
	data          []byte
	offset        atomic.Int64
	advanced      chan struct{}
	done          chan struct{}
	closeOnce     sync.Once
	gateAt        int
	gate          chan struct{}
	gateOnce      sync.Once
	heartbeat     chan struct{}
	heartbeatOnce sync.Once
}

func newRecvFixtureWire(data []byte, gateAt int) *recvFixtureWire {
	return &recvFixtureWire{data: data, advanced: make(chan struct{}, 1), done: make(chan struct{}), gateAt: gateAt, gate: make(chan struct{}), heartbeat: make(chan struct{})}
}

func (c *recvFixtureWire) Read(p []byte) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	position := int(c.offset.Load())
	if c.gateAt >= 0 && position >= c.gateAt {
		select {
		case <-c.gate:
		case <-c.done:
			return 0, net.ErrClosed
		}
	}
	if position >= len(c.data) {
		<-c.done
		return 0, net.ErrClosed
	}
	end := len(c.data)
	if c.gateAt >= 0 && position < c.gateAt {
		end = c.gateAt
	}
	n := copy(p, c.data[position:end])
	c.offset.Add(int64(n))
	select {
	case c.advanced <- struct{}{}:
	default:
	}
	return n, nil
}

func (c *recvFixtureWire) Write(p []byte) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	// Server writes are serialized by Session.connLock. Observe the actual
	// response frame, not a callback that could bypass protocol processing.
	if len(p) >= headerOverHeadSize && p[0] == cmdHeartResponse && binary.BigEndian.Uint16(p[5:7]) == 0 {
		c.heartbeatOnce.Do(func() { close(c.heartbeat) })
	}
	return len(p), nil
}

func (c *recvFixtureWire) Close() error { c.closeOnce.Do(func() { close(c.done) }); return nil }
func (c *recvFixtureWire) releaseWire() { c.gateOnce.Do(func() { close(c.gate) }) }
func (c *recvFixtureWire) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}
func (c *recvFixtureWire) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}
}
func (c *recvFixtureWire) SetDeadline(time.Time) error      { return nil }
func (c *recvFixtureWire) SetReadDeadline(time.Time) error  { return nil }
func (c *recvFixtureWire) SetWriteDeadline(time.Time) error { return nil }

func (c *recvFixtureWire) awaitOffset(t *testing.T, target int) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for int(c.offset.Load()) < target {
		select {
		case <-c.advanced:
		case <-timer.C:
			t.Fatalf("receiver consumed %d bytes, expected at least %d", c.offset.Load(), target)
		}
	}
}

func recvFixtureFrame(command byte, id uint32, data []byte) []byte {
	frame := make([]byte, headerOverHeadSize+len(data))
	frame[0] = command
	binary.BigEndian.PutUint32(frame[1:5], id)
	binary.BigEndian.PutUint16(frame[5:7], uint16(len(data)))
	copy(frame[headerOverHeadSize:], data)
	return frame
}

type recvFixture struct {
	session                  *Session
	wire                     *recvFixtureWire
	slow, fast               *Stream
	slowPayload, fastPayload []byte
	slowFrameEnd             int
	runDone                  chan struct{}
}

func startRecvFixture(t *testing.T, gateIncompleteFrame bool) *recvFixture {
	t.Helper()
	var factory atomic.Pointer[padding.PaddingFactory]
	if !padding.UpdatePaddingScheme(padding.DefaultPaddingScheme, &factory) {
		t.Fatal("default padding factory")
	}
	settings := []byte(fmt.Sprintf("v=2\npadding-md5=%s", factory.Load().Md5))
	data := recvFixtureFrame(cmdSettings, 0, settings)
	data = append(data, recvFixtureFrame(cmdSYN, 1, nil)...)
	data = append(data, recvFixtureFrame(cmdSYN, 3, nil)...)
	slow := bytes.Repeat([]byte{0x51, 0x7a, 0x00, 0xff}, 1024)
	fast := []byte("unrelated-stream-payload-must-stay-intact")
	gateAt := -1
	if gateIncompleteFrame {
		gateAt = len(data) + headerOverHeadSize + len(slow)/2
	}
	data = append(data, recvFixtureFrame(cmdPSH, 1, slow)...)
	slowEnd := len(data)
	data = append(data, recvFixtureFrame(cmdHeartRequest, 0, nil)...)
	data = append(data, recvFixtureFrame(cmdPSH, 3, fast)...)
	data = append(data, recvFixtureFrame(cmdFIN, 3, nil)...)
	data = append(data, recvFixtureFrame(cmdFIN, 1, nil)...)
	wire := newRecvFixtureWire(data, gateAt)
	accepted := make(chan *Stream, 2)
	s := NewServerSession(wire, func(stream *Stream) { accepted <- stream }, &factory)
	fixture := &recvFixture{session: s, wire: wire, slowPayload: slow, fastPayload: fast, slowFrameEnd: slowEnd, runDone: make(chan struct{})}
	go func() { s.Run(); close(fixture.runDone) }()
	t.Cleanup(func() {
		s.Close()
		select {
		case <-fixture.runDone:
		case <-time.After(time.Second):
			t.Error("recvLoop leaked after Session.Close")
		}
	})
	for i := 0; i < 2; i++ {
		select {
		case stream := <-accepted:
			switch stream.id {
			case 1:
				fixture.slow = stream
			case 3:
				fixture.fast = stream
			default:
				t.Fatal("unexpected stream")
			}
		case <-time.After(time.Second):
			t.Fatal("SYN was not dispatched")
		}
	}
	return fixture
}

func recvFixtureReader(stream *Stream, want []byte) <-chan error {
	done := make(chan error, 1)
	go func() {
		actual := make([]byte, len(want))
		if _, err := io.ReadFull(stream, actual); err != nil {
			done <- err
			return
		}
		if !bytes.Equal(actual, want) {
			done <- errors.New("stream payload corrupted")
			return
		}
		// Completion also requires FIN processing, not just DATA dispatch.
		var one [1]byte
		n, err := stream.Read(one[:])
		if n != 0 || !errors.Is(err, net.ErrClosed) {
			done <- fmt.Errorf("FIN result n=%d err=%v", n, err)
			return
		}
		done <- nil
	}()
	return done
}

func awaitRecvFixture(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not complete")
	}
}

// The default form records the current behavior without treating that behavior
// as a desirable contract. PCS_ANYTLS_REQUIRE_RECV_ISOLATION=1 turns the same
// fixture into a future candidate's acceptance gate and should fail on baseline.
func TestReceiveSlowReaderDiagnostic(t *testing.T) {
	blocked := startRecvFixture(t, false)
	blocked.wire.awaitOffset(t, blocked.slowFrameEnd)
	fastDone := recvFixtureReader(blocked.fast, blocked.fastPayload)

	// A separate Session on an independent synthetic transport must continue.
	healthy := startRecvFixture(t, false)
	healthySlow := recvFixtureReader(healthy.slow, healthy.slowPayload)
	healthyFast := recvFixtureReader(healthy.fast, healthy.fastPayload)
	awaitRecvFixture(t, healthyFast)
	awaitRecvFixture(t, healthySlow)

	progressed := false
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatal(err)
		}
		progressed = true
	case <-time.After(100 * time.Millisecond):
	}
	heartbeatProgressed := false
	select {
	case <-blocked.wire.heartbeat:
		heartbeatProgressed = true
	default:
	}
	t.Logf("complete slow DATA available; same-session DATA+FIN progressed=%t heartbeat=%t; separate-session DATA+FIN completed=true; consumed=%d available=%d", progressed, heartbeatProgressed, blocked.wire.offset.Load(), len(blocked.wire.data))

	// Releasing only the slow application reader must restore exact delivery.
	slowDone := recvFixtureReader(blocked.slow, blocked.slowPayload)
	awaitRecvFixture(t, slowDone)
	if !progressed {
		awaitRecvFixture(t, fastDone)
	}
	select {
	case <-blocked.wire.heartbeat:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not resume")
	}
	if os.Getenv("PCS_ANYTLS_REQUIRE_RECV_ISOLATION") == "1" && (!progressed || !heartbeatProgressed) {
		t.Fatal("a complete slow-stream frame blocked unrelated stream/control dispatch")
	}
}

func TestReceiveIncompleteFrameMustWaitForWireBytes(t *testing.T) {
	fixture := startRecvFixture(t, true)
	fixture.wire.awaitOffset(t, fixture.wire.gateAt)
	slowDone := recvFixtureReader(fixture.slow, fixture.slowPayload)
	fastDone := recvFixtureReader(fixture.fast, fixture.fastPayload)
	select {
	case err := <-fastDone:
		t.Fatalf("later frame dispatched before preceding frame bytes arrived: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case <-fixture.wire.heartbeat:
		t.Fatal("control frame bypassed an incomplete wire frame")
	default:
	}
	if fixture.wire.offset.Load() != int64(fixture.wire.gateAt) {
		t.Fatal("wire gate was bypassed")
	}
	fixture.wire.releaseWire()
	awaitRecvFixture(t, slowDone)
	awaitRecvFixture(t, fastDone)
	t.Log("both application readers ready; only releasing missing wire bytes restores DATA/FIN delivery")
}

func TestReceiveBlockedReaderSessionCancellation(t *testing.T) {
	fixture := startRecvFixture(t, false)
	fixture.wire.awaitOffset(t, fixture.slowFrameEnd)
	if err := fixture.session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fixture.runDone:
	case <-time.After(time.Second):
		t.Fatal("Session.Close did not cancel blocked stream dispatch")
	}
	for _, stream := range []*Stream{fixture.slow, fixture.fast} {
		var one [1]byte
		if _, err := stream.Read(one[:]); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("closed stream error: %v", err)
		}
	}
}
