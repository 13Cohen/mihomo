package session

import (
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

type evidenceConn struct {
	net.Conn
	entered chan struct{}
	shorten bool
	mu      sync.Mutex
	budgets []time.Duration
}

func (c *evidenceConn) Write(p []byte) (int, error) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	return c.Conn.Write(p)
}
func (c *evidenceConn) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() {
		c.mu.Lock()
		c.budgets = append(c.budgets, time.Until(t))
		c.mu.Unlock()
		if c.shorten {
			t = time.Now().Add(80 * time.Millisecond)
		}
	}
	return c.Conn.SetWriteDeadline(t)
}

func TestControlDeadlineDoesNotInterruptProgressingData(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	c := &evidenceConn{Conn: left, entered: make(chan struct{}, 2)}
	s := NewServerSession(c, nil, nil)
	defer s.Close()
	dataDone := make(chan error, 1)
	controlDone := make(chan error, 1)
	started := time.Now()
	go func() { _, err := s.writeDataFrame(1, make([]byte, 64*1024-1)); dataDone <- err }()
	<-c.entered
	go func() { _, err := s.writeControlFrame(newFrame(cmdFIN, 3)); controlDone <- err }()
	readDone := make(chan int, 1)
	go func() {
		buf := make([]byte, 1024)
		total := 0
		for {
			n, err := right.Read(buf)
			total += n
			if err != nil {
				readDone <- total
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	for _, done := range []chan error{dataDone, controlDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("write stuck")
		}
	}
	elapsed := time.Since(started)
	s.Close()
	right.Close()
	n := <-readDone
	if n != 65549 {
		t.Fatalf("framing bytes=%d, want65549", n)
	}
	t.Logf("progressing DATA plus unrelated FIN complete in %.3fs; bytes=%d", elapsed.Seconds(), n)
}

func TestDeadPeerStillHasBoundedWriteBudget(t *testing.T) {
	for _, control := range []bool{false, true} {
		left, right := net.Pipe()
		c := &evidenceConn{Conn: left, entered: make(chan struct{}, 1), shorten: true}
		s := NewServerSession(c, nil, nil)
		var err error
		want := 60 * time.Second
		if control {
			_, err = s.writeControlFrame(newFrame(cmdFIN, 3))
			want = 5 * time.Second
		} else {
			_, err = s.writeDataFrame(1, []byte("blocked"))
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("control=%v err=%v", control, err)
		}
		if !s.IsClosed() {
			t.Fatal("failed partial frame left session reusable")
		}
		if len(c.budgets) != 1 || c.budgets[0] < want-time.Second || c.budgets[0] > want {
			t.Fatalf("control=%v unexpected budgets=%v", control, c.budgets)
		}
		t.Logf("control=%v real requested budget=%v, accelerated dead-peer exit verified", control, c.budgets[0])
		s.Close()
		right.Close()
	}
}
