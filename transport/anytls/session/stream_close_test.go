package session

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func drainedStream(t *testing.T) *Stream {
	t.Helper()
	left, right := net.Pipe()
	s := NewServerSession(left, nil, nil)
	stream := newStream(1, s)
	s.streams[1] = stream
	done := make(chan struct{})
	go func() { io.Copy(io.Discard, right); close(done) }()
	t.Cleanup(func() { s.Close(); right.Close(); <-done })
	return stream
}

func TestStreamCloseErrorPublication(t *testing.T) {
	remoteError := errors.New("fixture remote close")
	for _, tc := range []struct {
		name  string
		want  error
		close func(*Stream)
	}{
		{"local", net.ErrClosed, func(s *Stream) { s.closeLocally() }},
		{"public", io.ErrClosedPipe, func(s *Stream) { s.Close() }},
		{"remote", remoteError, func(s *Stream) { s.closeWithError(remoteError) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := drainedStream(t)
			readResults := make(chan error, 8)
			for i := 0; i < cap(readResults); i++ {
				go func() {
					_, err := stream.Read(make([]byte, 1))
					readResults <- err
				}()
			}
			var writers sync.WaitGroup
			for i := 0; i < 8; i++ {
				writers.Add(1)
				go func() {
					defer writers.Done()
					for j := 0; j < 16; j++ {
						if _, err := stream.Write([]byte{1}); err != nil {
							return
						}
					}
				}()
			}
			tc.close(stream)
			for i := 0; i < cap(readResults); i++ {
				select {
				case err := <-readResults:
					if err != tc.want {
						t.Fatalf("Read error = %v, want exact published error %v", err, tc.want)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("Read did not observe stream closure")
				}
			}
			finished := make(chan struct{})
			go func() { writers.Wait(); close(finished) }()
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("Write did not finish during stream closure")
			}
			if _, err := stream.Write([]byte{1}); err != tc.want {
				t.Fatalf("post-close Write error = %v, want %v", err, tc.want)
			}
			if err := stream.Close(); err != tc.want {
				t.Fatalf("repeated Close error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestStreamConcurrentCloseReasonsFirstWins(t *testing.T) {
	stream := drainedStream(t)
	first, second := errors.New("first fixture reason"), errors.New("second fixture reason")
	var hookCalls atomic.Int32
	stream.dieHook = func() { hookCalls.Add(1) }
	start := make(chan struct{})
	var closers sync.WaitGroup
	for i := 0; i < 24; i++ {
		closers.Add(1)
		go func(choice int) {
			defer closers.Done()
			<-start
			switch choice % 3 {
			case 0:
				stream.closeWithError(first)
			case 1:
				stream.closeWithError(second)
			default:
				stream.closeLocally()
			}
		}(i)
	}
	close(start)
	finished := make(chan struct{})
	go func() { closers.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("concurrent closers did not finish")
	}
	_, published := stream.Write([]byte{1})
	if published != first && published != second && published != net.ErrClosed {
		t.Fatalf("unexpected published close reason: %v", published)
	}
	if _, err := stream.Read(make([]byte, 1)); err != published {
		t.Fatalf("Read and Write disagree about close reason: %v vs %v", err, published)
	}
	if err := stream.Close(); err != published {
		t.Fatalf("repeated Close changed reason: %v vs %v", err, published)
	}
	if hookCalls.Load() != 1 {
		t.Fatalf("close hook ran %d times", hookCalls.Load())
	}
}
