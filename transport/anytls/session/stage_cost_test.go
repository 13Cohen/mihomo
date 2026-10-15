package session

import (
	"io"
	"net"
	"testing"
	"time"
)

type stageCostConn struct{}

func (stageCostConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (stageCostConn) Write(b []byte) (int, error)      { return len(b), nil }
func (stageCostConn) Close() error                     { return nil }
func (stageCostConn) LocalAddr() net.Addr              { return nil }
func (stageCostConn) RemoteAddr() net.Addr             { return nil }
func (stageCostConn) SetDeadline(time.Time) error      { return nil }
func (stageCostConn) SetReadDeadline(time.Time) error  { return nil }
func (stageCostConn) SetWriteDeadline(time.Time) error { return nil }
func BenchmarkStageEvidenceDisabledCost(b *testing.B) {
	s := NewServerSession(stageCostConn{}, nil, nil)
	payload := make([]byte, 16384)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.writeDataFrame(1, payload)
	}
}
