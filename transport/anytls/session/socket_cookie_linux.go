//go:build linux

package session

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// SocketCookie reads Linux socket identity, never an address or socket option
// mutation. It is private join material, not a public log/artifact identifier.
func SocketCookie(conn net.Conn) *string {
	for depth := 0; depth < 8 && conn != nil; depth++ {
		if c, ok := conn.(syscall.Conn); ok {
			raw, err := c.SyscallConn()
			if err != nil {
				return nil
			}
			var cookie uint64
			var sockErr error
			if err = raw.Control(func(fd uintptr) { cookie, sockErr = unix.GetsockoptUint64(int(fd), unix.SOL_SOCKET, unix.SO_COOKIE) }); err != nil || sockErr != nil || cookie == 0 {
				return nil
			}
			value := hexUint(cookie)
			return &value
		}
		wrapped, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			return nil
		}
		next := wrapped.NetConn()
		if next == conn {
			return nil
		}
		conn = next
	}
	return nil
}
