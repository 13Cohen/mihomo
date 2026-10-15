//go:build !linux

package session

import "net"

func SocketCookie(net.Conn) *string { return nil }
