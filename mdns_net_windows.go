//go:build windows

package main

import (
	"net"
	"syscall"
	"unsafe"
)

var mdnsSetsockopt = syscall.NewLazyDLL("ws2_32.dll").NewProc("setsockopt")

// mdnsListenConfig 在 Windows 上仅设置 SO_REUSEADDR（无 SO_REUSEPORT）。
func mdnsListenConfig() net.ListenConfig {
	return net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var sockErr error
		controlErr := c.Control(func(fd uintptr) {
			if err := syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
				sockErr = err
			}
		})
		if controlErr != nil {
			return controlErr
		}
		return sockErr
	}}
}

// joinMDNSGroup 通过 Winsock setsockopt 加入 mDNS 组播组。
func joinMDNSGroup(conn *net.UDPConn, ip net.IP) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var callErr syscall.Errno
	controlErr := raw.Control(func(fd uintptr) {
		var mreq [8]byte
		copy(mreq[0:4], net.ParseIP(mdnsGroup).To4())
		if v4 := ip.To4(); v4 != nil {
			copy(mreq[4:8], v4)
		}
		r1, _, lastErr := mdnsSetsockopt.Call(
			fd,
			uintptr(syscall.IPPROTO_IP),
			uintptr(syscall.IP_ADD_MEMBERSHIP),
			uintptr(unsafe.Pointer(&mreq[0])),
			unsafe.Sizeof(mreq),
		)
		if r1 != 0 {
			if errno, ok := lastErr.(syscall.Errno); ok {
				callErr = errno
			}
		}
	})
	if controlErr != nil {
		return controlErr
	}
	if callErr != 0 {
		return callErr
	}
	return nil
}
