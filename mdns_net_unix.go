//go:build !windows

package main

import (
	"net"
	"syscall"
)

// mdnsListenConfig 允许与系统 mDNS 服务共享 5353 端口。
func mdnsListenConfig() net.ListenConfig {
	return net.ListenConfig{Control: func(network, address string, c syscall.RawConn) error {
		var sockErr error
		controlErr := c.Control(func(fd uintptr) {
			if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
				sockErr = err
				return
			}
			_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
		})
		if controlErr != nil {
			return controlErr
		}
		return sockErr
	}}
}

// joinMDNSGroup 通过 IP_ADD_MEMBERSHIP 加入 mDNS 组播组。
func joinMDNSGroup(conn *net.UDPConn, ip net.IP) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var joinErr error
	controlErr := raw.Control(func(fd uintptr) {
		var mreq [8]byte
		copy(mreq[0:4], net.ParseIP(mdnsGroup).To4())
		if v4 := ip.To4(); v4 != nil {
			copy(mreq[4:8], v4)
		}
		joinErr = syscall.SetsockoptString(int(fd), syscall.IPPROTO_IP, syscall.IP_ADD_MEMBERSHIP, string(mreq[:]))
	})
	if controlErr != nil {
		return controlErr
	}
	return joinErr
}
