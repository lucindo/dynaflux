// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package runtime

import (
	"os"
	"syscall"
)

// lingerZero is a [net.Dialer]'s Control that sets SO_LINGER 0 on the socket it dials.
func lingerZero(_, _ string, c syscall.RawConn) error {
	var err error
	if cerr := c.Control(func(fd uintptr) {
		err = syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET, syscall.SO_LINGER, &syscall.Linger{Onoff: 1})
	}); cerr != nil {
		return cerr
	}
	return os.NewSyscallError("setsockopt SO_LINGER", err)
}
