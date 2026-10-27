// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package runtime

import (
	"errors"
	"syscall"
)

// lingerZero fails every dial: the runtime binary serves Lambda, which runs Linux, and this
// file only lets code that imports runtime build elsewhere.
func lingerZero(string, string, syscall.RawConn) error {
	return errors.New("runtime: the runtime binary runs only on Linux")
}
