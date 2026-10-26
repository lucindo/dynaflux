// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux

import (
	"bytes"
	"encoding/hex"
	"errors"
)

// UUID is a 128-bit UUID. Its methods behave as those of Go 1.27's [uuid.UUID], which it
// will become an alias of; until then, the two types convert into each other. To make one,
// read crypto/rand into it, rand.Read(id[:]), or on Go 1.27 convert [uuid.NewV7].
type UUID [16]byte

// textLen is the length of the 8-4-4-4-12 form.
const textLen = 36

var errInvalidUUID = errors.New("dynaflux: invalid UUID")

// ParseUUID parses a UUID in one of the forms Go 1.27's [uuid.Parse] accepts, with hex digits
// in either case:
//
//	f81d4fae-7dec-11d0-a765-00a0c91e6bf6
//	{f81d4fae-7dec-11d0-a765-00a0c91e6bf6}
//	urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6
//	f81d4fae7dec11d0a76500a0c91e6bf6
func ParseUUID(s string) (UUID, error) {
	var u UUID
	err := u.UnmarshalText([]byte(s))
	return u, err
}

// String returns u in lowercase 8-4-4-4-12 form.
func (u UUID) String() string { return string(u.appendText(make([]byte, 0, textLen))) }

// Compare returns -1, 0 or +1 as u sorts before, with or after v, byte by byte.
func (u UUID) Compare(v UUID) int { return bytes.Compare(u[:], v[:]) }

// AppendText implements [encoding.TextAppender] with the form of [UUID.String].
func (u UUID) AppendText(b []byte) ([]byte, error) { return u.appendText(b), nil }

// MarshalText implements [encoding.TextMarshaler] with the form of [UUID.String].
func (u UUID) MarshalText() ([]byte, error) { return u.appendText(make([]byte, 0, textLen)), nil }

func (u UUID) appendText(b []byte) []byte {
	b = hex.AppendEncode(b, u[0:4])
	for _, part := range [][]byte{u[4:6], u[6:8], u[8:10], u[10:16]} {
		b = hex.AppendEncode(append(b, '-'), part)
	}
	return b
}

// UnmarshalText implements [encoding.TextUnmarshaler]; it accepts the forms of [ParseUUID].
func (u *UUID) UnmarshalText(b []byte) error {
	var dst UUID
	const urn, plain = "urn:uuid:", 32
	switch len(b) {
	case len(urn) + textLen:
		b = bytes.TrimPrefix(b, []byte(urn))
	case len("{}") + textLen:
		b = bytes.TrimSuffix(bytes.TrimPrefix(b, []byte("{")), []byte("}"))
	case plain:
		if _, err := hex.Decode(dst[:], b); err != nil {
			return errInvalidUUID
		}
		*u = dst
		return nil
	}
	if len(b) != textLen || b[8] != '-' || b[13] != '-' || b[18] != '-' || b[23] != '-' {
		return errInvalidUUID
	}
	digits := make([]byte, 0, plain)
	for _, part := range [][]byte{b[0:8], b[9:13], b[14:18], b[19:23], b[24:36]} {
		digits = append(digits, part...)
	}
	if _, err := hex.Decode(dst[:], digits); err != nil {
		return errInvalidUUID
	}
	*u = dst
	return nil
}
