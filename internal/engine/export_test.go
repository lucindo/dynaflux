// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

// TokenEvent is an event that a token names: the key of the receipt that a wait polls for
// it, and its flags.
type TokenEvent struct {
	EventID      string
	IngestMicros int64
	Flags        byte
}

// DecodeToken returns the region, stream and events of a token's text.
func DecodeToken(text string) (string, string, []TokenEvent, error) {
	t, err := decodeToken(text)
	if err != nil || t == nil {
		return "", "", nil, err
	}
	var evs []TokenEvent
	for _, ev := range t.events {
		k := ev.receipt()
		evs = append(evs, TokenEvent{EventID: k.EventID, IngestMicros: k.It, Flags: ev.flags})
	}
	return t.region, t.stream, evs, nil
}
