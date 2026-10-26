// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package keys

import (
	"cmp"
	"fmt"
	"math/big"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/store"
)

// hwmBackfill is the _hwm entry name of the backfill's mark.
const hwmBackfill = "B"

// HWM is a row item's high-water marks, its _hwm attribute (encoding.md §7.6).
type HWM struct {
	// Units maps a [UnitKey] to the last sequence number of that unit applied to the item, in
	// [PadSeq]'s form.
	Units map[string]string
	// Backfill is the backfill's mark: nil until a backfill applies a record to the item.
	Backfill *Mark
}

// Mark is a backfill's position: the [UnitKey] and [PadSeq] sequence number of the last
// record it applied. The backfill applies records in the order of [Mark.Compare].
type Mark struct {
	UnitKey, Seq string
}

// Compare orders marks by unit key, then by sequence number.
func (m Mark) Compare(o Mark) int {
	return cmp.Or(strings.Compare(m.UnitKey, o.UnitKey), strings.Compare(m.Seq, o.Seq))
}

// DecodeHWM reads an item's _hwm attribute; an absent one holds no marks. It accepts only
// what [HWM.Encode] writes.
func DecodeHWM(a store.Value, present bool) (HWM, error) {
	h := HWM{Units: map[string]string{}}
	if !present {
		return h, nil
	}
	if a.Type != store.M {
		return HWM{}, fmt.Errorf("%s must be a DynamoDB M", catalog.AttrHighWaterMarks)
	}
	for name, v := range a.Map {
		if v.Type != store.B {
			return HWM{}, fmt.Errorf("%s entry %q must be a DynamoDB B", catalog.AttrHighWaterMarks, name)
		}
		if name == hwmBackfill {
			m, ok := decodeMark(v.Bin)
			if !ok {
				return HWM{}, fmt.Errorf("%s entry %q is not a unit hash and a sequence number", catalog.AttrHighWaterMarks, name)
			}
			h.Backfill = &m
			continue
		}
		seq, ok := decodeSeq(v.Bin)
		if _, valid := decodeBase32(name, unitHashLen); !valid || !ok {
			return HWM{}, fmt.Errorf("%s entry %q is not a unit key and a sequence number", catalog.AttrHighWaterMarks, name)
		}
		h.Units[name] = seq
	}
	return h, nil
}

// Encode returns the _hwm attribute. It fails for a unit key that [UnitKey] doesn't return
// or a sequence number that [PadSeq] doesn't, such as a unit ID in place of its key.
func (h HWM) Encode() (store.Value, error) {
	m := make(map[string]store.Value, len(h.Units)+1)
	for uk, seq := range h.Units {
		if _, ok := decodeBase32(uk, unitHashLen); !ok || !validSeq(seq) {
			return store.Value{}, fmt.Errorf("%s entry %q: %q is not a unit key and a sequence number", catalog.AttrHighWaterMarks, uk, seq)
		}
		m[uk] = store.Value{Type: store.B, Bin: encodeSeq(nil, seq)}
	}
	if h.Backfill != nil {
		hash, ok := decodeBase32(h.Backfill.UnitKey, unitHashLen)
		if !ok || !validSeq(h.Backfill.Seq) {
			return store.Value{}, fmt.Errorf("backfill mark %+v is not a unit key and a sequence number", *h.Backfill)
		}
		m[hwmBackfill] = store.Value{Type: store.B, Bin: encodeSeq(hash, h.Backfill.Seq)}
	}
	return store.Value{Type: store.M, Map: m}, nil
}

// decodeMark reads the 8-byte unit hash and the sequence number that follow it.
func decodeMark(b []byte) (Mark, bool) {
	if len(b) <= unitHashLen {
		return Mark{}, false
	}
	seq, ok := decodeSeq(b[unitHashLen:])
	if !ok {
		return Mark{}, false
	}
	return Mark{UnitKey: string(appendBase32(nil, b[:unitHashLen])), Seq: seq}, true
}

// encodeSeq appends a padded sequence number as an unsigned big-endian integer without
// leading zero bytes (encoding.md §5.3).
func encodeSeq(dst []byte, seq string) []byte {
	n, _ := new(big.Int).SetString(seq, decimalBase)
	return append(dst, n.Bytes()...)
}

// decodeSeq accepts only what encodeSeq writes: at least one byte, no leading zero byte, and
// a number of at most 40 digits.
func decodeSeq(b []byte) (string, bool) {
	if len(b) == 0 || b[0] == 0 {
		return "", false
	}
	digits := new(big.Int).SetBytes(b).String()
	if len(digits) > catalog.SequenceWidth {
		return "", false
	}
	return padded(digits, catalog.SequenceWidth), true
}

// validSeq reports whether seq is in [PadSeq]'s form.
func validSeq(seq string) bool {
	return len(seq) == catalog.SequenceWidth && isDigits(seq) && strings.Trim(seq, "0") != ""
}
