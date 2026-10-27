// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"fmt"
)

// JSONValue is an attribute value in DynamoDB's JSON form, as stream events carry it and the
// backfill stages it. N keeps the text as sent. NULL and sets, which Dynaflux never writes,
// decode to the zero [Value], which encodes as {}. Empty B, L and M keep their type.
type JSONValue struct {
	S    *string               `json:"S,omitempty"`
	N    *string               `json:"N,omitempty"`
	B    *[]byte               `json:"B,omitempty"`
	BOOL *bool                 `json:"BOOL,omitempty"`
	L    *[]JSONValue          `json:"L,omitempty"`
	M    *map[string]JSONValue `json:"M,omitempty"`
}

// EncodeJSON returns it in DynamoDB's JSON form.
func EncodeJSON(it Item) map[string]JSONValue {
	out := make(map[string]JSONValue, len(it))
	for k, v := range it {
		out[k] = encodeJSON(v)
	}
	return out
}

func encodeJSON(v Value) JSONValue {
	switch v.Type {
	case S:
		return JSONValue{S: &v.Str}
	case N:
		return JSONValue{N: &v.Str}
	case B:
		return JSONValue{B: &v.Bin}
	case BOOL:
		return JSONValue{BOOL: &v.Bool}
	case L:
		l := make([]JSONValue, len(v.List))
		for i, e := range v.List {
			l[i] = encodeJSON(e)
		}
		return JSONValue{L: &l}
	case M:
		m := EncodeJSON(v.Map)
		return JSONValue{M: &m}
	}
	return JSONValue{}
}

// DecodeJSON returns the item that m holds in DynamoDB's JSON form. An attribute of two types
// fails.
func DecodeJSON(m map[string]JSONValue) (Item, error) {
	it := make(Item, len(m))
	for k, a := range m {
		v, err := a.value()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", k, err)
		}
		it[k] = v
	}
	return it, nil
}

func (a JSONValue) value() (Value, error) {
	var v Value
	set := 0
	if a.S != nil {
		v, set = Value{Type: S, Str: *a.S}, set+1
	}
	if a.N != nil {
		v, set = Value{Type: N, Str: *a.N}, set+1
	}
	if a.B != nil {
		v, set = Value{Type: B, Bin: *a.B}, set+1
	}
	if a.BOOL != nil {
		v, set = Value{Type: BOOL, Bool: *a.BOOL}, set+1
	}
	if a.L != nil {
		l := make([]Value, len(*a.L))
		for i, e := range *a.L {
			var err error
			if l[i], err = e.value(); err != nil {
				return Value{}, err
			}
		}
		v, set = Value{Type: L, List: l}, set+1
	}
	if a.M != nil {
		m, err := DecodeJSON(*a.M)
		if err != nil {
			return Value{}, err
		}
		v, set = Value{Type: M, Map: m}, set+1
	}
	if set > 1 {
		return Value{}, errors.New("an attribute of two types")
	}
	return v, nil
}
