// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package valuetext

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
)

// AppendJSON appends the JSON form of v, a value of type t.
func AppendJSON(dst []byte, t catalog.Type, v expr.Value) []byte {
	if v.IsNull() {
		return append(dst, "null"...)
	}
	if !t.Array {
		return appendJSONScalar(dst, t, v)
	}
	elem := scalar(t)
	dst = append(dst, '[')
	for i, e := range v.AsArray() {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendJSONScalar(dst, elem, e)
	}
	return append(dst, ']')
}

// ParseJSON parses data, one JSON value of type t.
func ParseJSON(t catalog.Type, data []byte) (expr.Value, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	// A RawMessage checks the syntax, with encoding/json's errors, without decoding elements.
	var raw json.RawMessage
	err := dec.Decode(&raw)
	if err == nil {
		if _, end := dec.Token(); !errors.Is(end, io.EOF) {
			err = errors.New("more than one JSON value")
		}
	}
	var x any
	if err == nil {
		x, err = decode(t, raw)
	}
	var v expr.Value
	if err == nil {
		v, err = fromJSON(t, x)
	}
	if err == nil {
		err = fits(t, v)
	}
	if err != nil {
		return expr.Value{}, fmt.Errorf("invalid %s: %w", t, err)
	}
	return v, nil
}

// decode decodes raw, one well-formed JSON value, with numbers as [json.Number]. It stops in
// an array of t at its first element past catalog.MaxArrayLen, so that refusing a long one
// costs no more than a full one.
func decode(t catalog.Type, raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if !t.Array || raw[0] != '[' {
		var x any
		return x, dec.Decode(&x)
	}
	if _, err := dec.Token(); err != nil { // the '['
		return nil, err
	}
	list := []any{}
	for dec.More() {
		if len(list) == catalog.MaxArrayLen {
			return nil, fmt.Errorf("more than %d elements", catalog.MaxArrayLen)
		}
		var e any
		if err := dec.Decode(&e); err != nil {
			return nil, err
		}
		list = append(list, e)
	}
	return list, nil
}

func appendJSONScalar(dst []byte, t catalog.Type, v expr.Value) []byte {
	switch t.Kind {
	case catalog.KindString:
		return appendJSONString(dst, v.AsString())
	case catalog.KindUInt8, catalog.KindUInt16, catalog.KindUInt32, catalog.KindInt8, catalog.KindInt16,
		catalog.KindInt32, catalog.KindFloat64, catalog.KindBool:
		return appendScalar(dst, t, v)
	case catalog.KindUInt64, catalog.KindInt64, catalog.KindDecimal, catalog.KindUUID, catalog.KindDate,
		catalog.KindDateTime, catalog.KindDateTime64:
		return append(appendScalar(append(dst, '"'), t, v), '"')
	}
	return dst
}

// appendJSONString writes s, valid UTF-8, as a JSON string without encoding/json's HTML
// escapes, which would turn the & of a URL into six bytes.
func appendJSONString(dst []byte, s string) []byte {
	const hexDigits = "0123456789abcdef"
	dst = append(dst, '"')
	for _, r := range s {
		switch {
		case r == '"' || r == '\\':
			dst = append(dst, '\\', byte(r))
		case r == '\n':
			dst = append(dst, `\n`...)
		case r == '\t':
			dst = append(dst, `\t`...)
		case r < ' ':
			dst = append(dst, `\u00`...)
			dst = append(dst, hexDigits[r>>4], hexDigits[r&0xF])
		default:
			dst = utf8.AppendRune(dst, r)
		}
	}
	return append(dst, '"')
}

// fromJSON converts x, a value that encoding/json decoded with UseNumber, to a value of t.
func fromJSON(t catalog.Type, x any) (expr.Value, error) {
	if x == nil {
		if !t.Nullable {
			return expr.Value{}, errors.New("not Nullable")
		}
		return expr.Null(), nil
	}
	if !t.Array {
		return scalarFromJSON(t, x)
	}
	list, ok := x.([]any)
	if !ok {
		return expr.Value{}, errors.New("want a JSON array")
	}
	elem := scalar(t)
	vals := make([]expr.Value, len(list))
	for i, e := range list {
		v, err := scalarFromJSON(elem, e)
		if err != nil {
			return expr.Value{}, fmt.Errorf("element %d: %w", i+1, err)
		}
		vals[i] = v
	}
	return expr.Array(vals), nil
}

// scalarFromJSON takes integers and decimals as numbers or strings, Float64 as a number,
// Bool as a boolean, and the other kinds as strings.
func scalarFromJSON(t catalog.Type, x any) (expr.Value, error) {
	k := expr.KindOf(t.Kind)
	numeric := k == expr.KindInt || k == expr.KindUint || k == expr.KindDecimal
	switch x := x.(type) {
	case bool:
		if k == expr.KindBool {
			return expr.Bool(x), nil
		}
	case json.Number:
		if numeric || k == expr.KindFloat {
			return parseScalar(t, string(x), true)
		}
	case string:
		if k != expr.KindBool && k != expr.KindFloat {
			return parseScalar(t, x, false)
		}
	}
	switch {
	case k == expr.KindBool:
		return expr.Value{}, errors.New("want true or false")
	case k == expr.KindFloat:
		return expr.Value{}, errors.New("want a JSON number")
	case numeric:
		return expr.Value{}, errors.New("want a JSON number or string")
	default:
		return expr.Value{}, errors.New("want a JSON string")
	}
}
