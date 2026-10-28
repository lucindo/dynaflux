// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/valuetext"
)

// unknownKeyError is a key of a JSON object that isn't one of the names asked for.
type unknownKeyError struct{ key string }

func (e *unknownKeyError) Error() string { return fmt.Sprintf("unknown key %q", e.key) }

// object reads data, one JSON object whose keys are among names, and returns each name's
// value in names' order, nil where it is absent. Empty data is the empty object. It stops at
// the first key that isn't one of names, an *unknownKeyError, or that appears twice:
// [json.Unmarshal] would keep the last value, and the caller may have meant the first. So it
// reads at most len(names)+1 keys, however many data has. On an error, the values are those
// read before it.
func object(data []byte, names []string) ([]json.RawMessage, error) {
	vals := make([]json.RawMessage, len(names))
	if len(bytes.TrimSpace(data)) == 0 {
		return vals, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return vals, errors.New("not a JSON object")
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return vals, err
		}
		key, ok := tok.(string)
		if !ok {
			return vals, errors.New("not a JSON object")
		}
		i := slices.Index(names, key)
		switch {
		case i < 0:
			return vals, &unknownKeyError{key}
		case vals[i] != nil:
			return vals, fmt.Errorf("key %q appears twice", key)
		}
		if err = dec.Decode(&vals[i]); errors.Is(err, io.EOF) {
			return vals, io.ErrUnexpectedEOF
		} else if err != nil {
			return vals, err
		}
	}
	if _, err := dec.Token(); err != nil {
		return vals, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return vals, errors.New("more than one JSON value")
	}
	return vals, nil
}

// bind returns the values of q's parameters from raw, their JSON values in q's order. Each
// has the JSON form of its type that valuetext reads, never null: parameters aren't Nullable.
func bind(q *catalog.Query, raw []json.RawMessage) ([]expr.Value, error) {
	vals := make([]expr.Value, len(q.Params))
	var missing []string
	for i, p := range q.Params {
		var err error
		switch {
		case raw[i] == nil:
			missing = append(missing, p.Name+" ("+p.Type.String()+")")
		case string(raw[i]) == "null":
			return nil, fmt.Errorf("parameter %s is null; give a %s", p.Name, p.Type)
		default:
			if vals[i], err = valuetext.ParseJSON(p.Type, raw[i]); err != nil {
				return nil, fmt.Errorf("parameter %s: %w", p.Name, err)
			}
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s needs %s", q.Name, strings.Join(missing, ", "))
	}
	return vals, nil
}
