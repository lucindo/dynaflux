// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package engine

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/keys"
	"github.com/lucindo/dynaflux/internal/store"
)

// encode checks v, the value of column name of type t, against sql.md §4.3 and returns its
// attribute; present is false for NULL. A failure is a *ValueError.
func encode(name string, t catalog.Type, v expr.Value) (store.Value, bool, error) {
	if err := check(t, v); err != nil {
		return store.Value{}, false, invalid(name, err)
	}
	a, present, err := keys.EncodeValue(t, v)
	if err != nil {
		return store.Value{}, false, invalid(name, err)
	}
	return a, present, nil
}

// check checks what keys.EncodeValue doesn't: UTF-8 and array length.
func check(t catalog.Type, v expr.Value) error {
	if !t.Array {
		return checkScalar(t.Kind, v)
	}
	elems := v.AsArray()
	if len(elems) > catalog.MaxArrayLen {
		return fmt.Errorf("%d elements, over %d", len(elems), catalog.MaxArrayLen)
	}
	for i, e := range elems {
		if err := checkScalar(t.Kind, e); err != nil {
			return fmt.Errorf("element %d: %w", i+1, err)
		}
	}
	return nil
}

func checkScalar(k catalog.Kind, v expr.Value) error {
	if k == catalog.KindString && !utf8.ValidString(v.AsString()) {
		return errors.New("not valid UTF-8")
	}
	return nil
}
