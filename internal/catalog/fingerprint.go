// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

// fingerprintScheme versions the identity projection. Changing it, or the JSON form of
// anything in a view version, changes every fingerprint and so renumbers deployed views.
const fingerprintScheme = 1

// Fingerprint returns the identity of a view version as lowercase hex SHA-256: a hash of
// everything that determines its stored data. It ignores Number, State, Fingerprint and
// Mode, and the order of the aggregates. Equal fingerprints share a version number and a
// table.
//
// The hash is over the JSON {"scheme":1,"version":V}, where V is the version's JSON form
// without the ignored fields. Object keys are sorted, there is no insignificant
// whitespace, and numbers are written as encoding/json writes a Go int or float64. Strings
// escape only '"' and '\' (as \" and \\), the characters below U+0020 (as \b, \f, \n, \r
// and \t, or else \u00xx in lowercase hex), and U+2028 and U+2029 (as \u2028 and \u2029).
func Fingerprint(v *ViewVersion) (string, error) {
	if v == nil {
		return "", errors.New("fingerprint: view version is nil")
	}
	identity := *v
	identity.Aggregates = sortedAggregates(v.Aggregates)
	raw, err := json.Marshal(&identity)
	if err != nil {
		return "", fmt.Errorf("fingerprint: %w", err)
	}
	// Decoding into maps sorts the keys when re-encoded, so reordering struct fields never
	// changes a fingerprint. Numbers keep their exact text.
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var fields map[string]any
	if err = dec.Decode(&fields); err != nil {
		return "", fmt.Errorf("fingerprint: %w", err)
	}
	for _, name := range []string{"number", "state", "fingerprint", "mode"} {
		delete(fields, name)
	}
	canon, err := marshalJSON(map[string]any{"scheme": fingerprintScheme, "version": fields})
	if err != nil {
		return "", fmt.Errorf("fingerprint: %w", err)
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}
