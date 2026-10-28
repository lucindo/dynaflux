// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux_test

import (
	"testing"

	"github.com/lucindo/dynaflux"
)

// ParseToken takes "" as the zero Token and refuses text that no append returned.
func TestParseToken(t *testing.T) {
	t.Parallel()
	if tok, err := dynaflux.ParseToken(""); err != nil || !tok.IsZero() {
		t.Errorf(`ParseToken("") = %q, %v; want the zero Token`, tok, err)
	}
	for _, text := range []string{"not base64!", "AQ", "AglzdHJlYW0"} {
		_, err := dynaflux.ParseToken(text)
		if want := "dynaflux: malformed token: not the text of a token that an append returned"; err == nil || err.Error() != want {
			t.Errorf("ParseToken(%q): %v, want %q", text, err, want)
		}
	}
}
