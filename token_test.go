// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux_test

import (
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/fluxtest"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
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

// A Token in a JSON document is its text, "" for the zero Token, and decodes back to the
// same Token; text that no append returned fails to decode.
func TestTokenJSON(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		cat := fluxgen.LoadCatalog(string(catalogtest.Marshal(t, "app", "CREATE STREAM visits (page String) PRIMARY KEY (page);")))
		var (
			page, id, region = "/home", "", ""
			at, ingest       time.Time
		)
		tok, err := fluxgen.AppendEvent(t.Context(), fluxtest.NewStore(t, cat), cat, "visits", []any{&page, &id, &at, &ingest, &region})
		if err != nil {
			t.Fatal(err)
		}
		type receipt struct {
			Token dynaflux.Token `json:"token"`
			None  dynaflux.Token `json:"none"`
		}
		data, err := json.Marshal(receipt{Token: tok})
		if want := `{"token":"` + tok.String() + `","none":""}`; err != nil || string(data) != want {
			t.Fatalf("json.Marshal = %s, %v; want %s", data, err, want)
		}
		var got receipt
		if err = json.Unmarshal(data, &got); err != nil || got != (receipt{Token: tok}) {
			t.Errorf("json.Unmarshal(%s) = %+v, %v; want the token back and the zero Token", data, got, err)
		}
		err = json.Unmarshal([]byte(`{"token":"AQ"}`), &got)
		if want := "dynaflux: malformed token: not the text of a token that an append returned"; err == nil || err.Error() != want {
			t.Errorf("json.Unmarshal of a malformed token: %v, want %q", err, want)
		}
	})
}
