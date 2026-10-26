// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package txtar_test

import (
	"reflect"
	"testing"

	"github.com/lucindo/dynaflux/internal/txtar"
)

func TestParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want *txtar.Archive
	}{
		{
			name: "empty",
			in:   "",
			want: &txtar.Archive{},
		},
		{
			name: "comment only, newline added",
			in:   "just a comment",
			want: &txtar.Archive{Comment: []byte("just a comment\n")},
		},
		{
			name: "comment and files",
			in:   "about\n-- a.sql --\nSELECT 1;\n-- want --\n",
			want: &txtar.Archive{
				Comment: []byte("about\n"),
				Files: []txtar.File{
					{Name: "a.sql", Data: []byte("SELECT 1;\n")},
					{Name: "want"},
				},
			},
		},
		{
			name: "last file gets a newline",
			in:   "-- a --\nx",
			want: &txtar.Archive{Files: []txtar.File{{Name: "a", Data: []byte("x\n")}}},
		},
		{
			name: "marker at end of input",
			in:   "-- a --",
			want: &txtar.Archive{Files: []txtar.File{{Name: "a"}}},
		},
		{
			name: "name is trimmed",
			in:   "--   dir/a b.sql   --\n",
			want: &txtar.Archive{Files: []txtar.File{{Name: "dir/a b.sql"}}},
		},
		{
			name: "lines that are not markers",
			in: "-- a --\n" +
				"-- name: PageStats :many\n" + // no closing " --"
				" -- b --\n" + // not at the start of the line
				"--  --\n" + // empty name
				"-- --\n" + // too short
				"-- c --\r\n", // \r before the newline
			want: &txtar.Archive{Files: []txtar.File{{
				Name: "a",
				Data: []byte("-- name: PageStats :many\n -- b --\n--  --\n-- --\n-- c --\r\n"),
			}}},
		},
		{
			name: "bytes that aren't UTF-8 are kept",
			in:   "-- a --\ncaf\xe9\n",
			want: &txtar.Archive{Files: []txtar.File{{Name: "a", Data: []byte("caf\xe9\n")}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, want := normalize(txtar.Parse([]byte(tt.in))), normalize(tt.want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Parse(%q) = %q, want %q", tt.in, got, want)
			}
		})
	}
}

func TestFormat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   *txtar.Archive
		want string
	}{
		{
			name: "newlines added",
			in: &txtar.Archive{
				Comment: []byte("about"),
				Files: []txtar.File{
					{Name: "a.sql", Data: []byte("SELECT 1;")},
					{Name: "want"},
				},
			},
			want: "about\n-- a.sql --\nSELECT 1;\n-- want --\n",
		},
		{
			name: "no comment",
			in:   &txtar.Archive{Files: []txtar.File{{Name: "a", Data: []byte("x\n")}}},
			want: "-- a --\nx\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := string(txtar.Format(tt.in)); got != tt.want {
				t.Errorf("Format() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatDoesNotModifyInput(t *testing.T) {
	t.Parallel()
	buf := []byte("xy")
	a := &txtar.Archive{Comment: buf[:1]}
	txtar.Format(a)
	if string(buf) != "xy" {
		t.Errorf("Format changed the comment's backing array to %q", buf)
	}
}

func TestParseFormatRoundTrip(t *testing.T) {
	t.Parallel()
	in := "comment\n-- dynaflux.yaml --\nstore: analytics\n-- dynaflux/a.sql --\n-- name: X :one\n-- want --\n"
	if got := string(txtar.Format(txtar.Parse([]byte(in)))); got != in {
		t.Errorf("Format(Parse(in)) = %q, want %q", got, in)
	}
}

// normalize makes nil and empty byte slices equal, which Parse doesn't promise to tell apart.
func normalize(a *txtar.Archive) txtar.Archive {
	n := txtar.Archive{Comment: nonNil(a.Comment)}
	for _, f := range a.Files {
		n.Files = append(n.Files, txtar.File{Name: f.Name, Data: nonNil(f.Data)})
	}
	return n
}

func nonNil(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
