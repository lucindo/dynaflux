// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package txtar reads and writes txtar archives, with the semantics of
// golang.org/x/tools/txtar: a free-form comment, then files that each start
// with a marker line "-- name --".
package txtar

import (
	"bytes"
	"slices"
	"strings"
)

// Archive is a parsed txtar archive.
type Archive struct {
	Comment []byte
	Files   []File
}

// File is one file of an archive.
type File struct {
	Name string
	Data []byte
}

// Parse splits data into its comment and files. It never fails: text before
// the first marker line is the comment, and a file runs to the next marker
// line. A non-empty comment or file gets a final newline if it lacks one.
func Parse(data []byte) *Archive {
	a := new(Archive)
	var name string
	a.Comment, name, data = nextFile(data)
	for name != "" {
		f := File{Name: name}
		f.Data, name, data = nextFile(data)
		a.Files = append(a.Files, f)
	}
	return a
}

// Format returns the text of a, adding a final newline to each non-empty comment or file
// that lacks one. Parse reads the text back as a, with those newlines, when no comment or
// file holds a marker line and every name is non-empty, has no newline, and has no white
// space at either end.
func Format(a *Archive) []byte {
	var b bytes.Buffer
	b.Write(withNewline(a.Comment))
	for _, f := range a.Files {
		b.WriteString("-- " + f.Name + " --\n")
		b.Write(withNewline(f.Data))
	}
	return b.Bytes()
}

// nextFile splits data at its first marker line into the text before it, the
// marker's file name and the text after it. Without a marker, the name is "".
func nextFile(data []byte) ([]byte, string, []byte) {
	for i := 0; i < len(data); {
		line, next := data[i:], len(data)
		if j := bytes.IndexByte(line, '\n'); j >= 0 {
			line, next = line[:j], i+j+1
		}
		if n, ok := markerName(line); ok {
			return data[:i], n, data[next:]
		}
		i = next
	}
	return withNewline(data), "", nil
}

func markerName(line []byte) (string, bool) {
	const prefix, suffix = "-- ", " --"
	if len(line) < len(prefix)+len(suffix) ||
		!bytes.HasPrefix(line, []byte(prefix)) || !bytes.HasSuffix(line, []byte(suffix)) {
		return "", false
	}
	name := strings.TrimSpace(string(line[len(prefix) : len(line)-len(suffix)]))
	return name, name != ""
}

func withNewline(data []byte) []byte {
	if len(data) == 0 || data[len(data)-1] == '\n' {
		return data
	}
	// Clip makes append copy, so the caller's data stays untouched.
	return append(slices.Clip(data), '\n')
}
