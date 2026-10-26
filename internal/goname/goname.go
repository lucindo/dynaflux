// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package goname derives the Go names that dynaflux generate declares (sql.md §3.2).
// validate checks them for collisions (DF1207) and codegen declares them, so both use
// this package and can't disagree.
package goname

import "strings"

// Reserved returns the package-level names that every generated package declares,
// besides those of its streams and queries.
func Reserved() []string {
	return []string{"Catalog", "Client", "New"}
}

// Stream holds the names generated for one stream.
type Stream struct {
	Event       string // PageView
	Key         string // PageViewsKey
	Append      string // AppendPageView
	AppendBatch string // AppendPageViewBatch
	Read        string // ReadPageViews
	Import      string // ImportPageViews
}

// ForStream returns the names generated for the stream called name, whose -- go:type is
// goType, or "" without one.
func ForStream(name, goType string) Stream {
	event := goType
	if event == "" {
		event = Field(Singular(name))
	}
	plural := Field(name)
	return Stream{
		Event:       event,
		Key:         plural + "Key",
		Append:      "Append" + event,
		AppendBatch: "Append" + event + "Batch",
		Read:        "Read" + plural,
		Import:      "Import" + plural,
	}
}

// Query holds the names generated for one named query.
type Query struct {
	Method string // PageStats
	Params string // PageStatsParams
	Row    string // PageStatsRow
}

// ForQuery returns the names generated for the query whose -- name: is name.
func ForQuery(name string) Query {
	return Query{Method: name, Params: name + "Params", Row: name + "Row"}
}

// Field turns a snake_case name into a Go name with Go's initialisms, as sqlc does:
// user_id becomes UserID.
func Field(name string) string {
	var b strings.Builder
	for part := range strings.SplitSeq(name, "_") {
		if part == "" {
			continue
		}
		if upper := strings.ToUpper(part); isInitialism(upper) {
			b.WriteString(upper)
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]) + part[1:])
	}
	return b.String()
}

// isInitialism reports whether s is one of the initialisms Go names keep in capitals:
// golint's list, plus MS for milliseconds.
func isInitialism(s string) bool {
	switch s {
	case "ACL", "API", "ASCII", "CPU", "CSS", "DNS", "EOF", "GUID", "HTML", "HTTP", "HTTPS", "ID", "IP", "JSON",
		"LHS", "MS", "QPS", "RAM", "RHS", "RPC", "SLA", "SMTP", "SQL", "SSH", "TCP", "TLS", "TTL", "UDP", "UI",
		"UID", "UUID", "URI", "URL", "UTF8", "VM", "XML", "XMPP", "XSRF", "XSS":
		return true
	}
	return false
}

// Singular returns a snake_case name with its last word in the singular: page_views
// becomes page_view, and categories becomes category.
func Singular(name string) string {
	i := strings.LastIndexByte(name, '_') + 1
	head, word := name[:i], name[i:]
	switch {
	case strings.HasSuffix(word, "ies") && len(word) > len("ies"):
		word = strings.TrimSuffix(word, "ies") + "y"
	case strings.HasSuffix(word, "sses"), strings.HasSuffix(word, "shes"), strings.HasSuffix(word, "ches"),
		strings.HasSuffix(word, "xes"), strings.HasSuffix(word, "zes"):
		word = strings.TrimSuffix(word, "es")
	case strings.HasSuffix(word, "ss"), strings.HasSuffix(word, "us"), strings.HasSuffix(word, "is"):
	case strings.HasSuffix(word, "s") && len(word) > len("s"):
		word = strings.TrimSuffix(word, "s")
	}
	return head + word
}
