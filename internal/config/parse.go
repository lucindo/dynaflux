// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/diag"
)

const (
	hintOneMapping = "keep one document of key: value lines"
	hintFixYAML    = "fix the YAML at the position shown"
	hintFullValues = "write each value out in full; quote a string instead of tagging it"
)

// parse reads src into a node tree and reports what makes it unusable: syntax errors
// (DF6003), anything but one mapping document (DF6004), duplicate keys (DF6006), and
// anchors, aliases, merge keys and tags (DF6014). It returns the top-level mapping.
func parse(src []byte) (*yaml.Node, []diag.Diagnostic) {
	var d decoder
	// Duplicates are reported as DF6006 below, not as syntax errors.
	l, err := yaml.NewLoader(bytes.NewReader(src), yaml.WithUniqueKeys(false))
	if err != nil {
		d.syntaxError(err)
		return nil, d.diags
	}
	var doc, next yaml.Node
	err = l.Load(&doc)
	switch {
	case errors.Is(err, io.EOF):
		d.errorf(pos(1, 1), "DF6004", hintOneMapping, "%s is empty", FileName)
		return nil, d.diags
	case err != nil:
		d.syntaxError(err)
		return nil, d.diags
	}
	err = l.Load(&next)
	switch {
	case err == nil:
		d.errorf(at(&next), "DF6004", hintOneMapping, "%s has more than one YAML document", FileName)
		return nil, d.diags
	case !errors.Is(err, io.EOF):
		d.syntaxError(err)
		return nil, d.diags
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		d.errorf(at(root), "DF6004", hintOneMapping, "the top level of %s is %s, not a mapping", FileName, describe(root))
		return nil, d.diags
	}
	d.plainTree(root)
	return root, d.diags
}

func (d *decoder) syntaxError(err error) {
	var le *yaml.LoadError
	if !errors.As(err, &le) {
		d.errorf(pos(1, 1), "DF6003", hintFixYAML, "invalid YAML: %v", err)
		return
	}
	msg := le.Message
	if le.ContextMsg != "" && le.ContextMark.Line > 0 {
		msg = fmt.Sprintf("%s %s at %d:%d", msg, le.ContextMsg, le.ContextMark.Line, le.ContextMark.Column)
	}
	p := pos(1, 1)
	if le.Mark.Line > 0 {
		p.Line, p.Col = le.Mark.Line, le.Mark.Column
	}
	d.errorf(p, "DF6003", hintFixYAML, "invalid YAML: %s", msg)
}

// plainTree reports duplicate keys, anchors, aliases, merge keys and explicit tags in n and
// below.
func (d *decoder) plainTree(n *yaml.Node) {
	switch {
	case n.Kind == yaml.AliasNode:
		d.errorf(at(n), "DF6014", hintFullValues, "alias *%s: aliases aren't allowed", n.Value)
		return
	case n.Anchor != "":
		d.errorf(at(n), "DF6014", hintFullValues, "anchor &%s: anchors aren't allowed", n.Anchor)
	case n.Style&yaml.TaggedStyle != 0 || n.Tag == "!":
		d.errorf(at(n), "DF6014", hintFullValues, "tag %s: explicit tags aren't allowed", n.Tag)
	}
	if n.Kind == yaml.MappingNode {
		firstLine := map[string]int{}
		for k := range pairs(n) {
			switch line, dup := firstLine[k.Value]; {
			case k.Tag == "!!merge":
				d.errorf(at(k), "DF6014", hintFullValues, "merge keys (<<) aren't allowed")
			case dup:
				d.errorf(at(k), "DF6006", "keep one", "duplicate key %s; the first is on line %d", k.Value, line)
			case k.Kind == yaml.ScalarNode:
				firstLine[k.Value] = k.Line
			}
		}
	}
	for _, c := range n.Content {
		d.plainTree(c)
	}
}
