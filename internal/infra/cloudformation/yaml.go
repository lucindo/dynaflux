// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"bytes"
	"maps"
	"slices"
	"strconv"

	"go.yaml.in/yaml/v4"
)

const indent = 2

// dump writes a template with sequences inside their parent's indent and no line folding,
// so a long value such as a pattern stays on one line.
func dump(doc *yaml.Node) ([]byte, error) {
	var buf bytes.Buffer
	d, err := yaml.NewDumper(&buf, yaml.WithIndent(indent), yaml.WithCompactSeqIndent(), yaml.WithLineWidth(-1),
		yaml.WithQuotePreference(yaml.QuoteDouble))
	if err != nil {
		return nil, err
	}
	if err = d.Dump(doc); err != nil {
		return nil, err
	}
	if err = d.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// pair is a key and its value in a mapping; a nil value leaves the key out.
type pair struct {
	key   string
	value *yaml.Node
}

func kv(key string, value *yaml.Node) pair {
	return pair{key, value}
}

// mapping returns the pairs in order, without those whose value is nil.
func mapping(pairs ...pair) *yaml.Node {
	n := &yaml.Node{Kind: yaml.MappingNode}
	for _, p := range pairs {
		if p.value != nil {
			n.Content = append(n.Content, str(p.key), p.value)
		}
	}
	return n
}

func seq(items ...*yaml.Node) *yaml.Node {
	return &yaml.Node{Kind: yaml.SequenceNode, Content: items}
}

func str(s string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: s}
}

func num(i int) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.Itoa(i)}
}

func boolean(b bool) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(b)}
}

// intrinsic returns a short-form intrinsic function, such as !Ref Prefix.
func intrinsic(fn string, arg *yaml.Node) *yaml.Node {
	n := *arg
	n.Tag = "!" + fn
	return &n
}

// tags returns CloudFormation's list form of tags, sorted by key.
func tags(t map[string]string) *yaml.Node {
	list := seq()
	for _, k := range slices.Sorted(maps.Keys(t)) {
		list.Content = append(list.Content, mapping(kv("Key", str(k)), kv("Value", str(t[k]))))
	}
	return list
}

// tagMap returns the map form of tags that SAM and API Gateway v2 take, sorted by key.
func tagMap(t map[string]string) *yaml.Node {
	m := mapping()
	for _, k := range slices.Sorted(maps.Keys(t)) {
		m.Content = append(m.Content, str(k), str(t[k]))
	}
	return m
}
