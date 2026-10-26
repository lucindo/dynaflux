// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"fmt"
	"slices"
	"strings"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
)

// prefixUnderscore is the !Sub variable that ${prefix_} becomes.
const prefixUnderscore = "PrefixUnderscore"

// resolver turns the texts of one template into CloudFormation values. A reference to a
// resource of the template becomes !Ref or !GetAtt, so CloudFormation orders the two. A
// regional template refers across scopes to stream tables, whose names and ARNs derive from
// their names.
type resolver struct {
	local       map[string]bool        // IDs of the resources in the template
	global      map[string]infra.Table // stream tables, by ID, for a regional template
	archiveOnly map[string]bool        // IDs of the archive-only resources
	// err is the first text that failed to resolve, which fails the template.
	err error
}

// value returns t as a plain string, as !Ref or !GetAtt when it is one substitution, or as
// !Sub. ${prefix_} has no pseudo parameter, so it is a !Sub variable: the prefix split at "-"
// and joined with "_". When t doesn't resolve, value records why in r.err.
func (r *resolver) value(t infra.Text) *yaml.Node {
	n, err := r.resolve(t)
	if err != nil {
		r.fail(fmt.Errorf("%q: %w", t, err))
		return str("")
	}
	return n
}

// fail records err as the template's error, unless it has one.
func (r *resolver) fail(err error) {
	if r.err == nil {
		r.err = err
	}
}

func (r *resolver) resolve(t infra.Text) (*yaml.Node, error) {
	segs, err := t.Segments()
	if err != nil {
		return nil, err
	}
	if len(segs) == 1 {
		switch s := segs[0]; {
		case s.Placeholder != "" && s.Placeholder != infra.PlaceholderPrefixUnderscore:
			return intrinsic("Ref", str(parameter(s.Placeholder))), nil
		case s.Ref.ID != "" && r.local[s.Ref.ID]:
			suffix, attrErr := localAttr(s.Ref)
			switch {
			case attrErr != nil:
				return nil, attrErr
			case suffix != "":
				return intrinsic("GetAtt", str(s.Ref.ID+suffix)), nil
			}
			return intrinsic("Ref", str(s.Ref.ID)), nil
		}
	}
	var b strings.Builder
	subs, underscore, err := r.write(&b, segs)
	if err != nil {
		return nil, err
	}
	switch {
	case underscore:
		joined := intrinsic("Join", seq(str("_"), intrinsic("Split", seq(str("-"), intrinsic("Ref", str("Prefix"))))))
		// One line per use keeps the many uses of the database name readable.
		variables := mapping(kv(prefixUnderscore, joined))
		variables.Style = yaml.FlowStyle
		return intrinsic("Sub", seq(str(b.String()), variables)), nil
	case subs:
		return intrinsic("Sub", str(b.String())), nil
	}
	return str(b.String()), nil
}

// optional returns the value of t, which exists only where the resources it refers to do:
// only in the regions that archive when it refers to an archive-only resource.
func (r *resolver) optional(t infra.Text) *yaml.Node {
	v := r.value(t)
	// value has recorded a t that doesn't parse.
	segs, err := t.Segments()
	if err == nil && slices.ContainsFunc(segs, func(s infra.Segment) bool { return r.archiveOnly[s.Ref.ID] }) {
		return ifArchive(v)
	}
	return v
}

// write writes segs to b in !Sub syntax, and reports whether it wrote a substitution and
// whether one was ${prefix_}. A literal can't hold "${", which would be a substitution.
func (r *resolver) write(b *strings.Builder, segs []infra.Segment) (bool, bool, error) {
	subs, underscore := false, false
	for _, s := range segs {
		switch {
		case s.Literal != "":
			b.WriteString(s.Literal)
			continue
		case s.Placeholder != "":
			underscore = underscore || s.Placeholder == infra.PlaceholderPrefixUnderscore
			b.WriteString("${" + parameter(s.Placeholder) + "}")
		case r.local[s.Ref.ID]:
			suffix, err := localAttr(s.Ref)
			if err != nil {
				return false, false, err
			}
			b.WriteString("${" + s.Ref.ID + suffix + "}")
		default:
			u, refErr := r.writeGlobal(b, s.Ref)
			if refErr != nil {
				return false, false, refErr
			}
			underscore = underscore || u
		}
		subs = true
	}
	return subs, underscore, nil
}

// localAttr returns what follows the ID of a resource in the template in !GetAtt or !Sub:
// nothing for the value of !Ref, which is a name or a mapping's UUID. The custom resources'
// attributes are those that package setup returns.
func localAttr(ref infra.Ref) (string, error) {
	switch ref.Attr {
	case infra.AttrName, infra.AttrUUID:
		return "", nil
	case infra.AttrARN:
		return ".Arn", nil
	case infra.AttrStreamARN:
		return ".StreamArn", nil
	case infra.AttrFingerprint:
		return ".Fingerprint", nil
	}
	return "", fmt.Errorf("refers to %s.%s, which a resource in the template can't provide", ref.ID, ref.Attr)
}

// writeGlobal writes a reference to a stream table of the global template, and reports
// whether it wrote ${prefix_}.
func (r *resolver) writeGlobal(b *strings.Builder, ref infra.Ref) (bool, error) {
	t, ok := r.global[ref.ID]
	if !ok {
		return false, fmt.Errorf("refers to %s, which is neither in the template nor a stream table", ref.ID)
	}
	name, err := t.Name.Segments()
	if err != nil {
		return false, err
	}
	switch ref.Attr {
	case infra.AttrName:
	case infra.AttrARN, infra.AttrStreamARNPattern:
		b.WriteString("arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/")
	default:
		return false, fmt.Errorf("refers to %s.%s, which a stream table doesn't provide", ref.ID, ref.Attr)
	}
	_, underscore, err := (&resolver{}).write(b, name)
	if ref.Attr == infra.AttrStreamARNPattern {
		b.WriteString("/stream/*")
	}
	return underscore, err
}

// parameter returns the template parameter, pseudo parameter or !Sub variable of a
// placeholder.
func parameter(placeholder string) string {
	switch placeholder {
	case infra.PlaceholderPrefix:
		return "Prefix"
	case infra.PlaceholderPrefixUnderscore:
		return prefixUnderscore
	case infra.PlaceholderRegion:
		return "AWS::Region"
	case infra.PlaceholderAccount:
		return "AWS::AccountId"
	default: // infra.PlaceholderPartition
		return "AWS::Partition"
	}
}
