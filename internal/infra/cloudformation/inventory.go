// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"errors"
	"fmt"
	"slices"
	"strconv"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
)

// Inventory is what dynaflux generate compares between the templates it wrote last time and
// the ones it writes now.
type Inventory struct {
	// Regions are the replica regions of the stream tables in global.yaml, sorted.
	Regions []string
	// Protected are the regions of Regions where a stream table's replica has deletion
	// protection.
	Protected []string
	// Views are the view version tables in regional.yaml, by their tags.
	Views []ViewTable
}

// ViewTable is the table of a view version.
type ViewTable struct {
	View        string
	Version     int
	Fingerprint string
}

// ReadInventory reads templates that [Render] wrote. Either may be nil, for a template that
// doesn't exist.
func ReadInventory(global, regional []byte) (Inventory, error) {
	var inv Inventory
	err := eachResource(global, func(typ string, props *yaml.Node) error {
		if typ != "AWS::DynamoDB::GlobalTable" {
			return nil
		}
		for _, r := range mappingValueOrEmpty(props, "Replicas").Content {
			region := mappingValueOrEmpty(r, "Region").Value
			inv.Regions = append(inv.Regions, region)
			if mappingValueOrEmpty(r, "DeletionProtectionEnabled").Value == "true" {
				inv.Protected = append(inv.Protected, region)
			}
		}
		return nil
	})
	if err != nil {
		return Inventory{}, fmt.Errorf("%s: %w", GlobalTemplate, err)
	}
	inv.Regions = slices.Compact(slices.Sorted(slices.Values(inv.Regions)))
	inv.Protected = slices.Compact(slices.Sorted(slices.Values(inv.Protected)))
	err = eachResource(regional, func(typ string, props *yaml.Node) error {
		tags := map[string]string{}
		for _, t := range mappingValueOrEmpty(props, "Tags").Content {
			tags[mappingValueOrEmpty(t, "Key").Value] = mappingValueOrEmpty(t, "Value").Value
		}
		if typ != "AWS::DynamoDB::Table" || tags[infra.TagKind] != infra.TagKindView {
			return nil
		}
		n, convErr := strconv.Atoi(tags[infra.TagVersion])
		if convErr != nil {
			return fmt.Errorf("a view table's %s tag is %q, not a number", infra.TagVersion, tags[infra.TagVersion])
		}
		inv.Views = append(inv.Views, ViewTable{View: tags[infra.TagName], Version: n, Fingerprint: tags[infra.TagFingerprint]})
		return nil
	})
	if err != nil {
		return Inventory{}, fmt.Errorf("%s: %w", RegionalTemplate, err)
	}
	return inv, nil
}

// eachResource calls fn with the type and properties of each resource of a template.
func eachResource(data []byte, fn func(typ string, props *yaml.Node) error) error {
	if data == nil {
		return nil
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return errors.New("not a template")
	}
	resources := mappingValueOrEmpty(doc.Content[0], "Resources")
	for i := 1; i < len(resources.Content); i += 2 {
		r := resources.Content[i]
		if err := fn(mappingValueOrEmpty(r, "Type").Value, mappingValueOrEmpty(r, "Properties")); err != nil {
			return fmt.Errorf("%s: %w", resources.Content[i-1].Value, err)
		}
	}
	return nil
}

// mappingValueOrEmpty returns the value of key in mapping n, or an empty node.
func mappingValueOrEmpty(n *yaml.Node, key string) *yaml.Node {
	if v := mappingValue(n, key); v != nil {
		return v
	}
	return &yaml.Node{}
}
