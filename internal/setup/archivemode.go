// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"context"
	"errors"
	"time"

	"github.com/lucindo/dynaflux/internal/meta"
)

type archiveModeProps struct {
	MetaTable string                      `json:"MetaTable"`
	Streams   map[string]meta.ArchiveMode `json:"Streams"`
}

// applyArchiveModes answers [TypeArchiveMode] on Create and Update (ADR 0017 §2): it records
// each stream's mode in the region, and a hole for each change to or from all between the
// old properties and the new, so a rollback, an update with them swapped, records another.
// Its physical ID never changes, so CloudFormation never replaces it.
func (h *Handler) applyArchiveModes(ctx context.Context, r Request) (Response, error) {
	var p, old archiveModeProps
	if err := decode(r.Props, &p); err != nil {
		return Response{}, err
	}
	if p.MetaTable == "" {
		return Response{}, errors.New("property MetaTable is missing")
	}
	create := r.Action == ActionCreate
	if !create {
		if err := decode(r.OldProps, &old); err != nil {
			return Response{}, err
		}
	}
	m := meta.New(h.Meta, p.MetaTable)
	err := retry(ctx, func() error {
		return denied(m.ApplyArchiveModes(ctx, old.Streams, p.Streams, create, time.Now()))
	})
	if err != nil {
		return Response{}, err
	}
	return Response{PhysicalID: p.MetaTable + "/archive-mode"}, nil
}
