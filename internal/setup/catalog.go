// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"context"
	"errors"
	"fmt"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/meta"
)

type catalogProps struct {
	MetaTable   string `json:"MetaTable"`
	CatalogHash string `json:"CatalogHash"`
	Generator   string `json:"Generator"`
}

// registerCatalog answers [TypeCatalog] (ADR 0013). Create and Update make the catalog that
// CatalogHash names current: the binary's own, or one registered before, as a rollback
// asks; Create then retires what it lists. The physical ID changes with the hash, so
// CloudFormation deletes the replaced registration only after the whole update succeeded,
// and that Delete retires what the current catalog lists. Deleting the current
// registration, as deleting the stack does, retires nothing.
func (h *Handler) registerCatalog(ctx context.Context, r Request) (Response, error) {
	var p catalogProps
	if err := decode(r.Props, &p); err != nil {
		return Response{}, err
	}
	switch {
	case p.MetaTable == "":
		return Response{}, errors.New("property MetaTable is missing")
	case p.CatalogHash == "":
		return Response{}, errors.New("property CatalogHash is missing")
	case p.Generator == "":
		return Response{}, errors.New("property Generator is missing")
	}
	m := meta.New(h.Meta, p.MetaTable)
	if r.Action == ActionDelete {
		err := retry(ctx, func() error { return denied(cleanUp(ctx, m, p.CatalogHash)) })
		return Response{PhysicalID: r.PhysicalID}, err
	}
	err := retry(ctx, func() error {
		err := h.point(ctx, m, p)
		if err == nil && r.Action == ActionCreate {
			err = retire(ctx, m, p.CatalogHash, "")
		}
		return denied(err)
	})
	if err != nil {
		return Response{}, err
	}
	return Response{PhysicalID: p.MetaTable + "/" + p.CatalogHash}, nil
}

// point makes the catalog of p current: the binary's when p names it, else one stored before.
func (h *Handler) point(ctx context.Context, m *meta.Meta, p catalogProps) error {
	own := catalog.Hash(h.Catalog)
	if p.CatalogHash == own {
		_, err := m.RegisterCatalog(ctx, h.Catalog, p.Generator)
		return err
	}
	err := m.PointCatalog(ctx, p.CatalogHash, p.Generator)
	if errors.Is(err, meta.ErrNoCatalog) {
		return fmt.Errorf("catalog %s isn't the runtime binary's (%s) and was never registered: "+
			"deploy the templates and the binary from the same dynaflux generate run", p.CatalogHash, own)
	}
	return err
}

// cleanUp answers the Delete of the registration of catalog deleted: when another catalog is
// current, an update replaced it, and cleanUp retires what the current catalog lists.
func cleanUp(ctx context.Context, m *meta.Meta, deleted string) error {
	reg, ok, err := m.Current(ctx)
	switch {
	case errorCode(err) == "ResourceNotFoundException":
		return nil // the stack deleted its meta table first
	case err != nil:
		return err
	case !ok || reg.Hash == deleted:
		return nil
	}
	return retire(ctx, m, reg.Hash, deleted)
}

// retire sets retired each version that the catalog stored under hash lists retired. A
// version without a state item never became active in this region; its stream, which its
// tombstone needs, comes from the catalog stored under previous when that one defines it,
// and otherwise the version is left as it is.
func retire(ctx context.Context, m *meta.Meta, hash, previous string) error {
	cat, err := readCatalog(ctx, m, hash)
	if err != nil || len(cat.Retired) == 0 {
		return err
	}
	states, err := m.ViewStates(ctx)
	if err != nil {
		return err
	}
	var streams map[string]string // of previous's versions, read on first need
	for _, id := range cat.Retired {
		vs, held := states[id]
		if vs.State == meta.Retired {
			continue
		}
		if !held {
			if streams == nil {
				if streams, err = versionStreams(ctx, m, previous); err != nil {
					return err
				}
			}
			if vs.Stream = streams[id]; vs.Stream == "" {
				continue
			}
		}
		if _, err = m.Retire(ctx, id, vs.Stream); err != nil {
			return err
		}
	}
	return nil
}

func readCatalog(ctx context.Context, m *meta.Meta, hash string) (*catalog.Catalog, error) {
	data, err := m.ReadCatalog(ctx, hash)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("catalog %s: %w", hash, err)
	}
	return cat, nil
}

// versionStreams returns the stream of each view version of the catalog stored under hash.
// It is empty without hash, when no catalog is stored under it, as after a failed Create,
// and when this binary can't decode it, as when a rollback deletes a newer catalog's
// registration.
func versionStreams(ctx context.Context, m *meta.Meta, hash string) (map[string]string, error) {
	out := map[string]string{}
	if hash == "" {
		return out, nil
	}
	data, err := m.ReadCatalog(ctx, hash)
	if errors.Is(err, meta.ErrNoCatalog) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	cat, err := catalog.Unmarshal(data)
	if err != nil {
		return out, nil //nolint:nilerr // a catalog this binary can't read names no stream it can use
	}
	for _, v := range cat.Views {
		for _, vv := range v.Versions {
			out[catalog.VersionID(v.Name, vv.Number)] = vv.Stream
		}
	}
	return out, nil
}

// denied marks DynamoDB's AccessDeniedException as [ErrAccessDenied], so a grant that the
// same deploy adds is retried.
func denied(err error) error {
	if errorCode(err) == "AccessDeniedException" {
		return fmt.Errorf("%w: %w", ErrAccessDenied, err)
	}
	return err
}

// errorCode returns the code of an AWS API error in err's chain, or "". It matches the SDK's
// errors by their method, since this package doesn't import the SDK (ADR 0004).
func errorCode(err error) string {
	var api interface{ ErrorCode() string }
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return ""
}
