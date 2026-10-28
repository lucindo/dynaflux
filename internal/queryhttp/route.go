// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/engine"
)

// route answers an HTTP request by its method and path.
func (h *Handler) route(ctx context.Context, r request) reply {
	name, isQuery := strings.CutPrefix(r.path, "/queries/")
	switch {
	case r.path == "/health" && r.method == http.MethodGet:
		return h.health(ctx)
	case r.path == "/health":
		return notAllowed(r, http.MethodGet)
	case isQuery && r.method == http.MethodPost:
		return h.query(ctx, name, r.body)
	case isQuery:
		return notAllowed(r, http.MethodPost)
	}
	return failed(http.StatusNotFound, codeNotFound, fmt.Sprintf("no route %s %s: the routes are POST /queries/{name} and GET /health",
		r.method, r.path))
}

// answer serves a decoded request.
func (h *Handler) answer(ctx context.Context, r request) reply {
	switch {
	case r.bad != "":
		return failed(http.StatusBadRequest, codeBadRequest, r.bad)
	case r.door == doorDirect:
		return h.query(ctx, r.query, r.body)
	}
	return h.route(ctx, r)
}

// query runs an exposed query with the parameters object body.
func (h *Handler) query(ctx context.Context, name string, body []byte) reply {
	q, ok := h.queries[name]
	if !ok {
		// An unexposed query gets the same answer, so its name can't be probed.
		return failed(http.StatusNotFound, codeNotFound, "no exposed query "+strconv.Quote(name))
	}
	names := make([]string, len(q.Params))
	for i, p := range q.Params {
		names[i] = p.Name
	}
	raw, err := object(body, names)
	var r reply
	switch u, unknown := errors.AsType[*unknownKeyError](err); {
	case unknown:
		msg := fmt.Sprintf("%s has no parameter %q", q.Name, u.key)
		if s, found := diag.Suggest(u.key, names); found {
			msg += "; did you mean " + s + "?"
		}
		r = failed(http.StatusBadRequest, codeInvalidParams, msg)
	case err != nil:
		r = failed(http.StatusBadRequest, codeBadRequest, "the parameters must be one JSON object: "+err.Error())
	default:
		r = h.run(ctx, q, raw)
	}
	// Only q's own names: a key the caller made up stays out of the log.
	for i, v := range raw {
		if v != nil {
			r.params = append(r.params, names[i])
		}
	}
	return r
}

func (h *Handler) run(ctx context.Context, q *catalog.Query, raw []json.RawMessage) reply {
	params, err := bind(q, raw)
	if err != nil {
		return failed(http.StatusBadRequest, codeInvalidParams, err.Error())
	}
	rows, err := h.e.Query(ctx, h.cat, q.Name, params, engine.QueryOptions{})
	if err != nil && !errors.Is(err, engine.ErrNotFound) {
		return failure(ctx, err)
	}
	body, ok := appendResult(nil, q, rows)
	if !ok {
		r := tooLarge()
		r.rows = len(rows)
		return r
	}
	return reply{status: http.StatusOK, body: body, rows: len(rows)}
}
