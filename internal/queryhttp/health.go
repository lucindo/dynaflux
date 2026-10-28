// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/lucindo/dynaflux/internal/engine"
)

// health answers 200 while the current version of every view an exposed query reads is
// live in the region, so a Route 53 health check takes the region out of DNS until it is.
// /health needs no auth on the API doors, so a failure's body has only its status and
// code: version names go to the log.
func (h *Handler) health(ctx context.Context) reply {
	var notReady []string
	for _, v := range h.views {
		err := h.e.Ready(ctx, h.cat, v, false)
		if _, ok := errors.AsType[*engine.NotReadyError](err); ok {
			notReady = append(notReady, err.Error())
		} else if err != nil {
			r := failure(ctx, err)
			r.msg = ""
			return r
		}
	}
	if len(notReady) > 0 {
		r := failed(http.StatusServiceUnavailable, codeViewNotReady, "")
		r.cause = errors.New(strings.Join(notReady, "; "))
		return r
	}
	return reply{status: http.StatusOK, body: append(appendString([]byte(`{"status":200,"region":`), h.region), '}')}
}
