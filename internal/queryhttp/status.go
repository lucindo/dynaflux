// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/store"
)

// Error codes (docs/spec/http.md).
const (
	codeBadRequest       = "bad_request"
	codeInvalidParams    = "invalid_params"
	codeTooManyKeys      = "too_many_keys"
	codeResultTooLarge   = "result_too_large"
	codeNotFound         = "not_found"
	codeMethodNotAllowed = "method_not_allowed"
	codeViewNotReady     = "view_not_ready"
	codeThrottled        = "throttled"
	codeTimeout          = "timeout"
	codeEvaluation       = "evaluation_error"
	codeInternal         = "internal"
)

// failure answers a query that failed with err, whose ctx is the handler's budget. The log
// gets err for a 5xx only: a 4xx's message can quote a parameter's value.
func failure(ctx context.Context, err error) reply {
	var r reply
	nr, notReady := errors.AsType[*engine.NotReadyError](err)
	switch {
	case errors.Is(err, engine.ErrInvalidValue):
		r = failed(http.StatusBadRequest, codeInvalidParams, engineText(err))
	case errors.Is(err, engine.ErrTooManyKeys):
		r = failed(http.StatusBadRequest, codeTooManyKeys, engineText(err))
	case notReady:
		r = failed(http.StatusServiceUnavailable, codeViewNotReady, fmt.Sprintf("view version %s is %s in %s", nr.Version, nr.State, nr.Region))
	case errors.Is(err, engine.ErrEvaluation):
		r = failed(http.StatusInternalServerError, codeEvaluation, engineText(err))
	case ctx.Err() != nil:
		r = failed(http.StatusGatewayTimeout, codeTimeout, "the query didn't finish before the function's deadline")
	case errors.Is(err, store.ErrThrottled):
		r = failed(http.StatusServiceUnavailable, codeThrottled, "DynamoDB throttled the query's reads; retry after a second")
	default:
		r = failed(http.StatusInternalServerError, codeInternal, "internal error; the function's log has it under this request_id")
	}
	if r.status >= http.StatusInternalServerError {
		r.cause = err
	}
	return r
}

// engineText is an engine error's text without its "dynaflux: ".
func engineText(err error) string {
	return strings.TrimPrefix(err.Error(), "dynaflux: ")
}

func notAllowed(r request, allow string) reply {
	f := failed(http.StatusMethodNotAllowed, codeMethodNotAllowed, fmt.Sprintf("%s takes %s, not %s", r.path, allow, r.method))
	f.allow = allow
	return f
}
