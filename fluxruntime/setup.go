// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package fluxruntime

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/cfn"

	"github.com/lucindo/dynaflux/internal/setup"
)

const (
	// replyReserve is how long before Lambda's deadline a request's work ends, so that a
	// stuck call still gets a FAILED reply.
	replyReserve = 30 * time.Second
	// maxReply is the largest reply body CloudFormation takes.
	maxReply = 4096
)

// setupFn serves DYNAFLUX_MODE=setup: the templates' custom resources (ADR 0008).
type setupFn struct {
	h   *setup.Handler
	log *slog.Logger
}

// newSetup returns the setup function. cfn.LambdaWrap sends the reply, PUT without a
// Content-Type; when it can't, the function fails, and Lambda's retries of the
// asynchronous invocation run the request again, which every handler allows.
func newSetup(h *setup.Handler, log *slog.Logger) cfn.CustomResourceLambdaFunction {
	s := &setupFn{h: h, log: log}
	return redact(cfn.LambdaWrap(s.serve))
}

// serve answers one request. On an error, it logs the error in full and returns it cut to
// fit the reply.
func (s *setupFn) serve(ctx context.Context, ev cfn.Event) (string, map[string]any, error) {
	work := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		work, cancel = context.WithDeadline(ctx, deadline.Add(-replyReserve))
		defer cancel()
	}
	resp, err := s.h.Handle(work, setup.Request{
		Type: ev.ResourceType, Action: string(ev.RequestType), PhysicalID: ev.PhysicalResourceID, Props: ev.ResourceProperties,
		OldProps: ev.OldResourceProperties,
	})
	// What LambdaWrap would fall back to: a failed Create has no ID, and the rollback's
	// Delete then carries the request's.
	id := cmp.Or(resp.PhysicalID, ev.PhysicalResourceID, ev.RequestID)
	if err != nil {
		s.log.LogAttrs(ctx, slog.LevelError, "custom resource failed", slog.String("requestId", lambdaRequestID(ctx)),
			slog.String("cfnRequestId", ev.RequestID), slog.String("resourceType", ev.ResourceType),
			slog.String("logicalResourceId", ev.LogicalResourceID), slog.String("requestType", string(ev.RequestType)),
			slog.String("error", err.Error()))
		return id, nil, errors.New(fit(ev, id, err.Error()))
	}
	var data map[string]any
	if len(resp.Data) > 0 {
		data = make(map[string]any, len(resp.Data))
		for k, v := range resp.Data {
			data[k] = v
		}
	}
	return id, data, nil
}

// fit returns reason, or its longest cut with which the FAILED reply that LambdaWrap sends
// for ev, with physical ID id, fits maxReply. JSON escaping can make a byte of the reason
// six, so the cut is searched for.
func fit(ev cfn.Event, id, reason string) string {
	r := cfn.NewResponse(&ev)
	r.Status, r.PhysicalResourceID = cfn.StatusFailed, id
	fits := func(n int) bool {
		r.Reason = cut(reason, n)
		body, err := json.Marshal(r)
		return err == nil && len(body) <= maxReply
	}
	if fits(len(reason)) {
		return reason
	}
	// Below len(reason), the body grows with n.
	n := sort.Search(len(reason), func(n int) bool { return !fits(n) })
	return cut(reason, max(n-1, 0))
}

// redact drops the query of the reply URL from the error LambdaWrap returns when it
// couldn't send the reply: the query holds the URL's presigned signature, and Lambda logs
// the error.
func redact(fn cfn.CustomResourceLambdaFunction) cfn.CustomResourceLambdaFunction {
	return func(ctx context.Context, ev cfn.Event) (string, error) {
		reason, err := fn(ctx, ev)
		if ue, ok := errors.AsType[*url.Error](err); ok {
			ue.URL, _, _ = strings.Cut(ue.URL, "?")
			reason = err.Error()
		}
		return reason, err
	}
}
