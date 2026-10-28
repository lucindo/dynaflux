// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package queryhttp

import (
	"context"
	"net/http"
	"strconv"

	"github.com/aws/aws-lambda-go/lambdacontext"

	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/expr"
	"github.com/lucindo/dynaflux/internal/valuetext"
)

// reply is the answer to a request, before a door wraps it.
type reply struct {
	status int
	code   string // a failure's code
	msg    string // a failure's message
	body   []byte // nil for a failure whose body is built from status, code and msg
	rows   int
	allow  string   // the methods a 405 names
	params []string // the parameters the request gave, which the log line names
	cause  error    // the error behind a 5xx, for the log line only
}

func failed(status int, code, msg string) reply {
	return reply{status: status, code: code, msg: msg}
}

var stringType = catalog.Type{Kind: catalog.KindString} //nolint:gochecknoglobals // a constant catalog.Type

func appendString(dst []byte, s string) []byte {
	return valuetext.AppendJSON(dst, stringType, expr.String(s))
}

// appendResult appends the body of q's rows: ClickHouse's FORMAT JSON without its
// statistics, so a client decodes any query by meta, and code that reads ClickHouse's
// results reads data. It gives up, returning false, once dst passes maxPayload, so a large
// result never takes more memory than a response can hold.
func appendResult(dst []byte, q *catalog.Query, rows [][]expr.Value) ([]byte, bool) {
	dst = append(dst, `{"status":200,"meta":[`...)
	for i, c := range q.Columns {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendString(append(dst, `{"name":`...), c.Name)
		dst = append(appendString(append(dst, `,"type":`...), c.Expr.Type.String()), '}')
	}
	dst = append(dst, `],"data":[`...)
	for i, row := range rows {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, '{')
		for j, c := range q.Columns {
			if j > 0 {
				dst = append(dst, ',')
			}
			dst = append(appendString(dst, c.Name), ':')
			dst = valuetext.AppendJSON(dst, c.Expr.Type, row[j])
		}
		if dst = append(dst, '}'); len(dst) > maxPayload {
			return nil, false
		}
	}
	dst = append(append(dst, `],"rows":`...), strconv.Itoa(len(rows))...)
	return append(dst, '}'), true
}

// errorBody is the body of a failure. Without a message, as /health answers, it has only
// the status and the code.
func errorBody(r reply, requestID string) []byte {
	b := []byte(`{"status":` + strconv.Itoa(r.status))
	b = appendString(append(b, `,"error":{"code":`...), r.code)
	if r.msg != "" {
		b = appendString(append(b, `,"message":`...), r.msg)
		b = appendString(append(b, `,"request_id":`...), requestID)
	}
	return append(b, "}}"...)
}

func tooLarge() reply {
	return failed(http.StatusBadRequest, codeResultTooLarge, "the response is over Lambda's limit of "+strconv.Itoa(maxPayload)+
		" bytes: narrow the parameters, or add a LIMIT to the query")
}

// requestID returns Lambda's ID of the invocation, which the function's log lines carry.
func requestID(ctx context.Context) string {
	if lc, ok := lambdacontext.FromContext(ctx); ok {
		return lc.AwsRequestID
	}
	return ""
}
