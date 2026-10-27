// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
)

// Each HTTP attempt ends after attemptTimeout; the invocation's deadline bounds the whole
// call. The core writes 16 rows at once, so twice that many connections stay open between
// invocations.
const (
	attemptTimeout = 10 * time.Second
	maxIdleConns   = 32
)

// httpClient returns the client of every AWS call. Its connections close with SO_LINGER 0,
// which sends a reset and drops the bytes the kernel hasn't sent yet: by default it keeps
// sending them, after the deadline and even from a frozen or exited process (ADR 0010).
// The dialer sets it, since a service client rebuilds DialContext from the dialer in every
// defaults mode but legacy. It speaks only HTTP/1.1: a cancelled HTTP/2 request resets its
// stream and leaves the connection open.
func httpClient() *awshttp.BuildableClient {
	return awshttp.NewBuildableClient().WithTimeout(attemptTimeout).
		WithDialerOptions(func(d *net.Dialer) { d.Control = lingerZero }).
		WithTransportOptions(func(tr *http.Transport) {
			tr.MaxIdleConnsPerHost = maxIdleConns
			tr.Protocols = new(http.Protocols)
			tr.Protocols.SetHTTP1(true)
			// The option gets a clone, whose TLS configuration already offers h2.
			tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
		})
}

// awsConfig returns the configuration of the function's AWS clients in region.
func awsConfig(ctx context.Context, region string) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithHTTPClient(httpClient()),
		config.WithRetryer(retryer))
	if err != nil {
		return aws.Config{}, fmt.Errorf("runtime: load the AWS configuration: %w", err)
	}
	return cfg, nil
}

// retryer is the SDK's standard retryer without its client-side retry quota, which would
// stop retries for the rest of the process after a burst of throttling; the invocation's
// deadline bounds them instead.
func retryer() aws.Retryer {
	return retry.NewStandard(func(o *retry.StandardOptions) { o.RateLimiter = ratelimit.None })
}
