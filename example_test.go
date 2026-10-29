// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/lucindo/dynaflux"
)

// An application's Store, with the AWS client that Config.AWS recommends. The prefix is
// the one dynaflux dev prints, or the one the templates were deployed with.
//
//nolint:testableexamples // it loads the AWS configuration of the machine it runs on
func ExampleOpen() {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.RateLimiter = ratelimit.None })
		}),
		config.WithHTTPClient(awshttp.NewBuildableClient().WithTimeout(10*time.Second)))
	if err != nil {
		fmt.Println(err)
		return
	}
	store, err := dynaflux.Open(dynaflux.Config{AWS: cfg, Prefix: os.Getenv("DYNAFLUX_PREFIX")})
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = store // pass it to the generated package's New
}

// A Decimal keeps its scale: == compares the representation, and Equal the value.
func ExampleParseDecimal() {
	a, err := dynaflux.ParseDecimal("12.5")
	if err != nil {
		fmt.Println(err)
		return
	}
	b, err := dynaflux.ParseDecimal("12.50")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(a, b)
	fmt.Println(a == b, a.Equal(b))
	// Output:
	// 12.5 12.50
	// false true
}

// ParseUUID accepts four forms, and String prints one.
func ExampleParseUUID() {
	for _, s := range []string{
		"f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
		"{F81D4FAE-7DEC-11D0-A765-00A0C91E6BF6}",
		"urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
		"f81d4fae7dec11d0a76500a0c91e6bf6",
	} {
		u, err := dynaflux.ParseUUID(s)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(u)
	}
	// Output:
	// f81d4fae-7dec-11d0-a765-00a0c91e6bf6
	// f81d4fae-7dec-11d0-a765-00a0c91e6bf6
	// f81d4fae-7dec-11d0-a765-00a0c91e6bf6
	// f81d4fae-7dec-11d0-a765-00a0c91e6bf6
}

// A retried batch on a stream with DEDUP BY reports the events that an earlier try stored
// as ErrDuplicate. [errors.Is] on the whole error can't tell those from failures, so check
// each event. Here the retry stored event 0, an earlier try stored event 1, and event 2
// isn't stored.
func ExampleBatchError() {
	err := error(&dynaflux.BatchError{Errs: []error{
		nil,
		dynaflux.ErrDuplicate,
		&dynaflux.ValueError{Name: "price", Err: fmt.Errorf("%w: more digits than its scale", dynaflux.ErrInvalidValue)},
	}})
	if be, ok := errors.AsType[*dynaflux.BatchError](err); ok {
		for i, e := range be.Errs {
			if e != nil && !errors.Is(e, dynaflux.ErrDuplicate) {
				fmt.Printf("event %d was not stored: %v\n", i, e)
			}
		}
	}
	// Output:
	// event 2 was not stored: dynaflux: price: invalid value: more digits than its scale
}

// A Token in a JSON document is its text, and the zero Token is "".
func ExampleToken_MarshalText() {
	tok, err := dynaflux.ParseToken("AQVsb2NhbApwYWdlX3ZpZXdzANxqz6wA2Yd-zm02iqwab4DA39mToNcBAA")
	if err != nil {
		fmt.Println(err)
		return
	}
	type receipt struct {
		Token dynaflux.Token `json:"wait"`
		Prior dynaflux.Token `json:"prior"`
	}
	data, err := json.Marshal(receipt{Token: tok})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
	var back receipt
	if err = json.Unmarshal(data, &back); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(back.Token == tok, back.Prior.IsZero())
	// Output:
	// {"wait":"AQVsb2NhbApwYWdlX3ZpZXdzANxqz6wA2Yd-zm02iqwab4DA39mToNcBAA","prior":""}
	// true true
}
