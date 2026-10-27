// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package runtime is the entry point of the Lambda binary that dynaflux generate writes to
// runtime.out. One binary serves every function of a store; DYNAFLUX_MODE picks the handler.
package runtime

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	fh "github.com/aws/aws-sdk-go-v2/service/firehose"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/internal/access"
	"github.com/lucindo/dynaflux/internal/archive/firehose"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/setup/awssetup"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

// Run serves the function that DYNAFLUX_MODE names, for the store of cat. It returns only
// when the function can't start, and the generated main exits with that error.
//
// Run makes no AWS call: it checks the catalog and the environment during Lambda's Init
// phase, so a function that can't start fails there.
func Run(cat *dynaflux.Catalog) error {
	c, err := access.Get().Catalog(cat)
	if err != nil {
		return err
	}
	e, err := readEnv(c.Catalog(), os.Getenv)
	if err != nil {
		return err
	}
	cfg, err := awsConfig(context.Background(), e.region)
	if err != nil {
		return err
	}
	log := newLogger(os.Stdout, e.logLevel)
	if e.mode == catalog.EnvModeSetup {
		lambda.Start(newSetup(awssetup.New(cfg), log))
	} else {
		lambda.Start(newProjector(c, e, dynamo.New(dynamodb.NewFromConfig(cfg)), firehose.SDK{Client: fh.NewFromConfig(cfg)}, log))
	}
	return nil // not reached: lambda.Start exits the process when its loop ends
}
