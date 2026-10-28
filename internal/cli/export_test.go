// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"io"
	"runtime/debug"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/store"
)

// ModuleVersion exposes moduleVersion: a test binary's build info can't show how the
// version reads in an installed binary or under go tool.
func ModuleVersion(bi *debug.BuildInfo) string {
	return moduleVersion(bi)
}

// HeaderVersion exposes headerVersion, for the same reason.
func HeaderVersion(bi *debug.BuildInfo) string {
	return headerVersion(bi)
}

// Env is what [RunEnv] gives a run in place of the process's.
type Env struct {
	Vars     map[string]string // the environment
	Terminal bool              // whether stdin is a terminal
	// Region is the AWS SDK's region, Account the caller's account, and Backends the
	// DynamoDB tables of each region.
	Region   string
	Account  string
	Backends map[string]store.Backend
	// ClockAhead is how far this machine's clock is ahead of DynamoDB's.
	ClockAhead time.Duration
	// StepFunctions answers each region's Step Functions calls, and Tables describes the
	// replicas of its stream tables.
	StepFunctions map[string]StepFunctions
	Tables        map[string]backfill.Tables
	// AWS answers the other calls of status and backfill in each region, and Units lists
	// the shards of its stream tables.
	AWS   map[string]AWS
	Units map[string]backfill.Lister
}

// AWS is the services that status and backfill call in a region besides DynamoDB's tables.
type AWS interface {
	awsbackfill.DynamoDB
	deployed.Lambda
	deployed.CloudWatch
	deployed.S3
	queryStopper
}

// StepFunctions is the Step Functions client of a region.
type StepFunctions = stepFunctions

// ExecutionName is executionName.
func ExecutionName(version string, created time.Time) string { return executionName(version, created) }

// LocalAccessKey is the access key that -endpoint sends to DynamoDB Local, which keeps the
// tables of each access key apart.
const LocalAccessKey = localAccessKey

// RunEnv runs dynaflux as Run does, in env.
func RunEnv(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, workdir string, env Env) int {
	c := &cli{
		ctx: ctx, workdir: workdir, stdin: stdin, terminal: env.Terminal, stdout: stdout, stderr: stderr,
		getenv:  func(k string) string { return env.Vars[k] },
		connect: func(context.Context, string) (cloud, error) { return fakeCloud(env), nil },
	}
	return c.exit(c.run(args))
}

type fakeCloud Env

func (f fakeCloud) sdkRegion() string { return f.Region }

func (f fakeCloud) clients(region string) regionClients {
	a := f.AWS[region]
	return regionClients{
		data: f.Backends[region], identity: fakeIdentity(f.Account), clock: fakeClock(f.ClockAhead),
		sfn: f.StepFunctions[region], tables: f.Tables[region], units: f.Units[region],
		describe: a, lambda: a, metrics: a, bucket: a, athena: a,
	}
}

type fakeClock time.Duration

func (f fakeClock) lastDate() (time.Time, time.Time, bool) {
	now := time.Now()
	return now.Add(-time.Duration(f)), now, true
}

type fakeIdentity string

func (a fakeIdentity) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{Account: aws.String(string(a)), Arn: aws.String("arn:aws:iam::" + string(a) + ":user/operator")}, nil
}

// Confirm runs confirm in a run whose stdin is stdin, and returns its status and stderr.
func Confirm(ctx context.Context, stdin io.Reader, terminal, yes bool) (int, string) {
	var stderr strings.Builder
	c := &cli{ctx: ctx, stdin: stdin, terminal: terminal, stdout: io.Discard, stderr: &stderr}
	return c.exit(c.confirm("retire", "retire page_daily@v1 in us-east-1?", yes)), stderr.String()
}

// IsTerminal is isTerminal.
func IsTerminal(r io.Reader) bool { return isTerminal(r) }
