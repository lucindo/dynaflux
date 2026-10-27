// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/backfill/awsbackfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/config"
	"github.com/lucindo/dynaflux/internal/deployed"
	"github.com/lucindo/dynaflux/internal/diag"
	"github.com/lucindo/dynaflux/internal/engine"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/dynamo"
)

const (
	// attemptTimeout bounds each AWS request; the SDK retries a request that times out.
	attemptTimeout = 30 * time.Second
	// localAccessKey and localSecretKey are the fake credentials sent to DynamoDB Local,
	// which accepts letters and digits only in an access key.
	localAccessKey = "dynafluxlocal"
	localSecretKey = "fake"
)

// cloud opens the clients of the regions a run reads: on AWS, or on DynamoDB Local with
// -endpoint. Tests pass a fake.
type cloud interface {
	// sdkRegion is the AWS SDK's region, "" when none is configured.
	sdkRegion() string
	clients(region string) regionClients
}

// regionClients are one region's clients. The SDK's clients satisfy each interface as they
// are.
type regionClients struct {
	data     store.Backend   // DynamoDB: the meta, stream and view tables
	identity callerIdentity  // STS; nil on DynamoDB Local
	clock    serverClock     // DynamoDB's clock, as its responses tell it
	sfn      stepFunctions   // the backfill state machine's executions; nil on DynamoDB Local
	tables   backfill.Tables // the stream tables' replicas; nil on DynamoDB Local
	units    backfill.Lister // the stream tables' shards
	// What status reads besides the tables; nil on DynamoDB Local.
	describe awsbackfill.DynamoDB
	lambda   deployed.Lambda
	metrics  deployed.CloudWatch
	bucket   deployed.S3
}

type stepFunctions interface {
	StartExecution(ctx context.Context, in *sfn.StartExecutionInput, opts ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error)
	DescribeExecution(ctx context.Context, in *sfn.DescribeExecutionInput, opts ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error)
	StopExecution(ctx context.Context, in *sfn.StopExecutionInput, opts ...func(*sfn.Options)) (*sfn.StopExecutionOutput, error)
}

type callerIdentity interface {
	GetCallerIdentity(ctx context.Context, in *sts.GetCallerIdentityInput, opts ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}

// serverClock reports the Date of the latest response, and this machine's time when it
// arrived; ok is false before the first response.
type serverClock interface {
	lastDate() (server, local time.Time, ok bool)
}

// dateRecorder is the serverClock of an SDK client.
type dateRecorder struct {
	mu            sync.Mutex
	server, local time.Time
}

func (r *dateRecorder) lastDate() (time.Time, time.Time, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.server, r.local, !r.server.IsZero()
}

// record makes a DynamoDB client keep the Date header of each response in r.
func (r *dateRecorder) record(o *dynamodb.Options) {
	o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
		return s.Deserialize.Add(middleware.DeserializeMiddlewareFunc("dynafluxDate",
			func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
				out, md, err := next.HandleDeserialize(ctx, in)
				if resp, ok := out.RawResponse.(*smithyhttp.Response); ok {
					if t, perr := http.ParseTime(resp.Header.Get("Date")); perr == nil {
						r.mu.Lock()
						r.server, r.local = t, time.Now()
						r.mu.Unlock()
					}
				}
				return out, md, err
			}), middleware.After)
	})
}

type sdkCloud struct{ cfg aws.Config }

func (s sdkCloud) sdkRegion() string { return s.cfg.Region }

func (s sdkCloud) clients(region string) regionClients {
	cfg := s.cfg.Copy()
	cfg.Region = region
	date := &dateRecorder{}
	ddb := dynamodb.NewFromConfig(cfg, date.record)
	return regionClients{
		data: dynamo.New(ddb), identity: sts.NewFromConfig(cfg), clock: date,
		sfn: sfn.NewFromConfig(cfg), tables: awsbackfill.NewReplicas(cfg), units: awsbackfill.NewUnits(ddb, dynamodbstreams.NewFromConfig(cfg)),
		describe: ddb, lambda: lambda.NewFromConfig(cfg), metrics: cloudwatch.NewFromConfig(cfg), bucket: s3.NewFromConfig(cfg),
	}
}

// localCloud is DynamoDB Local at its endpoint. It takes no AWS configuration, so nothing
// configured can send its requests, or its fake credentials, elsewhere.
type localCloud string

func (localCloud) sdkRegion() string { return "" }

func (l localCloud) clients(region string) regionClients {
	cfg := localConfig(string(l), region)
	date := &dateRecorder{}
	ddb := dynamodb.NewFromConfig(cfg, date.record)
	return regionClients{data: dynamo.New(ddb), clock: date, units: awsbackfill.NewUnits(ddb, dynamodbstreams.NewFromConfig(cfg))}
}

// localConfig configures the clients of DynamoDB Local at endpoint, in region, with fake
// static credentials.
func localConfig(endpoint, region string) aws.Config {
	return aws.Config{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		HTTPClient:   awshttp.NewBuildableClient().WithTimeout(attemptTimeout),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: localAccessKey, SecretAccessKey: localSecretKey, Source: "dynaflux -endpoint"}, nil
		}),
	}
}

// connectAWS reaches AWS with the SDK's configuration: the environment, the shared files,
// SSO, and container and instance roles, with AWS_PROFILE choosing a profile. With
// endpoint, it reaches DynamoDB Local there instead.
func connectAWS(ctx context.Context, endpoint string) (cloud, error) {
	if endpoint != "" {
		return localCloud(endpoint), nil
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithHTTPClient(awshttp.NewBuildableClient().WithTimeout(attemptTimeout)))
	if err != nil {
		return nil, fmt.Errorf("load the AWS configuration: %w", err)
	}
	return sdkCloud{cfg: cfg}, nil
}

// openDeployment starts a command that reads one region of the deployed store: it loads
// dynaflux.yaml, picks the region, prints the header on stderr, and reads and compiles the
// region's deployed catalog, refusing one in a newer format (DF7001).
func (c *cli) openDeployment(name string, f *awsFlags) (*deployment, int) {
	cfg, status := c.loadConfig()
	if status != exitOK {
		return nil, status
	}
	fl, status := c.openStore(c.ctx, name, cfg, f.prefix, f.endpoint, oneRegion(cfg, f.region, f.endpoint))
	if status != exitOK {
		return nil, status
	}
	d := fl.regions[0]
	d.err = d.readCatalog(c.ctx)
	if status = c.refused(name, d); status != exitOK {
		return nil, status
	}
	var err error
	if d.compiled, err = engine.Compile(d.data); err != nil {
		c.errorf("dynaflux %s: %s: the deployed catalog %s: %v", name, d.region, d.reg.Hash, err)
		return nil, exitErrors
	}
	d.engine = engine.New(d.clients.data, engine.Options{Region: d.region, Prefix: d.naming.Prefix})
	return d, exitOK
}

// applyPrefix sets naming's prefix to -prefix, when it was given.
func (c *cli) applyPrefix(name string, p optionalString, naming *catalog.Naming) int {
	if !p.set {
		return exitOK
	}
	if !catalog.ValidPrefix(p.value) {
		return c.usageError(name, fmt.Sprintf("-prefix %q isn't a prefix: use lowercase letters, digits and -, "+
			"starting with a letter and ending with -, such as prod-, or \"\" for none", p.value))
	}
	naming.Prefix = p.value
	return exitOK
}

// checkFormat refuses a deployed catalog in a newer format than this dynaflux reads (DF7001).
func (c *cli) checkFormat(region string, reg meta.Registered) int {
	f, newer := formatFinding(region, reg)
	if !newer {
		return exitOK
	}
	f.write(&c.errOut)
	return exitErrors
}

// finding prints a diagnostic about a region's deployed state (lifecycle.md §12).
func (c *cli) finding(region string, sev diag.Severity, code, msg, hint string) {
	regionFinding(region, sev, code, msg, hint).write(&c.errOut)
}

// pickRegion returns the region a single-region command reads: the one named with -region,
// or else the SDK's, which must be one of regions. On failure it returns a usage message.
func pickRegion(named, sdk string, regions []string) (string, string) {
	list := strings.Join(regions, ", ")
	switch {
	case named != "" && !slices.Contains(regions, named):
		return "", fmt.Sprintf("-region %s isn't in regions (%s)", named, list)
	case named != "":
		return named, ""
	case sdk == "":
		return "", fmt.Sprintf("name the region with -region, one of %s", list)
	case !slices.Contains(regions, sdk):
		return "", fmt.Sprintf("the AWS SDK's region %s isn't in regions (%s); name one with -region", sdk, list)
	}
	return sdk, ""
}

// checkEndpoint accepts only a loopback http URL: the fake credentials that -endpoint sends
// must never reach AWS.
func checkEndpoint(s string) error {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "http" || u.Host == "" {
		return errors.New("want a URL such as http://127.0.0.1:8000")
	}
	if ip := net.ParseIP(u.Hostname()); u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("%s isn't a loopback address", u.Hostname())
	}
	return nil
}

// loadConfig finds and loads dynaflux.yaml, for a command that reads the deployed catalog
// rather than the local SQL. Like explain, it prints the diagnostics only when they stop
// the command.
func (c *cli) loadConfig() (*config.Config, int) {
	root, status := c.projectRoot()
	if status != exitOK {
		return nil, status
	}
	defer root.Close()
	cfg, ds := config.Load(root.FS())
	switch {
	case slices.ContainsFunc(ds, func(d diag.Diagnostic) bool { return d.Code == codeUnreadable }):
		return nil, c.diagnostics(ds, exitUsage)
	case cfg == nil || diag.HasErrors(ds):
		return nil, c.diagnostics(ds, exitErrors)
	}
	return cfg, exitOK
}
