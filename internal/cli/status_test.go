// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/lucindo/dynaflux/internal/backfill"
	"github.com/lucindo/dynaflux/internal/catalog"
	"github.com/lucindo/dynaflux/internal/cli"
	"github.com/lucindo/dynaflux/internal/meta"
	"github.com/lucindo/dynaflux/internal/store"
	"github.com/lucindo/dynaflux/internal/store/mem"
)

// statusStore is testdata/status.txtar deployed in memory in us-east-1 and us-west-2 with
// every version live, its projectors 2 seconds behind and its archive 2 minutes. Make it in
// a synctest bubble, whose clock reads 2000-01-01T00:00:00Z.
type statusStore struct {
	dir      string
	cat      *catalog.Catalog
	backends map[string]store.Backend
	metas    map[string]*meta.Meta
	sfn      map[string]*fakeSFN
	aws      map[string]*fakeAWS
	units    map[string]fakeUnits
}

var shop = catalog.Naming{Store: "shop"} //nolint:gochecknoglobals // test fixture

func newStatusStore(t *testing.T) *statusStore {
	t.Helper()
	ctx := t.Context()
	s := &statusStore{dir: writeProject(t, "status"), backends: map[string]store.Backend{}, metas: map[string]*meta.Meta{},
		sfn: map[string]*fakeSFN{}, aws: map[string]*fakeAWS{}, units: map[string]fakeUnits{}}
	s.cat = compile(t, s.dir).Catalog
	data, err := catalog.Marshal(s.cat)
	must(t, err)
	for _, r := range backfillRegions {
		b := mem.New()
		s.backends[r], s.metas[r] = b, meta.New(b, shop.MetaTable())
		_, err = s.metas[r].RegisterCatalog(ctx, data, "v0.5.0")
		must(t, err)
		for id, vv := range s.cat.Versions() {
			split := meta.SplitNone
			if vv.FencedHandoff() {
				split = meta.SplitTime
			}
			_, err = s.metas[r].Activate(ctx, id, vv.Stream, split, meta.DefaultBounds)
			must(t, err)
			must(t, s.metas[r].MakeLive(ctx, id, time.Time{}))
		}
		s.sfn[r] = &fakeSFN{status: map[string]types.ExecutionStatus{}}
		s.aws[r] = healthyAWS(shop, r, s.cat)
		s.units[r] = fakeUnits{shop.StreamTable("page_views"): {"shardId-0002", "shardId-0001"}, shop.StreamTable("signups"): {"shardId-0003"}}
	}
	return s
}

// healthyAWS is region's services with each stream mapped to the store's projector, which
// is 2 seconds behind, and archived 2 minutes behind.
func healthyAWS(naming catalog.Naming, region string, cat *catalog.Catalog) *fakeAWS {
	a := &fakeAWS{bucket: naming.Bucket(account, region), arns: map[string]string{}, mappings: map[string][]lambdatypes.EventSourceMappingConfiguration{},
		metrics: map[string][]float64{"IteratorAge " + naming.ProjectorFunction(): {2000}}, objects: map[string]string{}}
	for _, st := range cat.Streams {
		arn := streamARN(naming, region, st.Name)
		a.arns[naming.StreamTable(st.Name)] = arn
		a.mappings[arn] = []lambdatypes.EventSourceMappingConfiguration{mapping(region, st.Name, naming.ProjectorFunction(), "Enabled", "OK")}
		a.metrics["DeliveryToIceberg.DataFreshness "+naming.ArchiveStream(st.Name)] = []float64{120}
	}
	return a
}

func streamARN(naming catalog.Naming, region, stream string) string {
	return "arn:aws:dynamodb:" + region + ":" + account + ":table/" + naming.StreamTable(stream) + "/stream/2000-01-01T00:00:00.000"
}

func mapping(region, stream, function, state, result string) lambdatypes.EventSourceMappingConfiguration {
	return lambdatypes.EventSourceMappingConfiguration{
		UUID:        aws.String(region + "-" + stream + "-" + function),
		FunctionArn: aws.String("arn:aws:lambda:" + region + ":" + account + ":function:" + function),
		State:       aws.String(state), LastProcessingResult: aws.String(result),
	}
}

func (s *statusStore) run(t *testing.T, args ...string) result {
	t.Helper()
	var stdout, stderr strings.Builder
	env := cli.Env{Region: "us-east-1", Account: account, Backends: s.backends, StepFunctions: map[string]cli.StepFunctions{},
		AWS: map[string]cli.AWS{}, Units: map[string]backfill.Lister{}}
	for r := range s.backends {
		env.StepFunctions[r], env.AWS[r], env.Units[r] = s.sfn[r], s.aws[r], s.units[r]
	}
	status := cli.RunEnv(t.Context(), args, nil, &stdout, &stderr, s.dir, env)
	return result{status, stdout.String(), stderr.String()}
}

// job deploys plan_counts@v2 and gives it a running job in region whose execution
// <name>-a1 has status, or doesn't exist when status is "". It returns the execution's ARN.
func (s *statusStore) job(t *testing.T, region string, status types.ExecutionStatus) string {
	t.Helper()
	s.pending(t)
	const name = "plan-counts-v2-0a1b2c"
	arn := s.sfn[region].arn(region, name+"-a1")
	_, err := backfill.Start(t.Context(), s.metas[region], backfill.Request{
		Version: "plan_counts@v2", Stream: "signups", Name: name, Execution: func(int) string { return arn }, Split: meta.SplitTime,
	})
	must(t, err)
	if status != "" {
		s.sfn[region].status[arn] = status
	}
	return arn
}

// pending deploys plan_counts@v2, a rebuild, beside v1 in every region: a new version
// without a state.
func (s *statusStore) pending(t *testing.T) {
	t.Helper()
	cat := *s.cat
	cat.Views = slices.Clone(cat.Views)
	i := slices.IndexFunc(cat.Views, func(v catalog.View) bool { return v.Name == "plan_counts" })
	v1 := cat.Views[i].Versions[0]
	v1.State = catalog.StateRetained
	v2 := v1
	v2.Number, v2.State, v2.Rebuild = 2, catalog.StateCurrent, 1
	var err error
	v2.Fingerprint, err = catalog.Fingerprint(&v2)
	must(t, err)
	cat.Views[i].Versions = []catalog.ViewVersion{v1, v2}
	s.register(t, &cat)
}

func (s *statusStore) register(t *testing.T, cat *catalog.Catalog) {
	t.Helper()
	data, err := catalog.Marshal(cat)
	must(t, err)
	for _, m := range s.metas {
		_, err = m.RegisterCatalog(t.Context(), data, "v0.6.0")
		must(t, err)
	}
}

// gap puts an on-failure record of page_views's shardId-0001 in us-east-1's bucket, whose
// records arrived until last.
func (s *statusStore) gap(key string, last time.Time) {
	s.aws["us-east-1"].objects["aws/lambda/"+key] = fmt.Sprintf(`{"requestContext":{"requestId":"r1","condition":"RetryAttemptsExhausted"},`+
		`"version":"1.0","DDBStreamBatchInfo":{"shardId":"shardId-0001","startSequenceNumber":"100","endSequenceNumber":"250",`+
		`"approximateArrivalOfFirstRecord":"%s","approximateArrivalOfLastRecord":"%s","batchSize":3,"streamArn":%q},"payload":"{}"}`,
		last.Add(-time.Minute).Format(time.RFC3339), last.Format(time.RFC3339), streamARN(shop, "us-east-1", "page_views"))
}

// statusCase is a run of status on the deployed store, after change.
type statusCase struct {
	name   string
	args   []string
	status int
	change func(t *testing.T, s *statusStore)
}

// TestStatus runs status on the deployed store. Each case's output is in
// testdata/status.txtar.
func TestStatus(t *testing.T) {
	t.Parallel()
	g := readGoldens(t, "status")
	for _, tt := range slices.Concat(statusCases(), statusJobCases(), statusDeliveryCases()) {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				s := newStatusStore(t)
				if tt.change != nil {
					tt.change(t, s)
				}
				got := s.run(t, append([]string{"status"}, tt.args...)...)
				if got.status != tt.status {
					t.Errorf("status %d, want %d; stderr:\n%s", got.status, tt.status, got.stderr)
				}
				g.check(t, strings.ReplaceAll(tt.name, " ", "_"), got)
			})
		})
	}
}

var east = []string{"-region", "us-east-1"} //nolint:gochecknoglobals // test fixture

func statusCases() []statusCase {
	return []statusCase{
		{"healthy", nil, 0, nil},
		{"healthy json", []string{"-json"}, 0, nil},
		{"one region", east, 0, nil},
		{"region not in regions", []string{"-region", "eu-west-1"}, 2, nil},
		{"unexpected argument", []string{"page_counts"}, 2, nil},
		{"verbose", slices.Concat([]string{"-verbose"}, east), 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			m := s.metas["us-east-1"]
			u, err := m.Unit(t.Context(), "page_views", "shardId-0001")
			must(t, err)
			must(t, m.Advance(t.Context(), u, fmt.Sprintf("%040d", 250), time.Now(), func(string) bool { return true }))
			u, err = m.Unit(t.Context(), "page_views", "shardId-0001")
			must(t, err)
			_, err = m.EnsureFence(t.Context(), u, "page_counts@v1")
			must(t, err)
		}},
		{"broken", east, 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			markBroken(t, s.metas["us-east-1"], "page_counts@v1", "page_views", meta.Failure{Cause: meta.CauseRecord,
				Unit: "shardId-0001", Seq: fmt.Sprintf("%040d", 7), EventID: "e1", Error: "division by zero", At: time.Now()})
		}},
		{"broken by the time split", []string{"-region", "us-west-2"}, 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			markBroken(t, s.metas["us-west-2"], "plan_counts@v1", "signups", meta.Failure{Cause: meta.CauseSplit,
				EventID: "e2", Error: "the event landed after the split's late bound", At: time.Now()})
		}},
		{"broken retained version", east, 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.pending(t)
			markBroken(t, s.metas["us-east-1"], "plan_counts@v1", "signups", meta.Failure{Cause: meta.CauseRecord,
				EventID: "e3", Error: "division by zero", At: time.Now()})
		}},
		{"needs a backfill", nil, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.pending(t)
		}},
		{"partial", east, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.pending(t)
			m := s.metas["us-east-1"]
			_, err := m.Activate(t.Context(), "plan_counts@v2", "signups", meta.SplitTime, meta.DefaultBounds)
			must(t, err)
			must(t, m.MakeLive(t.Context(), "plan_counts@v2", time.Date(1999, 12, 30, 0, 0, 0, 0, time.UTC)))
		}},
		{"blocked", east, 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			cat := *s.cat
			cat.Views = slices.DeleteFunc(slices.Clone(cat.Views), func(v catalog.View) bool { return v.Name == "page_seen" })
			s.register(t, &cat)
			s.aws["us-east-1"].metrics["IteratorAge shop-projector"] = []float64{float64(time.Hour / time.Millisecond)}
		}},
		{"unreachable", nil, 2, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.backends["us-west-2"] = unreachable{s.backends["us-west-2"]}
		}},
		{"not deployed", nil, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.backends["us-west-2"] = mem.New()
		}},
		{"newer catalog format", east, 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			_, err := s.metas["us-east-1"].RegisterCatalog(t.Context(), []byte(`{"format":2}`), "v9.0.0")
			must(t, err)
		}},
		{"local", []string{"-endpoint", "http://127.0.0.1:8000", "-verbose"}, 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.job(t, "us-east-1", "")
			markBroken(t, s.metas["us-east-1"], "page_counts@v1", "page_views", meta.Failure{Cause: meta.CauseRecord,
				EventID: "e5", Error: "division by zero", At: time.Now()})
		}},
		{"local json", []string{"-endpoint", "http://127.0.0.1:8000", "-json"}, 0, nil},
		{"timeout zero", []string{"-timeout", "0s"}, 2, nil},
	}
}

// statusJobCases are the runs with a backfill job of plan_counts@v2.
func statusJobCases() []statusCase {
	return []statusCase{
		{"cancelled", []string{"-region", "us-west-2"}, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.job(t, "us-west-2", types.ExecutionStatusAborted)
			_, err := backfill.Cancel(t.Context(), s.metas["us-west-2"], "plan_counts@v2")
			must(t, err)
		}},
		{"running", east, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.job(t, "us-east-1", types.ExecutionStatusRunning)
			s.apply(t, 100)
		}},
		{"continuing", east, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			arn := s.job(t, "us-east-1", types.ExecutionStatusSucceeded)
			s.sfn["us-east-1"].status[arn+"-c1"] = types.ExecutionStatusRunning
			s.apply(t, 0)
		}},
		{"interrupted", east, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.job(t, "us-east-1", types.ExecutionStatusFailed)
			_, err := s.metas["us-east-1"].UpdateJob(t.Context(), "plan_counts@v2", func(j *meta.Job) (bool, error) {
				j.Error = &meta.JobError{Phase: meta.PhaseStage, Message: "AccessDenied: s3:PutObject", At: time.Now()}
				return true, nil
			})
			must(t, err)
		}},
		{"continuation missing", east, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.job(t, "us-east-1", types.ExecutionStatusSucceeded)
		}},
		{"paused", east, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.job(t, "us-east-1", types.ExecutionStatusRunning)
			s.apply(t, 0)
			_, err := s.metas["us-east-1"].UpdateJob(t.Context(), "plan_counts@v2", func(j *meta.Job) (bool, error) {
				j.Paused = true
				return true, nil
			})
			must(t, err)
		}},
		{"waits", east, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.pending(t)
			m := s.metas["us-east-1"]
			arn := s.sfn["us-east-1"].arn("us-east-1", "plan-counts-v2-0a1b2c-a1")
			_, err := backfill.Start(t.Context(), m, backfill.Request{
				Version: "plan_counts@v2", Stream: "signups", Name: "plan-counts-v2-0a1b2c", Execution: func(int) string { return arn }, Split: meta.SplitFence,
			})
			must(t, err)
			s.sfn["us-east-1"].status[arn] = types.ExecutionStatusSucceeded
			_, err = m.Activate(t.Context(), "plan_counts@v2", "signups", meta.SplitFence, meta.DefaultBounds)
			must(t, err)
			_, err = m.UpdateJob(t.Context(), "plan_counts@v2", func(j *meta.Job) (bool, error) {
				j.Phase, j.Waits = meta.PhaseWatermark, meta.WaitArchive
				return true, nil
			})
			must(t, err)
		}},
		{"waits json", slices.Concat([]string{"-json"}, east), 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.pending(t)
			m := s.metas["us-east-1"]
			_, err := backfill.Start(t.Context(), m, backfill.Request{
				Version: "plan_counts@v2", Stream: "signups", Name: "plan-counts-v2-0a1b2c", Execution: backfill.LocalExecution, Split: meta.SplitFence,
			})
			must(t, err)
			_, err = m.UpdateJob(t.Context(), "plan_counts@v2", func(j *meta.Job) (bool, error) {
				j.Phase, j.Waits, j.Paused = meta.PhaseWatermark, meta.WaitArchive, false
				return true, nil
			})
			must(t, err)
		}},
		{"jobs gaps and units json", slices.Concat([]string{"-json", "-verbose"}, east), 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.job(t, "us-east-1", types.ExecutionStatusRunning)
			s.apply(t, 0)
			time.Sleep(time.Hour)
			s.gap("uuid/shardId-0001/2000/01/01/2000-01-01T00.30.00-a", time.Date(2000, 1, 1, 0, 30, 0, 0, time.UTC))
			markBroken(t, s.metas["us-east-1"], "page_seen@v1", "page_views", meta.Failure{Cause: meta.CauseRecord,
				Unit: "shardId-0002", Seq: fmt.Sprintf("%040d", 9), Error: "division by zero", At: time.Now()})
		}},
	}
}

// statusDeliveryCases are the runs with gaps, lag, mappings or an archive to act on.
func statusDeliveryCases() []statusCase {
	return []statusCase{
		{"gaps", east, 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			before := time.Now()
			time.Sleep(time.Hour)
			s.gap("uuid/shardId-0001/2000/01/01/2000-01-01T00.30.00-a", before.Add(30*time.Minute))
			s.gap("uuid/shardId-0001/1999/12/31/1999-12-31T23.00.00-b", before.Add(-time.Hour))
			markBroken(t, s.metas["us-east-1"], "page_seen@v1", "page_views", meta.Failure{Cause: meta.CauseRecord,
				EventID: "e4", Error: "division by zero", At: time.Now()})
		}},
		{"delivery problems", east, 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			a := s.aws["us-east-1"]
			views, signups := streamARN(shop, "us-east-1", "page_views"), streamARN(shop, "us-east-1", "signups")
			a.mappings[views] = []lambdatypes.EventSourceMappingConfiguration{
				mapping("us-east-1", "page_views", "shop-projector-page_views", "Enabled", "PROBLEM: Function call failed"),
				mapping("us-east-1", "page_views", "audit", "Enabled", "OK"),
			}
			a.mappings[signups] = []lambdatypes.EventSourceMappingConfiguration{mapping("us-east-1", "signups", "shop-projector", "Disabled", "OK")}
			a.metrics["IteratorAge shop-projector-page_views"] = []float64{float64(20 * time.Minute / time.Millisecond)}
			a.metrics["DeliveryToIceberg.DataFreshness shop-archive-signups"] = []float64{1800}
			a.metrics["DeliveryToIceberg.FailedRowCount shop-archive-page_views"] = []float64{3, 4}
		}},
		// A part of the region that can't be read hides nothing read from the meta table.
		{"mappings unreadable", east, 2, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.aws["us-east-1"].fail = map[string]error{"ListEventSourceMappings": errors.New("AccessDeniedException: lambda:ListEventSourceMappings")}
			brokenSeen(t, s)
		}},
		{"metrics and executions unreadable", east, 2, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.aws["us-east-1"].fail = map[string]error{"GetMetricData": errors.New("ThrottlingException: Rate exceeded")}
			s.job(t, "us-east-1", types.ExecutionStatusRunning)
			s.sfn["us-east-1"].fail = errors.New("AccessDeniedException: states:DescribeExecution")
			brokenSeen(t, s)
		}},
		{"gaps unreadable", east, 2, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.aws["us-east-1"].fail = map[string]error{"ListObjectsV2": errors.New("AccessDenied: s3:ListBucket")}
			brokenSeen(t, s)
		}},
		{"a gap record unreadable", east, 2, func(t *testing.T, s *statusStore) {
			t.Helper()
			s.aws["us-east-1"].objects["aws/lambda/uuid/shardId-0001/2000/01/01/2000-01-01T00.30.00-z"] = "not a record"
			brokenSeen(t, s)
		}},
		{"older gaps", east, 0, func(t *testing.T, s *statusStore) {
			t.Helper()
			before := time.Now()
			time.Sleep(time.Hour)
			s.gap("uuid/shardId-0001/1999/12/31/1999-12-31T20.00.00-b", before.Add(-4*time.Hour))
		}},
		{"delivery problems json", slices.Concat([]string{"-json"}, east), 1, func(t *testing.T, s *statusStore) {
			t.Helper()
			delete(s.aws["us-east-1"].mappings, streamARN(shop, "us-east-1", "signups"))
			s.aws["us-east-1"].metrics["DeliveryToIceberg.FailedRowCount shop-archive-page_views"] = []float64{1}
		}},
	}
}

// apply activates plan_counts@v2 in us-east-1 and moves its job to the apply phase, a
// quarter done, at rate.
func (s *statusStore) apply(t *testing.T, rate int) {
	t.Helper()
	m := s.metas["us-east-1"]
	_, err := m.Activate(t.Context(), "plan_counts@v2", "signups", meta.SplitTime, meta.DefaultBounds)
	must(t, err)
	_, err = m.UpdateJob(t.Context(), "plan_counts@v2", func(j *meta.Job) (bool, error) {
		j.Phase, j.Staged, j.Applied, j.Skipped, j.Rate = meta.PhaseApply, 5000, 1200, 50, rate
		return true, nil
	})
	must(t, err)
}

// brokenSeen breaks page_seen@v1 in us-east-1.
func brokenSeen(t *testing.T, s *statusStore) {
	t.Helper()
	markBroken(t, s.metas["us-east-1"], "page_seen@v1", "page_views", meta.Failure{Cause: meta.CauseRecord,
		EventID: "e6", Error: "division by zero", At: time.Now()})
}

func markBroken(t *testing.T, m *meta.Meta, version, stream string, f meta.Failure) {
	t.Helper()
	_, _, err := m.MarkBroken(t.Context(), version, stream, f)
	must(t, err)
}

// must fails the test on err.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// fakeAWS answers status's calls in a region besides DynamoDB's tables.
type fakeAWS struct {
	bucket   string
	arns     map[string]string                                        // a stream table → its stream's ARN
	mappings map[string][]lambdatypes.EventSourceMappingConfiguration // by stream ARN
	metrics  map[string][]float64                                     // by metric and dimension value, newest first
	objects  map[string]string                                        // the bucket's objects
	fail     map[string]error                                         // by operation
}

func (f *fakeAWS) DescribeTable(_ context.Context, in *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	arn, ok := f.arns[aws.ToString(in.TableName)]
	if !ok {
		return nil, &ddbtypes.ResourceNotFoundException{Message: aws.String("Requested resource not found")}
	}
	return &dynamodb.DescribeTableOutput{Table: &ddbtypes.TableDescription{
		TableName: in.TableName, LatestStreamArn: aws.String(arn), StreamSpecification: &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true)},
	}}, nil
}

func (f *fakeAWS) ListEventSourceMappings(_ context.Context, in *lambda.ListEventSourceMappingsInput, _ ...func(*lambda.Options)) (*lambda.ListEventSourceMappingsOutput, error) {
	if err := f.fail["ListEventSourceMappings"]; err != nil {
		return nil, err
	}
	return &lambda.ListEventSourceMappingsOutput{EventSourceMappings: f.mappings[aws.ToString(in.EventSourceArn)]}, nil
}

func (f *fakeAWS) GetMetricData(_ context.Context, in *cloudwatch.GetMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	if err := f.fail["GetMetricData"]; err != nil {
		return nil, err
	}
	out := &cloudwatch.GetMetricDataOutput{}
	for _, q := range in.MetricDataQueries {
		m := q.MetricStat.Metric
		out.MetricDataResults = append(out.MetricDataResults,
			cwtypes.MetricDataResult{Id: q.Id, Values: f.metrics[aws.ToString(m.MetricName)+" "+aws.ToString(m.Dimensions[0].Value)]})
	}
	return out, nil
}

func (f *fakeAWS) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if err := f.fail["ListObjectsV2"]; err != nil {
		return nil, err
	}
	if aws.ToString(in.Bucket) != f.bucket {
		return nil, &s3types.NoSuchBucket{Message: aws.String("The specified bucket does not exist")}
	}
	out := &s3.ListObjectsV2Output{}
	for _, k := range slices.Sorted(maps.Keys(f.objects)) {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) {
			out.Contents = append(out.Contents, s3types.Object{Key: aws.String(k)})
		}
	}
	return out, nil
}

func (f *fakeAWS) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	body, ok := f.objects[aws.ToString(in.Key)]
	if !ok || aws.ToString(in.Bucket) != f.bucket {
		return nil, &s3types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(body))}, nil
}

// fakeUnits lists each stream table's shards.
type fakeUnits map[string][]string

func (f fakeUnits) Units(_ context.Context, table string) ([]string, error) { return f[table], nil }

// unreachable is a region whose DynamoDB can't be reached.
type unreachable struct{ store.Backend }

func (unreachable) Get(context.Context, string, store.Key, bool) (store.Item, error) {
	return nil, errors.New("operation error DynamoDB: GetItem, dial tcp 10.0.0.1:443: i/o timeout")
}
