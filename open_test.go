// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynaflux_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/lucindo/dynaflux"
	"github.com/lucindo/dynaflux/fluxgen"
	"github.com/lucindo/dynaflux/internal/catalog/catalogtest"
)

// loopback is where test clients would send requests: no test reaches AWS.
const loopback = "http://127.0.0.1:1"

func awsConfig(region string, c aws.HTTPClient) aws.Config {
	return aws.Config{
		Region:       region,
		BaseEndpoint: new(loopback),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "fake", SecretAccessKey: "fake"}, nil
		}),
		HTTPClient: c,
	}
}

func TestOpenChecksConfig(t *testing.T) {
	t.Parallel()
	badPrefix := func(p string) string {
		return `dynaflux: Config.Prefix is "` + p + `"; use lowercase letters, digits and -, ` +
			`starting with a letter and ending with -, such as prod-, or "" for none`
	}
	tests := []struct {
		name, region, prefix string
		want                 string // the error; "" for none
	}{
		{"no region", "", "prod-", "dynaflux: Config.AWS has no Region"},
		{
			"FIPS pseudo-region", "fips-us-east-1", "",
			`dynaflux: Config.AWS.Region "fips-us-east-1" is a FIPS pseudo-region; ` +
				"name the region, such as us-east-1, and set AWS_USE_FIPS_ENDPOINT=true to use FIPS endpoints",
		},
		{
			"FIPS suffix", "us-east-1-fips", "",
			`dynaflux: Config.AWS.Region "us-east-1-fips" is a FIPS pseudo-region; ` +
				"name the region, such as us-east-1, and set AWS_USE_FIPS_ENDPOINT=true to use FIPS endpoints",
		},
		{
			"uppercase region", "US-EAST-1", "",
			`dynaflux: Config.AWS.Region "US-EAST-1" isn't an AWS region code; use a code such as us-east-1`,
		},
		{"no prefix", "us-east-1", "", ""},
		{"prefix", "us-east-1", "prod-", ""},
		{"prefix with digits and dashes", "us-east-1", "a1-b-", ""},
		{"prefix without a final dash", "us-east-1", "prod", badPrefix("prod")},
		{"uppercase prefix", "us-east-1", "Prod-", badPrefix("Prod-")},
		{"prefix starting with a digit", "us-east-1", "1prod-", badPrefix("1prod-")},
		{"prefix that is only a dash", "us-east-1", "-", badPrefix("-")},
		{"prefix with an underscore", "us-east-1", "prod_-", badPrefix("prod_-")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := dynaflux.Open(dynaflux.Config{AWS: awsConfig(tt.region, nil), Prefix: tt.prefix})
			switch {
			case tt.want == "" && (err != nil || s == nil):
				t.Errorf("Open = %v, %v; want a store", s, err)
			case tt.want != "" && (err == nil || err.Error() != tt.want || s != nil):
				t.Errorf("Open = %v, %v; want the error %q", s, err, tt.want)
			}
		})
	}
}

// sent is an aws.HTTPClient that answers every request with an empty success and keeps
// what each one asked.
type sent []string

func (s *sent) Do(r *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	*s = append(*s, r.Header.Get("X-Amz-Target")+" "+string(body))
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader("{}")),
		Request:    r,
	}, nil
}

func TestOpenSendsNothingUntilAnAppend(t *testing.T) {
	t.Parallel()
	var calls sent
	s, err := dynaflux.Open(dynaflux.Config{AWS: awsConfig("eu-west-1", &calls), Prefix: "prod-"})
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 {
		t.Fatalf("Open sent %q; want nothing", calls)
	}
	var (
		user, eventID, origin = "u1", "", ""
		eventTime, ingestTime time.Time
	)
	ev := []any{&user, &eventID, &eventTime, &ingestTime, &origin}
	if _, err = fluxgen.AppendEvent(t.Context(), s, pageViews(t, "us-east-1, eu-west-1"), "page_views", ev); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 {
		t.Fatalf("an append sent %d requests; want 1", len(calls))
	}
	target, body, _ := strings.Cut(calls[0], " ")
	var put struct {
		TableName string `json:"TableName"`
		Item      struct {
			OriginRegion struct {
				S string `json:"S"`
			} `json:"_r"`
		} `json:"Item"`
	}
	if err = json.Unmarshal([]byte(body), &put); err != nil {
		t.Fatalf("%s: %v", body, err)
	}
	got := []string{target, put.TableName, put.Item.OriginRegion.S}
	want := []string{"DynamoDB_20120810.PutItem", "prod-analytics-stream-page_views", "eu-west-1"}
	if !slices.Equal(got, want) {
		t.Errorf("the append sent (target, table, origin_region) %q; want %q", got, want)
	}
}

// pageView and its fields are what dynaflux generate writes for page_views.
type pageView struct {
	UserID       string
	EventTime    time.Time
	EventID      string
	IngestTime   time.Time
	OriginRegion string
}

func (e *pageView) fields() []any {
	return []any{&e.UserID, &e.EventID, &e.EventTime, &e.IngestTime, &e.OriginRegion}
}

// pageViews returns the catalog of store analytics deployed in regions, whose stream
// page_views has a query, Visits.
func pageViews(t *testing.T, regions string) *dynaflux.Catalog {
	t.Helper()
	return fluxgen.LoadCatalog(string(catalogtest.MarshalFS(t, fstest.MapFS{
		"dynaflux.yaml": {Data: []byte("store: analytics\nregions: [" + regions + "]\n")},
		"dynaflux/store.sql": {Data: []byte(`CREATE STREAM page_views (user_id String) PRIMARY KEY (user_id);

-- name: Visits :many
SELECT user_id FROM page_views WHERE user_id = {user:String};`)},
	})))
}

// A Store opened in a region that the catalog doesn't list fails each call over it before
// any request: its tables don't exist there.
func TestStoreRefusesARegionTheCatalogDoesNotList(t *testing.T) {
	t.Parallel()
	var calls sent
	s, err := dynaflux.Open(dynaflux.Config{AWS: awsConfig("eu-west-1", &calls), Prefix: "prod-"})
	if err != nil {
		t.Fatal(err)
	}
	cat := pageViews(t, "us-west-2, us-east-1")
	ev := pageView{UserID: "u1"}
	_, appendErr := fluxgen.AppendEvent(t.Context(), s, cat, "page_views", ev.fields())
	_, queryErr := fluxgen.QueryRows(t.Context(), s, cat, "Visits", []any{&ev.UserID}, func(u *string) []any { return []any{u} })
	var readErr error
	for _, err := range fluxgen.ReadEvents(t.Context(), s, cat, "page_views", []any{&ev.UserID}, (*pageView).fields) {
		readErr = err
	}
	const want = "dynaflux: store analytics has no region eu-west-1; its regions are us-east-1, us-west-2"
	for name, err := range map[string]error{"append": appendErr, "query": queryErr, "read": readErr} {
		if err == nil || err.Error() != want {
			t.Errorf("%s: %v, want %q", name, err, want)
		}
	}
	if len(calls) != 0 {
		t.Errorf("the calls sent %q; want nothing", calls)
	}
}
