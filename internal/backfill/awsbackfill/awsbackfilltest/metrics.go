// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfilltest

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
)

// Metrics answers GetMetricData and ListMetrics in the RPC v2 CBOR protocol from the series
// a test sets. A query reads the series with its namespace, name and exact dimensions; its
// statistic is the series' value. It is safe for concurrent use.
type Metrics struct {
	mu     sync.Mutex
	now    func() time.Time
	series []series
}

// series is a metric by its dimensions, with its value at the start of each period, and the
// StatusCode of its results: Complete unless a test sets another.
type series struct {
	namespace, metric string
	dims              map[string]string
	value             func(at time.Time) (float64, bool)
	status            string
}

// NewMetrics returns Metrics without series, whose clock is now: a series has datapoints only
// for periods that start before it.
func NewMetrics(now func() time.Time) *Metrics {
	return &Metrics{now: now}
}

// Set gives the metric with dims, in namespace, a datapoint for each period whose start value
// returns true for, of the value it returns.
func (m *Metrics) Set(namespace, metric string, dims map[string]string, value func(at time.Time) (float64, bool)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.series = append(m.series, series{namespace: namespace, metric: metric, dims: dims, value: value, status: "Complete"})
}

// Fail makes the metric with dims, in namespace, return no datapoints with status, such as
// InternalError.
func (m *Metrics) Fail(namespace, metric string, dims map[string]string, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	none := func(time.Time) (float64, bool) { return 0, false }
	m.series = append(m.series, series{namespace: namespace, metric: metric, dims: dims, value: none, status: status})
}

// metricsRequest is what the fake reads of a GetMetricData or ListMetrics request.
type metricsRequest struct {
	metric

	MetricDataQueries []struct {
		ID         string `json:"Id"`
		MetricStat struct {
			Metric metric `json:"Metric"`
			Period int64  `json:"Period"`
		} `json:"MetricStat"`
	} `json:"MetricDataQueries"`
	// StartTime and EndTime are seconds since 1970.
	StartTime float64 `json:"StartTime"`
	EndTime   float64 `json:"EndTime"`
}

// metric is a metric, or a filter of metrics.
type metric struct {
	Namespace  string `json:"Namespace"`
	MetricName string `json:"MetricName"`
	Dimensions []struct {
		Name  string `json:"Name"`
		Value string `json:"Value"`
	} `json:"Dimensions"`
}

func (m metric) dims() map[string]string {
	out := map[string]string{}
	for _, d := range m.Dimensions {
		out[d.Name] = d.Value
	}
	return out
}

// ServeHTTP implements [http.Handler].
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	in, err := decodeCBOR(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out cbor.Map
	switch {
	case strings.HasSuffix(r.URL.Path, "/operation/GetMetricData"):
		out = m.data(in)
	case strings.HasSuffix(r.URL.Path, "/operation/ListMetrics"):
		out = m.list(in.metric)
	default:
		http.Error(w, "the fake doesn't serve "+r.URL.Path, http.StatusBadRequest)
		return
	}
	w.Header().Set("Smithy-Protocol", "rpc-v2-cbor")
	w.Header().Set("Content-Type", "application/cbor")
	w.Write(cbor.Encode(out)) //nolint:errcheck,gosec // the client sees a short body
}

// decodeCBOR reads a request's CBOR body into a metricsRequest, by way of JSON.
func decodeCBOR(body io.Reader) (metricsRequest, error) {
	var in metricsRequest
	data, err := io.ReadAll(body)
	if err != nil {
		return in, err
	}
	v, err := cbor.Decode(data)
	if err != nil {
		return in, fmt.Errorf("a request that isn't CBOR: %w", err)
	}
	if data, err = json.Marshal(plain(v)); err == nil {
		err = json.Unmarshal(data, &in)
	}
	return in, err
}

// plain returns v as the values that encoding/json writes, a timestamp as seconds.
func plain(v cbor.Value) any {
	switch v := v.(type) {
	case cbor.Map:
		out := map[string]any{}
		for k, e := range v {
			out[k] = plain(e)
		}
		return out
	case cbor.List:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = plain(e)
		}
		return out
	case *cbor.Tag:
		return plain(v.Value)
	case cbor.String:
		return string(v)
	case cbor.Uint:
		return uint64(v)
	case cbor.Float64:
		return float64(v)
	case cbor.Float32:
		return float64(v)
	}
	return nil
}

// data answers GetMetricData: each query's datapoints in [StartTime, EndTime), one page.
func (m *Metrics) data(in metricsRequest) cbor.Map {
	start, end := seconds(in.StartTime), seconds(in.EndTime)
	results := cbor.List{}
	for _, q := range in.MetricDataQueries {
		times, values, status := cbor.List{}, cbor.List{}, "Complete"
		if s, found := m.find(q.MetricStat.Metric); found && q.MetricStat.Period > 0 {
			status = s.status
			step := time.Duration(q.MetricStat.Period) * time.Second
			for at := start.Truncate(step); at.Before(end) && at.Before(m.now()); at = at.Add(step) {
				if v, has := s.value(at); has && !at.Before(start) {
					times = append(times, &cbor.Tag{ID: 1, Value: cbor.Float64(float64(at.UnixMicro()) / float64(time.Second/time.Microsecond))})
					values = append(values, cbor.Float64(v))
				}
			}
		}
		results = append(results, cbor.Map{"Id": cbor.String(q.ID), "StatusCode": cbor.String(status), "Timestamps": times, "Values": values})
	}
	return cbor.Map{"MetricDataResults": results}
}

func seconds(s float64) time.Time {
	return time.UnixMicro(int64(s * float64(time.Second/time.Microsecond))).UTC()
}

func (m *Metrics) find(x metric) (series, bool) {
	i := slices.IndexFunc(m.series, func(s series) bool {
		return s.namespace == x.Namespace && s.metric == x.MetricName && maps.Equal(s.dims, x.dims())
	})
	if i < 0 {
		return series{}, false
	}
	return m.series[i], true
}

// list answers ListMetrics: the series whose namespace and name match, and that have every
// dimension of the filter.
func (m *Metrics) list(filter metric) cbor.Map {
	listed := cbor.List{}
	for _, s := range m.series {
		matched := s.namespace == filter.Namespace && s.metric == filter.MetricName
		for k, v := range filter.dims() {
			matched = matched && s.dims[k] == v
		}
		if !matched {
			continue
		}
		dims := cbor.List{}
		for _, k := range slices.Sorted(maps.Keys(s.dims)) {
			dims = append(dims, cbor.Map{"Name": cbor.String(k), "Value": cbor.String(s.dims[k])})
		}
		listed = append(listed, cbor.Map{"Namespace": cbor.String(s.namespace), "MetricName": cbor.String(s.metric), "Dimensions": dims})
	}
	return cbor.Map{"Metrics": listed}
}
