// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"maps"
	"strings"
	"unicode"

	"github.com/lucindo/dynaflux/internal/catalog"
)

// Fixed values of the alarms (docs/spec/infra.md §10).
const (
	maxAlarmName       = 255
	alarmPeriodSeconds = 300
	// sustained is how many periods in a row a warning, or the polling heartbeat, waits.
	sustained = 3
	// A projector 6 hours behind has 18 hours left before the stream trims records it
	// hasn't applied.
	lagMillis     = 15 * 60 * 1000
	gapRiskMillis = 6 * 60 * 60 * 1000
	// freshnessSeconds is three Firehose buffer intervals.
	freshnessSeconds = 3 * archiveBufferSeconds
)

// CloudWatch names that the alarms and the dashboard share.
const (
	namespaceLambda     = "AWS/Lambda"
	namespaceDynamoDB   = "AWS/DynamoDB"
	namespaceFirehose   = "AWS/Firehose"
	dimFunctionName     = "FunctionName"
	dimTableName        = "TableName"
	dimDeliveryStream   = "DeliveryStreamName"
	statSum             = "Sum"
	statMaximum         = "Maximum"
	metricIteratorAge   = "IteratorAge"
	metricErrors        = "Errors"
	metricThrottles     = "Throttles"
	metricDataFreshness = "DeliveryToIceberg.DataFreshness"
	metricFailedRows    = "DeliveryToIceberg.FailedRowCount"
)

// rule is what an alarm watches for: by default, its metric at least threshold in periods
// periods in a row.
type rule struct {
	signal      string // PascalCase, such as GapRisk
	severity    Severity
	threshold   int
	periods     int
	description string
}

// alarms returns the alarms of the regional scope s: four per projector, three per stream,
// two for the store, and two per archived stream.
func (b *builder) alarms(s *Scope) []Alarm {
	var alarms []Alarm
	for _, f := range s.Functions {
		if f.Mode == catalog.EnvModeProjector {
			alarms = append(alarms, b.projectorAlarms(f)...)
		}
	}
	for _, st := range b.cat.Streams {
		alarms = append(alarms, b.streamAlarms(st.Name)...)
		if st.Archive != nil {
			alarms = append(alarms, b.archiveAlarms(st.Name)...)
		}
	}
	alarms = append(alarms, b.storeAlarms()...)
	sortByID(alarms)
	return alarms
}

func (b *builder) projectorAlarms(f Function) []Alarm {
	fn := Dimension{Name: dimFunctionName, Value: ref(f.ID, AttrName)}
	age := Metric{Namespace: namespaceLambda, Name: metricIteratorAge, Statistic: statMaximum, Dimensions: []Dimension{fn}}
	sum := func(name string) Metric {
		return Metric{Namespace: namespaceLambda, Name: name, Statistic: statSum, Dimensions: []Dimension{fn}}
	}
	alarm := func(r rule, m Metric) Alarm {
		return b.alarm(f.ID, string(f.Name), maps.Clone(f.Tags), r, m)
	}
	return []Alarm{
		alarm(rule{"Lag", SeverityWarning, lagMillis, sustained,
			"the projector is more than 15 minutes behind, so the views are stale. " +
				"The store's dashboard shows the projector's errors and throttles and the tables' throttles."}, age),
		alarm(rule{"GapRisk", SeverityCritical, gapRiskMillis, 1,
			"the projector is more than 6 hours behind. The stream trims records after 24 hours, " +
				"and a record trimmed before the projector applies it leaves a hole in the views. " +
				"Fix the projector's errors or throttles now."}, age),
		alarm(rule{"Errors", SeverityWarning, 1, sustained,
			"the projector has failed for 15 minutes, and the shards it reads wait. " +
				"A deploy that changes the archive schema causes brief failures; otherwise read the projector's log group."},
			sum(metricErrors)),
		alarm(rule{"Throttles", SeverityWarning, 1, sustained,
			"Lambda has throttled the projector for 15 minutes. Raise the account's concurrent executions quota."},
			sum(metricThrottles)),
	}
}

// streamAlarms returns the alarms on a stream's event source mapping and on its table's
// replica in the region.
func (b *builder) streamAlarms(stream string) []Alarm {
	mapping := mappingID(stream)
	uuid := []Dimension{{Name: "EventSourceMappingUUID", Value: ref(mapping, AttrUUID)}}
	// The mapping's alarms are named after the stream's projector, whatever runtime.split is.
	subject := b.naming.StreamProjectorFunction(stream)
	tags := b.tags(TagKindProjector, stream)
	// With unlimited retries and record age, Lambda counts no record that the stream trims.
	discarded := b.alarm(mapping, subject, tags, rule{"Discarded", SeverityCritical, 1, 1,
		"the event source mapping of stream " + stream + " sent records to its on-failure destination, " +
			"under aws/lambda/ in the store's bucket, so the views miss them. " +
			"Records that the stream trims after 24 hours aren't counted here: the projector's gap-risk alarm warns before that."},
		Metric{Namespace: namespaceLambda, Name: "OnFailureDestinationDeliveredEventCount", Statistic: statSum, Dimensions: uuid},
		Metric{Namespace: namespaceLambda, Name: "DroppedEventCount", Statistic: statSum, Dimensions: uuid})
	// A mapping counts its polls even when they find nothing, so no data means no polling.
	polling := b.alarm(mapping, subject, maps.Clone(tags), rule{"Polling", SeverityCritical, 1, sustained,
		"the event source mapping of stream " + stream + " hasn't polled for 15 minutes: it is disabled, " +
			"or the projector can't read the stream. Check the mapping's state and the projector's role."},
		Metric{Namespace: namespaceLambda, Name: "PolledEventCount", Statistic: "SampleCount", Dimensions: uuid})
	polling.Comparison, polling.TreatMissing = ComparisonBelow, MissingBreaching
	table := streamID(stream)
	throttles := b.alarm(table, b.naming.StreamTable(stream), b.tags(TagKindStream, stream),
		rule{"WriteThrottles", SeverityWarning, 1, sustained,
			"DynamoDB has throttled writes to the table of stream " + stream + " for 15 minutes, so writers retry or fail. " +
				"Check its capacity."},
		Metric{Namespace: namespaceDynamoDB, Name: "WriteThrottleEvents", Statistic: statSum,
			Dimensions: []Dimension{{Name: dimTableName, Value: ref(table, AttrName)}}})
	return []Alarm{discarded, polling, throttles}
}

// storeAlarms returns the alarms on the runtime's metrics.
func (b *builder) storeAlarms() []Alarm {
	store := []Dimension{{Name: catalog.MetricDimensionStore, Value: Text(b.naming.Base())}}
	alarm := func(r rule, name string) Alarm {
		return b.alarm("", b.naming.Base(), b.tags(TagKindView, ""), r,
			Metric{Namespace: catalog.MetricNamespace, Name: name, Statistic: statSum, Dimensions: store})
	}
	return []Alarm{
		alarm(rule{"ViewsBroken", SeverityCritical, 1, 1,
			"a view stopped at an event it can't apply, and the other views go on. The projector's log names the view and the event."},
			catalog.MetricBreakerOpened),
		alarm(rule{"RowsTooLarge", SeverityCritical, 1, 1,
			"a view row is over DynamoDB's 400 KB item limit, so the projector can't write it and its shard waits. " +
				"The projector's log names the view and the row."},
			catalog.MetricRowTooLarge),
	}
}

// archiveAlarms returns the alarms on the Firehose stream that archives a stream.
func (b *builder) archiveAlarms(stream string) []Alarm {
	id := deliveryStreamID(stream)
	name := b.naming.ArchiveStream(stream)
	metric := func(name, statistic string) Metric {
		return Metric{Namespace: namespaceFirehose, Name: name, Statistic: statistic,
			Dimensions: []Dimension{{Name: dimDeliveryStream, Value: ref(id, AttrName)}}}
	}
	alarms := []Alarm{
		b.alarm(id, name, b.tags(TagKindArchive, stream), rule{"Freshness", SeverityWarning, freshnessSeconds, sustained,
			"Firehose has held records of stream " + stream + " for more than 15 minutes without writing them to the archive. " +
				"Read its log group."},
			metric(metricDataFreshness, statMaximum)),
		b.alarm(id, name, b.tags(TagKindArchive, stream), rule{"FailedRows", SeverityCritical, 1, 1,
			"Firehose couldn't write rows of stream " + stream + " to its archive table, so the archive misses them. " +
				"They are under " + firehoseErrorPrefix(stream) + " in the store's bucket."},
			metric(metricFailedRows, statSum)),
	}
	for i := range alarms {
		alarms[i].ArchiveOnly = b.archiveOnly
	}
	return alarms
}

// alarm returns the alarm on the sum of metrics of the resource with ID subjectID and name
// subject, or of the store when subjectID is empty. It checks every 5 minutes and treats
// missing data as not breaching.
func (b *builder) alarm(subjectID, subject string, tags map[string]string, r rule, metrics ...Metric) Alarm {
	return Alarm{
		Common:            Common{ID: alarmID(r.signal, subjectID), Tags: tags},
		Name:              b.fit(maxAlarmName, subject+"-"+kebab(r.signal)),
		Severity:          r.severity,
		Description:       string(r.severity) + ": " + r.description,
		Metrics:           metrics,
		PeriodSeconds:     alarmPeriodSeconds,
		EvaluationPeriods: r.periods,
		Threshold:         r.threshold,
		Comparison:        ComparisonAtLeast,
		TreatMissing:      MissingNotBreaching,
	}
}

// kebab returns the form of a signal in alarm names: GapRisk becomes gap-risk.
func kebab(signal string) string {
	var s strings.Builder
	for i, r := range signal {
		if unicode.IsUpper(r) && i > 0 {
			s.WriteByte('-')
		}
		s.WriteRune(unicode.ToLower(r))
	}
	return s.String()
}
