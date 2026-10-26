// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"strconv"
	"strings"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
)

// The optional SNS topic of the alarms, chosen at deploy time.
const (
	alarmTopicArn    = "AlarmTopicArn"
	hasAlarmTopicArn = "HasAlarmTopicArn"
)

func alarmTopicParameter() *yaml.Node {
	return mapping(
		kv("Type", str("String")),
		kv("Default", str("")),
		kv("Description", str("ARN of the SNS topic, in the stack's region, that the alarms notify. Empty: the alarms notify no one.")),
	)
}

// hasAlarmTopic is the condition that holds when the stack has an alarm topic.
func hasAlarmTopic() *yaml.Node {
	return intrinsic("Not", seq(intrinsic("Equals", seq(intrinsic("Ref", str(alarmTopicArn)), str("")))))
}

func alarm(r *resolver, a infra.Alarm) *yaml.Node {
	props := []pair{kv("AlarmName", r.value(a.Name)), kv("AlarmDescription", str(a.Description))}
	if len(a.Metrics) == 1 {
		m := a.Metrics[0]
		props = append(props,
			kv("Namespace", str(m.Namespace)),
			kv("MetricName", str(m.Name)),
			kv("Dimensions", dimensions(r, m.Dimensions)),
			kv("Statistic", str(m.Statistic)),
			kv("Period", num(a.PeriodSeconds)),
		)
	} else {
		props = append(props, kv("Metrics", metricSum(r, a)))
	}
	comparison, missing := "GreaterThanOrEqualToThreshold", "notBreaching"
	switch a.Comparison {
	case infra.ComparisonBelow:
		comparison = "LessThanThreshold"
	case infra.ComparisonAtLeast:
	}
	switch a.TreatMissing {
	case infra.MissingBreaching:
		missing = "breaching"
	case infra.MissingNotBreaching:
	}
	actions := intrinsic("If", seq(str(hasAlarmTopicArn), seq(intrinsic("Ref", str(alarmTopicArn))), intrinsic("Ref", str("AWS::NoValue"))))
	props = append(props,
		kv("EvaluationPeriods", num(a.EvaluationPeriods)),
		kv("Threshold", num(a.Threshold)),
		kv("ComparisonOperator", str(comparison)),
		kv("TreatMissingData", str(missing)),
		kv("AlarmActions", actions),
		kv("OKActions", actions),
		kv("Tags", tags(a.Tags)),
	)
	return resource("AWS::CloudWatch::Alarm", mapping(props...), archiveCondition(a.Common)...)
}

// metricSum returns the Metrics of an alarm on the sum of its metrics. Metric math counts
// a missing value as 0.
func metricSum(r *resolver, a infra.Alarm) *yaml.Node {
	ids := make([]string, len(a.Metrics))
	stats := make([]*yaml.Node, len(a.Metrics))
	for i, m := range a.Metrics {
		ids[i] = "m" + strconv.Itoa(i+1)
		stats[i] = mapping(
			kv("Id", str(ids[i])),
			kv("ReturnData", boolean(false)),
			kv("MetricStat", mapping(
				kv("Metric", mapping(kv("Namespace", str(m.Namespace)), kv("MetricName", str(m.Name)), kv("Dimensions", dimensions(r, m.Dimensions)))),
				kv("Period", num(a.PeriodSeconds)),
				kv("Stat", str(m.Statistic)),
			)),
		)
	}
	total := mapping(kv("Id", str("total")), kv("Expression", str(strings.Join(ids, " + "))), kv("ReturnData", boolean(true)))
	return seq(append([]*yaml.Node{total}, stats...)...)
}

func dimensions(r *resolver, ds []infra.Dimension) *yaml.Node {
	n := seq()
	for _, d := range ds {
		n.Content = append(n.Content, mapping(kv("Name", str(d.Name)), kv("Value", r.value(d.Value))))
	}
	return n
}

// dashboard returns a dashboard, its body as a literal block, which keeps a widget per line.
func dashboard(r *resolver, d infra.Dashboard) *yaml.Node {
	body := r.value(d.Body)
	body.Style = yaml.LiteralStyle
	return resource("AWS::CloudWatch::Dashboard", mapping(
		kv("DashboardName", r.value(d.Name)),
		kv("DashboardBody", body),
	))
}
