// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"slices"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
)

// The parameters of a custom domain, which ${domain} and ${hosted_zone} become.
const (
	domainName   = "DomainName"
	hostedZoneID = "HostedZoneId"
)

// queryTemplate returns query.yaml: the query scope, whose role names the tables of the
// global and regional templates. The alarms notify the SNS topic of the AlarmTopicArn
// parameter, if it is set.
func queryTemplate(m *infra.Manifest) (*resources, *yaml.Node, error) {
	s := m.Query
	r := &resolver{local: map[string]bool{}, remote: map[string]infra.Table{}, refARN: map[string]bool{}}
	for _, t := range slices.Concat(m.Global.Tables, m.Regional.Tables) {
		r.remote[t.ID] = t
	}
	for _, id := range s.IDs() {
		r.local[id] = true
	}
	for _, c := range s.Certificates {
		r.refARN[c.ID] = true
	}
	rs := newResources(r)
	for _, l := range s.LogGroups {
		rs.add(l.ID, func() *yaml.Node { return logGroup(r, l) })
	}
	for _, x := range s.Roles {
		addRole(rs, r, x)
	}
	for _, f := range s.Functions {
		rs.add(f.ID, func() *yaml.Node { return function(r, f) })
	}
	for _, u := range s.FunctionURLs {
		rs.add(u.ID, func() *yaml.Node { return functionURL(r, u) })
	}
	addDoors(rs, r, s)
	for _, p := range s.Permissions {
		rs.add(p.ID, func() *yaml.Node { return permission(r, p) })
	}
	for _, x := range s.Alarms {
		rs.add(x.ID, func() *yaml.Node { return alarm(r, x) })
	}
	outputs := mapping()
	for _, o := range s.Outputs {
		outputs.Content = append(outputs.Content, str(o.Name), mapping(kv("Description", str(o.Description)), kv("Value", r.value(o.Value))))
	}
	if rs.err == nil {
		rs.err = r.err
	}
	if rs.err != nil {
		return nil, nil, rs.err
	}
	parameters := mapping(kv("Prefix", prefixParameter(m.Prefix)), kv(alarmTopicArn, alarmTopicParameter()))
	if m.Domain != "" {
		parameters.Content = append(parameters.Content, domainParameters(m.Domain).Content...)
	}
	return rs, template("Dynaflux store "+m.Store+": the query function and its front door. Deploy in every region of the store "+
		"with the application, after regional.yaml and the backfill.", true,
		kv("Parameters", parameters),
		kv("Conditions", mapping(kv(hasAlarmTopicArn, hasAlarmTopic()))),
		kv("Resources", rs.node),
		kv("Outputs", outputs),
	), nil
}

// domainParameters returns the parameters of a custom domain whose default name is def.
func domainParameters(def string) *yaml.Node {
	return mapping(
		kv(domainName, mapping(
			kv("Type", str("String")),
			kv("Default", str(def)),
			kv("Description", str("The custom domain of the API, in the hosted zone HostedZoneId. Each environment needs its own.")),
		)),
		kv(hostedZoneID, mapping(
			kv("Type", str("AWS::Route53::HostedZone::Id")),
			kv("Description", str("The ID of the public Route 53 hosted zone that holds DomainName.")),
		)),
	)
}

// functionURL returns a function URL. A public one tells checkov why: CKV_AWS_258 flags
// AuthType NONE, which http.allow_public confirmed.
func functionURL(r *resolver, u infra.FunctionURL) *yaml.Node {
	var metadata []pair
	if u.Auth == infra.URLAuthNone {
		metadata = append(metadata, checkovSkip("CKV_AWS_258",
			"http.auth none, confirmed by http.allow_public: the exposed queries are public by choice (DF6603)."))
	}
	return resource("AWS::Lambda::Url", mapping(
		kv("TargetFunctionArn", r.value(u.Function)),
		kv("AuthType", str(urlAuthType(u.Auth))),
	), metadata...)
}

// checkovSkip returns the Metadata that tells checkov to skip check id on a resource, and
// why.
func checkovSkip(id, comment string) pair {
	return kv("Metadata", mapping(kv("checkov", mapping(kv("skip", seq(mapping(
		kv("id", str(id)),
		kv("comment", str(comment)),
	)))))))
}

func permission(r *resolver, p infra.Permission) *yaml.Node {
	var urlAuth, viaURL, source *yaml.Node
	if p.URLAuth != "" {
		urlAuth = str(urlAuthType(p.URLAuth))
	}
	if p.ViaURL {
		viaURL = boolean(true)
	}
	if p.SourceARN != "" {
		source = r.value(p.SourceARN)
	}
	return resource("AWS::Lambda::Permission", mapping(
		kv("FunctionName", r.value(p.Function)),
		kv("Action", str(p.Action)),
		kv("Principal", str(p.Principal)),
		kv("SourceArn", source),
		kv("FunctionUrlAuthType", urlAuth),
		kv("InvokedViaFunctionUrl", viaURL),
	))
}

func urlAuthType(a infra.URLAuth) string {
	if a == infra.URLAuthNone {
		return "NONE"
	}
	return "AWS_IAM"
}
