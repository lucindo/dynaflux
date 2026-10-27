// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra_test

import (
	"testing"

	"github.com/lucindo/dynaflux/internal/infra"
)

func TestResolve(t *testing.T) {
	t.Parallel()
	m, err := buildWith(t, "store: analytics\nregions: [us-east-1]\narchive: {enabled: true}\n",
		"CREATE STREAM page_views (k String) PRIMARY KEY (k);", infra.Options{})
	if err != nil {
		t.Fatal(err)
	}
	d := infra.Deployment{Prefix: "prod-", Partition: "aws", Account: "123456789012", Region: "eu-west-1"}
	tests := []struct {
		text    infra.Text
		want    string
		wantErr string
	}{
		{text: "${ref:Meta.arn}", want: "arn:aws:dynamodb:eu-west-1:123456789012:table/prod-analytics-meta"},
		{text: "${ref:StreamPageViews.name}", want: "prod-analytics-stream-page_views"},
		{
			text: "${ref:StreamPageViews.stream_arn_pattern}",
			want: "arn:aws:dynamodb:eu-west-1:123456789012:table/prod-analytics-stream-page_views/stream/*",
		},
		{
			text: "${ref:FirehosePageViews.arn}",
			want: "arn:aws:firehose:eu-west-1:123456789012:deliverystream/prod-analytics-archive-page_views",
		},
		{text: "${ref:ArchiveWorkGroup.name}", want: "prod-analytics"},
		{text: "${ref:ArchiveWorkGroup.arn}", want: "arn:aws:athena:eu-west-1:123456789012:workgroup/prod-analytics"},
		{text: "${ref:RoleSetup.arn}", want: "arn:aws:iam::123456789012:role/RoleSetup"},
		{text: "${prefix_}x:${partition}:${region}:${account}", want: "prod_x:aws:eu-west-1:123456789012"},
		{
			text:    "${ref:Bucket.arn}",
			wantErr: "infra: ${ref:Bucket.arn}: Resolve resolves references to tables, delivery streams, archive workgroups and roles only",
		},
		{
			text:    "${ref:FirehosePageViews.stream_arn_pattern}",
			wantErr: "infra: ${ref:FirehosePageViews.stream_arn_pattern}: a firehose resource doesn't provide stream_arn_pattern",
		},
	}
	for _, tt := range tests {
		got, rerr := m.Resolve(tt.text, d)
		var gotErr string
		if rerr != nil {
			gotErr = rerr.Error()
		}
		if got != tt.want || gotErr != tt.wantErr {
			t.Errorf("Resolve(%s) = %q, %q; want %q, %q", tt.text, got, gotErr, tt.want, tt.wantErr)
		}
	}
}
