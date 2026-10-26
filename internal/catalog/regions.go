// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package catalog

import (
	"slices"
	"strings"
)

// BucketNamespaceRegion reports whether region offers the S3 account regional namespace, in
// which the store's bucket is named, as of
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/gpbucketnamespaces.html.
func BucketNamespaceRegion(region string) bool {
	return region != "me-south-1" && region != "me-central-1"
}

// FirehoseIcebergRegion reports whether Firehose delivers to Apache Iceberg Tables in
// region: every region but the China and AWS GovCloud (US) regions, Asia Pacific (Taipei),
// Asia Pacific (Malaysia), Asia Pacific (New Zealand) and Mexico (Central), as of
// https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-considerations.html.
func FirehoseIcebergRegion(region string) bool {
	return !strings.HasPrefix(region, "cn-") && !strings.HasPrefix(region, "us-gov-") &&
		!slices.Contains([]string{"ap-east-2", "ap-southeast-5", "ap-southeast-6", "mx-central-1"}, region)
}

// GlueOptimizerRegion reports whether AWS Glue offers the table optimizers that compact the
// archive's files and expire its snapshots in region, as of
// https://docs.aws.amazon.com/glue/latest/dg/regions-optimizers.html.
func GlueOptimizerRegion(region string) bool {
	return slices.Contains([]string{
		"ap-northeast-1", "ap-northeast-2", "ap-south-1", "ap-southeast-1", "ap-southeast-2", "ap-southeast-3",
		"ca-central-1", "eu-central-1", "eu-north-1", "eu-west-1", "eu-west-2", "sa-east-1",
		"us-east-1", "us-east-2", "us-west-2",
	}, region)
}
