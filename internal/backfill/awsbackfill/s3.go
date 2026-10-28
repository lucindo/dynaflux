// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package awsbackfill

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/lucindo/dynaflux/internal/backfill"
)

// Runs keeps a job's staged objects in a bucket: [backfill.Runs].
type Runs struct {
	c      S3
	bucket string
}

// NewRuns returns the staged objects in bucket, which c reaches.
func NewRuns(c S3, bucket string) *Runs {
	return &Runs{c: c, bucket: bucket}
}

// Put implements [backfill.Runs]. It holds the object in memory, a run's size at most, so
// that the SDK can send it again on a retry.
func (r *Runs) Put(ctx context.Context, name string, write func(io.Writer) error) error {
	var b bytes.Buffer
	if err := write(&b); err != nil {
		return err
	}
	_, err := r.c.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.bucket, Key: &name, Body: bytes.NewReader(b.Bytes()), ContentLength: aws.Int64(int64(b.Len())),
	})
	if err != nil {
		return fmt.Errorf("awsbackfill: put %s: %w", name, err)
	}
	return nil
}

// ReadAt implements [backfill.Runs] with a ranged GetObject.
func (r *Runs) ReadAt(ctx context.Context, name string, off, n int64) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	rng := fmt.Sprintf("bytes=%d-%d", off, off+n-1)
	if off < 0 {
		rng = fmt.Sprintf("bytes=-%d", n)
	}
	body, err := get(ctx, r.c, r.bucket, name, &rng)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return readAll(name, body, n)
}

// get reads key from bucket, its range rng when that is set. A missing key fails with
// [backfill.ErrStagingMissing].
func get(ctx context.Context, c S3, bucket, key string, rng *string) (io.ReadCloser, error) {
	out, err := c.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key, Range: rng})
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok && apiErr.ErrorCode() == "AccessDenied" {
		// S3 answers a missing key so to a caller that can list the bucket only with a
		// prefix condition, which a GetObject request doesn't carry.
		if gone, lerr := missing(ctx, c, bucket, key); lerr == nil && gone {
			err = &types.NoSuchKey{}
		}
	}
	if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
		return nil, fmt.Errorf("%s: %w", key, backfill.ErrStagingMissing)
	}
	if err != nil {
		return nil, fmt.Errorf("awsbackfill: read %s: %w", key, err)
	}
	return out.Body, nil
}

// readAll reads at most n bytes of the object name from body.
func readAll(name string, body io.Reader, n int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, n))
	if err != nil {
		return nil, fmt.Errorf("awsbackfill: read %s: %w", name, err)
	}
	return data, nil
}

// missing reports whether a listing of bucket lacks key.
func missing(ctx context.Context, c S3, bucket, key string) (bool, error) {
	out, err := c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &key, MaxKeys: aws.Int32(1)})
	if err != nil {
		return false, err
	}
	return !slices.ContainsFunc(out.Contents, func(o types.Object) bool { return aws.ToString(o.Key) == key }), nil
}

// DeletePrefix implements [backfill.Runs]: it deletes each page of a listing of prefix.
func (r *Runs) DeletePrefix(ctx context.Context, prefix string) error {
	pages := s3.NewListObjectsV2Paginator(r.c, &s3.ListObjectsV2Input{Bucket: &r.bucket, Prefix: &prefix})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("awsbackfill: list %s: %w", prefix, err)
		}
		if len(page.Contents) == 0 {
			continue
		}
		objects := make([]types.ObjectIdentifier, len(page.Contents))
		for i, o := range page.Contents {
			objects[i] = types.ObjectIdentifier{Key: o.Key}
		}
		out, err := r.c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &r.bucket, Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("awsbackfill: delete under %s: %w", prefix, err)
		}
		if len(out.Errors) > 0 {
			e := out.Errors[0]
			return fmt.Errorf("awsbackfill: delete %s: %s: %s", aws.ToString(e.Key), aws.ToString(e.Code), aws.ToString(e.Message))
		}
	}
	return nil
}
