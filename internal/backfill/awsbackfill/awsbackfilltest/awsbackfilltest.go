// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

// Package awsbackfilltest serves, over HTTP, the AWS calls of package awsbackfill that the
// emulator lacks, for tests: S3 objects in memory, and CloudWatch without data.
package awsbackfilltest

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// S3 is buckets of objects in memory behind S3's REST API, path-style, with the operations
// awsbackfill sends: PutObject, GetObject with a Range, ListObjectsV2 by prefix, and
// DeleteObjects. It ignores signatures and checksums, and takes plain bodies only, as the
// SDK sends them to an http endpoint. It is safe for concurrent use.
type S3 struct {
	mu      sync.Mutex
	objects map[string][]byte // bucket/key → data
	// deny, when set, refuses a GetObject of a key it returns true for with AccessDenied, as
	// S3 answers a missing key to a caller without s3:ListBucket.
	deny func(key string) bool
}

// NewS3 returns an S3 with no object.
func NewS3() *S3 { return &S3{objects: map[string][]byte{}} }

// DenyGet refuses GetObject of the keys deny returns true for with AccessDenied.
func (s *S3) DenyGet(deny func(key string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deny = deny
}

// Keys returns the keys of bucket under prefix, sorted.
func (s *S3) Keys(bucket, prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys(bucket, prefix)
}

func (s *S3) keys(bucket, prefix string) []string {
	var out []string
	for k := range s.objects {
		if key, ok := strings.CutPrefix(k, bucket+"/"); ok && strings.HasPrefix(key, prefix) {
			out = append(out, key)
		}
	}
	slices.Sort(out)
	return out
}

// ServeHTTP implements [http.Handler].
func (s *S3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	q := r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.Method == http.MethodPut && key != "":
		s.objects[bucket+"/"+key] = body
	case r.Method == http.MethodGet && key != "":
		s.get(w, r, bucket, key)
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		s.list(w, bucket, q.Get("prefix"), q.Get("continuation-token"), q.Get("max-keys"))
	case r.Method == http.MethodPost && q.Has("delete"):
		s.delete(w, bucket, body)
	default:
		s3Error(w, http.StatusNotImplemented, "NotImplemented", r.Method+" "+r.URL.String())
	}
}

func (s *S3) get(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if s.deny != nil && s.deny(key) {
		s3Error(w, http.StatusForbidden, "AccessDenied", "Access Denied")
		return
	}
	data, ok := s.objects[bucket+"/"+key]
	if !ok {
		s3Error(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	size := int64(len(data))
	from, to := int64(0), size-1
	if rng, ranged := strings.CutPrefix(r.Header.Get("Range"), "bytes="); ranged {
		first, last, _ := strings.Cut(rng, "-")
		f, ferr := strconv.ParseInt(first, 10, 64)
		l, lerr := strconv.ParseInt(last, 10, 64)
		switch {
		case first == "" && lerr == nil:
			from = max(0, size-l)
		case ferr == nil && lerr == nil:
			from, to = f, min(to, l)
		default:
			s3Error(w, http.StatusBadRequest, "InvalidArgument", "Range "+rng)
			return
		}
		if from >= size {
			s3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable")
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, size))
		w.Header().Set("Content-Length", strconv.FormatInt(to-from+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	}
	w.Write(data[from : to+1]) //nolint:errcheck,gosec // the client sees a short body
}

type listResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	Contents              []listed `xml:"Contents"`
	KeyCount              int      `xml:"KeyCount"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
}

type listed struct {
	Key  string `xml:"Key"`
	Size int    `xml:"Size"`
}

// list answers ListObjectsV2: maxKeys keys at most, 1,000 by default, after token, which is
// the last key of the page before.
func (s *S3) list(w http.ResponseWriter, bucket, prefix, token, maxKeys string) {
	limit, err := strconv.Atoi(maxKeys)
	if err != nil || limit <= 0 || limit > 1000 {
		limit = 1000
	}
	keys := s.keys(bucket, prefix)
	keys = keys[min(len(keys), sortSearch(keys, token)):]
	res := listResult{IsTruncated: len(keys) > limit}
	keys = keys[:min(len(keys), limit)]
	for _, k := range keys {
		res.Contents = append(res.Contents, listed{Key: k, Size: len(s.objects[bucket+"/"+k])})
	}
	res.KeyCount = len(keys)
	if res.IsTruncated {
		res.NextContinuationToken = keys[len(keys)-1]
	}
	writeXML(w, res)
}

// sortSearch returns the index of the first key after token, or 0 without a token.
func sortSearch(keys []string, token string) int {
	if token == "" {
		return 0
	}
	i, found := slices.BinarySearch(keys, token)
	if found {
		i++
	}
	return i
}

type deleteRequest struct {
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type deleteResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
}

func (s *S3) delete(w http.ResponseWriter, bucket string, body []byte) {
	var req deleteRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		s3Error(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	for _, o := range req.Objects {
		delete(s.objects, bucket+"/"+o.Key)
	}
	writeXML(w, deleteResult{})
}

func writeXML(w http.ResponseWriter, v any) {
	data, err := xml.Marshal(v)
	if err != nil {
		s3Error(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Write(append([]byte(xml.Header), data...)) //nolint:errcheck,gosec // the client sees a short body
}

type s3ErrorBody struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func s3Error(w http.ResponseWriter, status int, code, message string) {
	data, err := xml.Marshal(s3ErrorBody{Code: code, Message: message})
	if err != nil {
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write(append([]byte(xml.Header), data...)) //nolint:errcheck,gosec // the client sees a short body
}

// CloudWatch answers every request as CloudWatch answers GetMetricData without data, in the
// RPC v2 CBOR protocol: an empty map.
func CloudWatch(w http.ResponseWriter, r *http.Request) {
	if _, err := io.Copy(io.Discard, r.Body); err != nil {
		return
	}
	w.Header().Set("Smithy-Protocol", "rpc-v2-cbor")
	w.Header().Set("Content-Type", "application/cbor")
	w.Write([]byte{0xa0}) //nolint:errcheck,gosec // the client sees a short body
}
