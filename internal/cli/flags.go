// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"errors"
	"flag"
	"strconv"
	"strings"
	"time"
)

// defaultReadTimeout bounds a run of status or diff.
const defaultReadTimeout = 2 * time.Minute

// awsFlags are the flags of a command that reads one region of a deployed store.
type awsFlags struct {
	prefix   optionalString
	region   string
	endpoint string
}

func addAWSFlags(fs *flag.FlagSet) *awsFlags {
	f := &awsFlags{}
	fs.Var(&f.prefix, "prefix", "the `prefix` of the environment's resource names (default prefix in dynaflux.yaml)")
	fs.StringVar(&f.region, "region", "", "the `region` to read, one of regions in dynaflux.yaml (default the AWS SDK's region)")
	fs.StringVar(&f.endpoint, "endpoint", "",
		"read DynamoDB Local at this loopback `URL`, such as http://127.0.0.1:8000, with fake credentials, in the first of regions unless -region names another")
	return f
}

// storeFlags are the flags of a command that reads several regions of a deployed store.
type storeFlags struct {
	prefix   optionalString
	regions  repeated
	endpoint string
	json     bool
	timeout  time.Duration
}

func addStoreFlags(fs *flag.FlagSet, f *storeFlags) {
	fs.Var(&f.prefix, "prefix", "the `prefix` of the environment's resource names (default prefix in dynaflux.yaml)")
	fs.Var(&f.regions, "region", "a `region` to read, one of regions in dynaflux.yaml; repeat it for each one (default all of them)")
	fs.StringVar(&f.endpoint, "endpoint", "",
		"read DynamoDB Local at this loopback `URL`, such as http://127.0.0.1:8000, with fake credentials, in the first of regions unless -region names others")
	fs.BoolVar(&f.json, "json", false, "print one JSON document")
	fs.DurationVar(&f.timeout, "timeout", defaultReadTimeout, "stop reading after this `duration`")
}

// check refuses what the flags can't mean for command name.
func (f storeFlags) check(c *cli, name string) int {
	if f.timeout <= 0 {
		return c.usageError(name, "-timeout "+f.timeout.String()+": want a duration above 0, such as 5m")
	}
	return exitOK
}

// optionalString is a string flag that tells "" from unset.
type optionalString struct {
	value string
	set   bool
}

func (o *optionalString) String() string { return o.value }

func (o *optionalString) Set(s string) error {
	o.value, o.set = s, true
	return nil
}

// repeated is a flag that may be given more than once.
type repeated []string

func (r *repeated) String() string { return strings.Join(*r, ",") }

func (r *repeated) Set(s string) error {
	*r = append(*r, s)
	return nil
}

// optionalInt is a non-negative int flag that tells 0 from unset.
type optionalInt struct {
	value int
	set   bool
}

func (o *optionalInt) String() string { return strconv.Itoa(o.value) }

func (o *optionalInt) Set(s string) error {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return errors.New("want a number of 0 or more")
	}
	o.value, o.set = n, true
	return nil
}
