// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package deployed

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/lucindo/dynaflux/internal/meta"
)

// Execution is what runs a backfill job.
type Execution struct {
	// ARN is the running execution: the one that ran the job's last step, or the
	// continuation that one started. It is "" when none runs: the job is interrupted.
	ARN string
	// Continuation reports whether ARN is the continuation.
	Continuation bool
}

// Running finds what runs the running job j in the backfill state machine whose ARN is
// machine (ADR 0011). A job whose recorded execution isn't the machine's, such as one that
// dynaflux dev ran or one of a deleted stack, has none.
func Running(ctx context.Context, c StepFunctions, machine string, j meta.Job) (Execution, error) {
	executions := strings.Replace(machine, ":stateMachine:", ":execution:", 1) + ":"
	name, ok := strings.CutPrefix(j.Execution, executions)
	if !ok {
		return Execution{}, nil
	}
	status, err := describe(ctx, c, j.Execution)
	switch {
	case err != nil:
		return Execution{}, err
	case status == types.ExecutionStatusRunning:
		return Execution{ARN: j.Execution}, nil
	case status != types.ExecutionStatusSucceeded:
		return Execution{}, nil
	}
	next := executions + Continuation(name)
	if status, err = describe(ctx, c, next); err != nil || status != types.ExecutionStatusRunning {
		return Execution{}, err
	}
	return Execution{ARN: next, Continuation: true}, nil
}

// describe returns the status of the execution whose ARN is arn, or "" when there is none.
func describe(ctx context.Context, c StepFunctions, arn string) (types.ExecutionStatus, error) {
	out, err := c.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: &arn})
	if _, missing := errors.AsType[*types.ExecutionDoesNotExist](err); missing {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("deployed: describe execution %s: %w", arn, err)
	}
	return out.Status, nil
}

// Continuation returns the name of the continuation that an execution called name starts:
// its name with -c and the next k.
func Continuation(name string) string {
	base, k := name, 0
	if i := strings.LastIndex(name, "-c"); i >= 0 {
		if n, err := strconv.Atoi(name[i+2:]); err == nil && strconv.Itoa(n) == name[i+2:] {
			base, k = name[:i], n
		}
	}
	return base + "-c" + strconv.Itoa(k+1)
}
