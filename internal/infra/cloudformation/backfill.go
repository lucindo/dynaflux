// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package cloudformation

import (
	"strconv"

	"go.yaml.in/yaml/v4"

	"github.com/lucindo/dynaflux/internal/infra"
)

// managedPolicy returns a managed policy attached to role. It names the role, rather than the
// role the policy, so the role exists before its policies.
func managedPolicy(r *resolver, role infra.Role, p infra.ManagedPolicy) *yaml.Node {
	statements := make([]*yaml.Node, 0, len(p.Statements))
	for _, st := range p.Statements {
		statements = append(statements, statement(r, st))
	}
	return resource("AWS::IAM::ManagedPolicy", mapping(
		kv("Roles", seq(intrinsic("Ref", str(role.ID)))),
		kv("PolicyDocument", policyDocument(statements...)),
	), archiveCondition(role.Common)...)
}

// Retries of the state machine's tasks.
const (
	backoff                      = 2
	quickInterval, quickAttempts = 2, 6  // Lambda's service errors, and continuing
	slowInterval, slowAttempts   = 30, 5 // any other error, such as a step's
)

// stateMachine returns the backfill state machine (ADR 0011). Each step invokes the function
// with the execution's job and attempt; its reply says whether the job needs no more steps,
// and how long to wait before the next. After MaxSteps steps, the execution starts its
// continuation, -c and the next k after its name, and ends. Lambda's service errors are
// retried 6 times from 2 s, and any other error 5 times from 30 s, before the execution
// fails.
func stateMachine(r *resolver, x infra.StateMachine) *yaml.Node {
	steps := strconv.Itoa(x.MaxSteps)
	definition := mapping(
		kv("Comment", str("Dynaflux backfill: runs a job's steps until it needs no more, waiting as each step asks, "+
			"and continues in a new execution after "+steps+" steps.")),
		kv("QueryLanguage", str("JSONata")),
		kv("StartAt", str("Step")),
		kv("States", mapping(
			kv("Step", stepState()),
			kv("Route", routeState(steps)),
			kv("Wait", mapping(kv("Type", str("Wait")), kv("Seconds", input("wait")), kv("Next", str("Step")))),
			kv("Continue", continueState()),
			kv("Done", mapping(kv("Type", str("Succeed")))),
		)),
	)
	return resource("AWS::StepFunctions::StateMachine", mapping(
		kv("StateMachineName", r.value(x.Name)),
		kv("StateMachineType", str("STANDARD")),
		kv("RoleArn", r.value(x.Role)),
		kv("LoggingConfiguration", mapping(
			kv("Level", str("ERROR")),
			kv("IncludeExecutionData", boolean(false)),
			kv("Destinations", seq(mapping(kv("CloudWatchLogsLogGroup", mapping(kv("LogGroupArn", r.value(x.LogGroup))))))),
		)),
		kv("DefinitionSubstitutions", mapping(
			kv("Function", r.value(x.Function)),
			kv("LambdaInvoke", intrinsic("Sub", str("arn:${AWS::Partition}:states:::lambda:invoke"))),
			kv("StartExecution", intrinsic("Sub", str("arn:${AWS::Partition}:states:::states:startExecution"))),
		)),
		kv("Definition", definition),
		kv("Tags", tags(x.Tags)),
	))
}

// input is a field of the state's input, in JSONata.
func input(field string) *yaml.Node { return str("{% $states.input." + field + " %}") }

// retry retries errs attempts times, from interval seconds.
func retry(errs []string, interval, attempts int) *yaml.Node {
	names := seq()
	for _, e := range errs {
		names.Content = append(names.Content, str(e))
	}
	return mapping(kv("ErrorEquals", names), kv("IntervalSeconds", num(interval)), kv("MaxAttempts", num(attempts)),
		kv("BackoffRate", num(backoff)), kv("JitterStrategy", str("FULL")))
}

// stepState invokes the function for a step of the job, and counts the step.
func stepState() *yaml.Node {
	lambdaErrors := []string{"Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"}
	return mapping(
		kv("Type", str("Task")),
		kv("Resource", str("${LambdaInvoke}")),
		kv("Arguments", mapping(
			kv("FunctionName", str("${Function}")),
			kv("Payload", mapping(
				kv("job", input("job")),
				kv("attempt", input("attempt")),
				kv("execution", str("{% $states.context.Execution.Id %}")),
			)),
		)),
		kv("Output", str("{% $merge([$states.input, {'steps': $states.input.steps + 1, "+
			"'done': $states.result.Payload.done, 'wait': $states.result.Payload.wait_seconds}]) %}")),
		kv("Retry", seq(retry(lambdaErrors, quickInterval, quickAttempts), retry([]string{"States.ALL"}, slowInterval, slowAttempts))),
		kv("Next", str("Route")),
	)
}

// routeState ends a job that needs no more steps, continues after steps of them, and waits as
// the last step asked.
func routeState(steps string) *yaml.Node {
	return mapping(
		kv("Type", str("Choice")),
		kv("Choices", seq(
			mapping(kv("Condition", str("{% $states.input.done %}")), kv("Next", str("Done"))),
			mapping(kv("Condition", str("{% $states.input.steps >= "+steps+" %}")), kv("Next", str("Continue"))),
			mapping(kv("Condition", str("{% $states.input.wait > 0 %}")), kv("Next", str("Wait"))),
		)),
		kv("Default", str("Step")),
	)
}

// continueState starts the execution's continuation, and ends.
func continueState() *yaml.Node {
	return mapping(
		kv("Type", str("Task")),
		kv("Resource", str("${StartExecution}")),
		kv("Arguments", mapping(
			kv("StateMachineArn", str("{% $states.context.StateMachine.Id %}")),
			kv("Name", str("{% $states.input.name & '-a' & $states.input.attempt & '-c' & ($states.input.k + 1) %}")),
			kv("Input", mapping(
				kv("job", input("job")),
				kv("attempt", input("attempt")),
				kv("name", input("name")),
				kv("k", input("k + 1")),
				kv("steps", num(0)),
			)),
		)),
		kv("Retry", seq(retry([]string{"States.ALL"}, quickInterval, quickAttempts))),
		kv("End", boolean(true)),
	)
}
