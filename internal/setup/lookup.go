// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"context"
	"errors"
	"fmt"
)

const (
	statusActive   = "ACTIVE"
	statusUpdating = "UPDATING"
	// The projector needs both images of a record (S2).
	streamView = "NEW_AND_OLD_IMAGES"
)

type lookupProps struct {
	TableName string `json:"TableName"`
}

// lookupStream answers [TypeStreamARN] with the table's stream ARN, as the physical ID and
// the attribute StreamArn. An UPDATING table is accepted: a replica added elsewhere, or a
// billing or TTL change, leaves its stream as it is.
func (h *Handler) lookupStream(ctx context.Context, props map[string]any) (Response, error) {
	var p lookupProps
	if err := decode(props, &p); err != nil {
		return Response{}, err
	}
	if p.TableName == "" {
		return Response{}, errors.New("property TableName is missing")
	}
	var t StreamTable
	var found bool
	err := retry(ctx, func() error {
		var err error
		t, found, err = h.DynamoDB.DescribeTable(ctx, p.TableName)
		return err
	})
	if err != nil {
		return Response{}, fmt.Errorf("describe stream table %s: %w", p.TableName, err)
	}

	const (
		redeploy  = "wait until the deploy of global.yaml has finished, then deploy again"
		generated = "deploy the global.yaml that Dynaflux generates"
	)
	switch {
	case !found:
		return Response{}, fmt.Errorf("stream table %s doesn't exist in this region: deploy global.yaml first, with the same Prefix", p.TableName)
	case t.Status != statusActive && t.Status != statusUpdating:
		return Response{}, fmt.Errorf("stream table %s is %s: %s", p.TableName, t.Status, redeploy)
	case t.ReplicaStatus != "" && t.ReplicaStatus != statusActive:
		return Response{}, fmt.Errorf("the replica of stream table %s in this region is %s: %s", p.TableName, t.ReplicaStatus, redeploy)
	case t.StreamARN == "":
		return Response{}, fmt.Errorf("stream table %s has no stream: %s", p.TableName, generated)
	case t.StreamView != streamView:
		return Response{}, fmt.Errorf("the stream of stream table %s is %s, not %s: %s", p.TableName, t.StreamView, streamView, generated)
	}
	return Response{PhysicalID: t.StreamARN, Data: map[string]string{"StreamArn": t.StreamARN}}, nil
}
