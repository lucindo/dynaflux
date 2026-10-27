// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package dynamo

import (
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/lucindo/dynaflux/internal/store"
)

// toItem converts an item that passed [store.CheckPut], or an M inside one. N text goes out
// as it is: DynamoDB parses it exactly.
func toItem(m map[string]store.Value) (map[string]types.AttributeValue, error) {
	out := make(map[string]types.AttributeValue, len(m))
	for name, v := range m {
		av, err := toAV(v)
		if err != nil {
			return nil, err
		}
		out[name] = av
	}
	return out, nil
}

// toAV always allocates B, L and M: the SDK sends a nil L or M as invalid JSON, which
// DynamoDB answers with a 500 that the SDK retries as transient.
func toAV(v store.Value) (types.AttributeValue, error) {
	switch v.Type {
	case store.S:
		return &types.AttributeValueMemberS{Value: v.Str}, nil
	case store.N:
		return &types.AttributeValueMemberN{Value: v.Str}, nil
	case store.B:
		return &types.AttributeValueMemberB{Value: append([]byte{}, v.Bin...)}, nil
	case store.BOOL:
		return &types.AttributeValueMemberBOOL{Value: v.Bool}, nil
	case store.L:
		list := make([]types.AttributeValue, len(v.List))
		for i, e := range v.List {
			av, err := toAV(e)
			if err != nil {
				return nil, err
			}
			list[i] = av
		}
		return &types.AttributeValueMemberL{Value: list}, nil
	case store.M:
		m, err := toItem(v.Map)
		if err != nil {
			return nil, err
		}
		return &types.AttributeValueMemberM{Value: m}, nil
	}
	return nil, fmt.Errorf("%w: type %d is not one Dynaflux writes", store.ErrInvalidRequest, v.Type)
}

// fromItem converts an item DynamoDB returned; nil or empty is an absent item.
func fromItem(av map[string]types.AttributeValue) store.Item {
	if len(av) == 0 {
		return nil
	}
	return fromMap(av)
}

func fromMap(av map[string]types.AttributeValue) map[string]store.Value {
	out := make(map[string]store.Value, len(av))
	for name, a := range av {
		out[name] = fromAV(a)
	}
	return out
}

// fromAV returns the zero Value for NULL, a set, or a type newer than the SDK: only a
// foreign writer leaves them, and the layers above decide what that means.
func fromAV(a types.AttributeValue) store.Value {
	switch a := a.(type) {
	case *types.AttributeValueMemberS:
		return store.Value{Type: store.S, Str: a.Value}
	case *types.AttributeValueMemberN:
		return store.Value{Type: store.N, Str: a.Value}
	case *types.AttributeValueMemberB:
		return store.Value{Type: store.B, Bin: slices.Clone(a.Value)}
	case *types.AttributeValueMemberBOOL:
		return store.Value{Type: store.BOOL, Bool: a.Value}
	case *types.AttributeValueMemberL:
		list := make([]store.Value, len(a.Value))
		for i, e := range a.Value {
			list[i] = fromAV(e)
		}
		return store.Value{Type: store.L, List: list}
	case *types.AttributeValueMemberM:
		return store.Value{Type: store.M, Map: fromMap(a.Value)}
	}
	return store.Value{}
}
