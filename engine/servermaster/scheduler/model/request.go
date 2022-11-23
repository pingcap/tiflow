// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
)

// SchedulerRequest represents a request for an executor to run a given task.
type SchedulerRequest struct {
	TenantID          string // reserved for future use.
	ExternalResources []resModel.ResourceKey
	Selectors         []*label.Selector
}

// NewSchedulerRequestFromPB converts a protobuf message ScheduleTaskRequest
// to the internal data structure SchedulerRequest.
func NewSchedulerRequestFromPB(req *enginepb.ScheduleTaskRequest) (*SchedulerRequest, error) {
	selectors := make([]*label.Selector, 0, len(req.GetSelectors()))
	for _, sel := range req.GetSelectors() {
		internalSel, err := SelectorFromPB(sel)
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, internalSel)
	}

	schedulerReq := &SchedulerRequest{
		ExternalResources: resModel.ToResourceKeys(req.GetResources()),
		Selectors:         selectors,
	}
	return schedulerReq, nil
}

// SelectorFromPB converts a protobuf Selector message to the internal
// data type label.Selector.
// This function does the necessary sanity checks, so it's safe to use
// with an RPC argument.
func SelectorFromPB(req *enginepb.Selector) (*label.Selector, error) {
	var op label.Op
	switch req.Op {
	case enginepb.Selector_Eq:
		op = label.OpEq
	case enginepb.Selector_Neq:
		op = label.OpNeq
	case enginepb.Selector_Regex:
		op = label.OpRegex
	default:
		// Future-proof the code in case a new Op is added.
		return nil, errors.ErrIncompatibleSchedulerRequest.GenWithStackByArgs("unknown selector op")
	}

	ret := &label.Selector{
		Key:    label.Key(req.Label),
		Target: req.Target,
		Op:     op,
	}

	if err := ret.Validate(); err != nil {
		return nil, errors.ErrIncompatibleSchedulerRequest.Wrap(err).GenWithStackByArgs("invalid selector")
	}
	return ret, nil
}

// SelectorToPB converts a label.Selector to a protobuf message Selector.
func SelectorToPB(sel *label.Selector) (*enginepb.Selector, error) {
	if err := sel.Validate(); err != nil {
		return nil, errors.Annotate(err, "SelectorToPB")
	}

	var pbOp enginepb.Selector_Op
	switch sel.Op {
	case label.OpEq:
		pbOp = enginepb.Selector_Eq
	case label.OpNeq:
		pbOp = enginepb.Selector_Neq
	case label.OpRegex:
		pbOp = enginepb.Selector_Regex
	default:
		return nil, errors.Errorf("unknown selector op %s", sel.Op)
	}

	return &enginepb.Selector{
		Label:  string(sel.Key),
		Target: sel.Target,
		Op:     pbOp,
	}, nil
}

// SchedulerResponse represents a response to a task scheduling request.
type SchedulerResponse struct {
	ExecutorID model.ExecutorID
}
