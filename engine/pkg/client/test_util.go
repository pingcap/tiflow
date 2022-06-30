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

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/engine/enginepb"
)

type preDispatchArgsMatcher struct {
	args *DispatchTaskArgs
}

func matchPreDispatchArgs(args *DispatchTaskArgs) gomock.Matcher {
	return &preDispatchArgsMatcher{args: args}
}

func (m *preDispatchArgsMatcher) Matches(x interface{}) bool {
	// TODO match ProjectInfo
	req, ok := x.(*enginepb.PreDispatchTaskRequest)
	if !ok {
		return false
	}

	if !(m.args.WorkerID == req.GetWorkerId() &&
		m.args.MasterID == req.GetMasterId() &&
		m.args.WorkerType == req.GetTaskTypeId()) {

		return false
	}

	if !bytes.Equal(m.args.WorkerConfig, req.GetTaskConfig()) {
		return false
	}

	return true
}

func (m *preDispatchArgsMatcher) String() string {
	jsonBytes, err := json.Marshal(m.args)
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf("PreDispatchRequest Matches Args %s", string(jsonBytes))
}

type confirmDispatchMatcher struct {
	// requestID is a pointer to a string,
	// because the pointed string will be changed
	// after the matcher has been created.
	requestID *string

	workerID string
}

func matchConfirmDispatch(requestID *string, workerID string) gomock.Matcher {
	return &confirmDispatchMatcher{
		requestID: requestID,
		workerID:  workerID,
	}
}

func (m *confirmDispatchMatcher) Matches(x interface{}) bool {
	req, ok := x.(*enginepb.ConfirmDispatchTaskRequest)
	if !ok {
		return false
	}

	return reflect.DeepEqual(req, &enginepb.ConfirmDispatchTaskRequest{
		WorkerId:  m.workerID,
		RequestId: *m.requestID,
	})
}

func (m *confirmDispatchMatcher) String() string {
	expected := &enginepb.ConfirmDispatchTaskRequest{
		WorkerId:  m.workerID,
		RequestId: *m.requestID,
	}

	return fmt.Sprintf("ConfirmDispatchRequest Matches Args %s", expected.String())
}
