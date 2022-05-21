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

package dm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
)

func TestMessageAgent(t *testing.T) {
	messageAgent := NewMessageAgent(&MockSender{})

	// Test QueryStatus
	dumpStatus := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  "task1",
			Stage: metadata.StageRunning,
		},
	}
	require.Nil(t, messageAgent.QueryStatusResponse(context.Background(), 1, dumpStatus, ""))
	require.Nil(t, messageAgent.QueryStatusResponse(context.Background(), 1, nil, "status error"))
}

type MockSender struct {
	id libModel.WorkerID
}

func (s *MockSender) ID() libModel.WorkerID {
	return s.id
}

func (s *MockSender) SendMessage(ctx context.Context, topic string, message interface{}, nonblocking bool) error {
	return nil
}
