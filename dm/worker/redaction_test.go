// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package worker

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func TestQueryStatusRedactsSourceErrorsWithoutWorker(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	log.SetRedactLog(true)
	t.Cleanup(func() { log.SetRedactLog(previous) })
	s := &Server{cfg: &Config{Name: "worker-one"}, sourceStatus: pb.SourceStatus{
		Source: "source-one", Result: &pb.ProcessResult{Errors: []*pb.ProcessError{{RawCause: "private-value"}}},
	}}
	resp, err := s.QueryStatus(context.Background(), &pb.QueryStatusRequest{})
	require.NoError(t, err)
	require.NotContains(t, resp.String(), "private-value")
	require.Contains(t, s.sourceStatus.String(), "private-value")
}

func TestRuntimeStatusRedaction(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	st := &SubTask{
		cfg: &config.SubTaskConfig{Name: "task-one"}, stage: pb.Stage_Paused,
		result: &pb.ProcessResult{Errors: []*pb.ProcessError{{ErrCode: 1062, RawCause: "private-value"}}},
	}
	w := &SourceWorker{subTaskHolder: newSubTaskHolder(), l: log.L()}
	w.subTaskHolder.recordSubTask(st)
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		output := w.GetUnitAndSourceStatusJSON("", nil)
		require.Contains(t, output, "task-one")
		require.Contains(t, output, "Paused")
		require.Contains(t, output, "1062")
		if enabled {
			require.NotContains(t, output, "private-value")
		} else {
			require.Contains(t, output, "private-value")
		}
		require.Equal(t, "private-value", st.Result().Errors[0].RawCause)
	}
}
