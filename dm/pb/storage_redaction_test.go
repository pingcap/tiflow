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

package pb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStorageRequestRedaction(t *testing.T) {
	rawURL := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk"
	cfg := "name: task-one\nloaders:\n  global:\n    dir: '" + rawURL + "'\n"
	for _, req := range []fmt.Stringer{
		&StartTaskRequest{Task: cfg},
		&CheckTaskRequest{Task: cfg},
		&UpdateTaskRequest{Task: cfg},
	} {
		before := req.String()
		core, logs := observer.New(zap.InfoLevel)
		zap.New(core).Info("request", zap.Any("payload", req))
		require.Len(t, logs.All(), 1)
		output := fmt.Sprint(logs.All()[0].ContextMap())
		require.NotContains(t, output, "private-")
		require.Contains(t, output, "task-one")
		require.Contains(t, output, "bucket/path")
		require.Equal(t, before, req.String())
	}
}

func TestStorageStatusRedaction(t *testing.T) {
	message := "403 s3://bucket/path?secret-access-key=private-sk request id: test-id"
	r := &ProcessResult{Errors: []*ProcessError{{ErrCode: 32001, Message: message, RawCause: message}}}
	status := &QueryStatusResponse{
		Msg:          message,
		SourceStatus: &SourceStatus{Result: r, RelayStatus: &RelayStatus{Result: r}},
		SubTaskStatus: []*SubTaskStatus{{
			Name: "task-one", Result: r,
			Validation: &ValidationStatus{Result: r},
			Status: &SubTaskStatus_Sync{Sync: &SyncStatus{
				TotalRows: 42, ConflictMsg: message, BlockingDDLs: []string{message},
				UnresolvedGroups: []*ShardingGroup{{DDLs: []string{message}}},
			}},
		}},
	}
	before := status.String()
	copyStatus := status.Redacted()
	require.NotContains(t, copyStatus.String(), "private-sk")
	require.Contains(t, copyStatus.String(), "request id: test-id")
	require.Equal(t, before, status.String())
	require.Equal(t, int32(32001), copyStatus.SubTaskStatus[0].Result.Errors[0].ErrCode)
	require.Equal(t, int64(42), copyStatus.SubTaskStatus[0].GetSync().TotalRows)
	require.Nil(t, (*QueryStatusResponse)(nil).Redacted())
}
