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
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRedactStatusPreservesInternalErrors(t *testing.T) {
	result := &ProcessResult{IsCanceled: true, Detail: []byte("private-value"), Errors: []*ProcessError{{
		ErrCode: 1062, ErrClass: "database", Message: "private-value", RawCause: "private-value", Workaround: "private-value",
	}}}
	status := &QueryStatusResponse{
		Result:       true,
		SourceStatus: &SourceStatus{Source: "source-one", Result: result, RelayStatus: &RelayStatus{Result: result}},
		SubTaskStatus: []*SubTaskStatus{{
			Name: "task-one", Stage: Stage_Paused, Result: result,
			Validation: &ValidationStatus{Result: result},
			Status: &SubTaskStatus_Sync{Sync: &SyncStatus{
				TotalRows:    42,
				BlockingDDLs: []string{"private-value"}, ConflictMsg: "private-value",
				UnresolvedGroups: []*ShardingGroup{{DDLs: []string{"private-value"}}},
			}},
		}},
	}
	before := status.String()
	redacted := RedactQueryStatus(status)
	require.NotContains(t, redacted.String(), "private-value")
	require.Equal(t, before, status.String())
	require.Equal(t, int32(1062), redacted.SubTaskStatus[0].Result.Errors[0].ErrCode)
	require.True(t, redacted.SubTaskStatus[0].Result.IsCanceled)
	require.Equal(t, int64(42), redacted.SubTaskStatus[0].GetSync().TotalRows)
	require.Equal(t, "task-one", redacted.SubTaskStatus[0].Name)
	require.Equal(t, Stage_Paused, redacted.SubTaskStatus[0].Stage)
	require.Nil(t, RedactProcessResult(nil))
	require.Nil(t, RedactSubTaskStatus(nil))
}

func TestSQLRequestRedaction(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		for _, req := range []any{
			&HandleErrorRequest{Task: "task-one", Sqls: []string{"private-value"}},
			&HandleWorkerErrorRequest{Task: "task-one", Sqls: []string{"private-value"}},
			&OperateSchemaRequest{Task: "task-one", Schema: "private-value"},
			&OperateWorkerSchemaRequest{Task: "task-one", Schema: "private-value"},
		} {
			output := requestLog(t, zap.Any("request", req))
			require.Contains(t, output, "task-one")
			if enabled {
				require.NotContains(t, output, "private-value")
			} else {
				require.Contains(t, output, "private-value")
			}
		}
	}
}
