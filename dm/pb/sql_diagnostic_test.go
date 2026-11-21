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

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSQLDiagnosticStatusCopies(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	sql := "ALTER TABLE t ADD v VARCHAR(100) DEFAULT 'private-value'"
	process := &ProcessResult{Errors: []*ProcessError{{
		ErrCode:  int32(terror.ErrDBExecuteFailed.Code()),
		Message:  "execute statement failed: " + sql,
		RawCause: "Error 1064 (42000): syntax error near 'private-value'",
	}}}
	original := &QueryStatusResponse{
		SourceStatus: &SourceStatus{Result: process},
		SubTaskStatus: []*SubTaskStatus{{
			Name: "task-one", Result: process,
			Status: &SubTaskStatus_Sync{Sync: &SyncStatus{
				TotalRows: 42, BlockingDDLs: []string{sql}, ConflictMsg: "columns have different types",
				UnresolvedGroups: []*ShardingGroup{{DDLs: []string{sql}}},
			}},
		}},
	}
	before := original.String()
	log.SetRedactLog(true)
	redacted := original.SQLDiagnosticCopy()
	require.Equal(t, before, original.String(), "diagnostics must not change runtime errors or DDLs")
	require.NotContains(t, redacted.String(), "private-value")
	require.Contains(t, redacted.String(), "VARCHAR(100)")
	require.Contains(t, redacted.SourceStatus.Result.Errors[0].RawCause, "1064")
	require.Contains(t, redacted.SourceStatus.Result.Errors[0].RawCause, "SQL syntax error")
	require.Equal(t, int64(42), redacted.SubTaskStatus[0].GetSync().TotalRows)
	require.Equal(t, "columns have different types", redacted.SubTaskStatus[0].GetSync().ConflictMsg)
	require.Equal(t, original.SubTaskStatus[0].Result.Errors[0].ErrCode, redacted.SubTaskStatus[0].Result.Errors[0].ErrCode)
	log.SetRedactLog(false)
	require.Same(t, original, original.SQLDiagnosticCopy())
	require.Nil(t, (*QueryStatusResponse)(nil).SQLDiagnosticCopy())
}

func TestSQLOperationRequestLogCopies(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	sql := "ALTER TABLE t ADD v VARCHAR(100) DEFAULT 'private-value'"
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		for _, request := range []fmt.Stringer{
			&HandleWorkerErrorRequest{Task: "task-one", Sqls: []string{sql}},
			&HandleErrorRequest{Task: "task-one", Sqls: []string{sql}},
			&OperateWorkerSchemaRequest{Task: "task-one", Schema: sql},
			&OperateSchemaRequest{Task: "task-one", Schema: sql},
		} {
			before := request.String()
			core, logs := observer.New(zap.InfoLevel)
			zap.New(core).Info("request", zap.Any("payload", request))
			require.Len(t, logs.All(), 1)
			output := fmt.Sprint(logs.All()[0].ContextMap())
			require.Equal(t, before, request.String(), "request SQL used for execution must remain exact")
			require.Contains(t, output, "task-one")
			require.Contains(t, output, "VARCHAR(100)")
			if enabled {
				require.NotContains(t, output, "private-value")
				require.Contains(t, output, "DEFAULT ?")
			} else {
				require.Contains(t, output, "private-value")
			}
		}
	}
}
