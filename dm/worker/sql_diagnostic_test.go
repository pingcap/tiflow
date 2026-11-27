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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestQueryStatusSQLDiagnosticBoundary(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		server := &Server{cfg: &Config{Name: "worker-one"}, sourceStatus: pb.SourceStatus{
			Source: "source-one", Result: &pb.ProcessResult{Errors: []*pb.ProcessError{{
				ErrCode:  int32(terror.ErrDBExecuteFailed.Code()),
				Message:  "execute statement failed: ALTER TABLE t ADD v VARCHAR(100) DEFAULT 'private-value'",
				RawCause: "Error 1064 (42000): syntax error near private-value",
			}}},
		}}
		original := server.sourceStatus.String()
		status, err := server.QueryStatus(context.Background(), &pb.QueryStatusRequest{})
		require.NoError(t, err)
		require.Equal(t, original, server.sourceStatus.String())
		if enabled {
			require.NotContains(t, status.String(), "private-value")
			require.Contains(t, status.String(), "VARCHAR(100)")
			require.Contains(t, status.SourceStatus.Result.Errors[0].RawCause, "1064")
		} else {
			require.Contains(t, status.String(), "private-value")
		}
	}
}

func TestSQLDiagnosticOperationErrorResponse(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	err := terror.ErrParseSQL.Delegate(errors.New("line 1 column 19 near private-value"),
		"ALTER TABLE t ADD v VARCHAR(100) DEFAULT 'private-value'")
	log.SetRedactLog(true)
	response := makeCommonWorkerResponse(err)
	require.False(t, response.Result)
	require.NotContains(t, response.Msg, "private-value")
	require.Contains(t, response.Msg, "VARCHAR(100)")
	require.Contains(t, response.Msg, "line 1 column 19")
	require.Contains(t, err.Error(), "private-value")
	log.SetRedactLog(false)
	require.Equal(t, err.Error(), makeCommonWorkerResponse(err).Msg)
}
