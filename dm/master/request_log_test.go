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

package master

import (
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/dm/pb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestMasterRequestLogFieldsHideCredentials(t *testing.T) {
	core, observed := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	requests := []struct {
		method string
		req    interface{}
	}{
		{"StartTask", &pb.StartTaskRequest{Task: "password=start-secret"}},
		{"UpdateTask", &pb.UpdateTaskRequest{Task: "password=update-secret"}},
		{"CheckTask", &pb.CheckTaskRequest{Task: "password=check-secret"}},
		{"OperateSource", &pb.OperateSourceRequest{Config: []string{"password=source-secret"}}},
		{"Encrypt", &pb.EncryptRequest{Plaintext: "encrypt-secret"}},
	}
	for _, tc := range requests {
		logger.Info("master request", masterRequestLogFields(tc.method, tc.req)...)
		entries := observed.TakeAll()
		require.Len(t, entries, 1)
		fields := fmt.Sprint(entries[0].ContextMap())
		require.Contains(t, fields, tc.method)
		require.NotContains(t, fields, "secret")
	}

	logger.Info("master request", masterRequestLogFields("QueryStatus", &pb.QueryStatusListRequest{Name: "debug-task"})...)
	entries := observed.TakeAll()
	require.Len(t, entries, 1)
	require.Contains(t, fmt.Sprint(entries[0].ContextMap()), "debug-task")
}
