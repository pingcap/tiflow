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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/openapi/fixtures"
	"github.com/pingcap/tiflow/dm/pbmock"
	"github.com/stretchr/testify/require"
)

func TestTaskCredentialCopies(t *testing.T) {
	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	require.NoError(t, err)
	original, err := task.ToJSON()
	require.NoError(t, err)

	redacted := redactTaskForResponse(task)
	require.Equal(t, config.ObfuscatedPasswordForFeedback, redacted.TargetConfig.Password)
	for _, security := range []*openapi.Security{redacted.TargetConfig.Security, redacted.SourceConfig.FullMigrateConf.Security} {
		require.NotNil(t, security)
		require.Empty(t, security.SslCaContent)
		require.Empty(t, security.SslCertContent)
		require.Empty(t, security.SslKeyContent)
	}
	require.Equal(t, task.TargetConfig.Security.CertAllowedCn, redacted.TargetConfig.Security.CertAllowedCn)
	require.Equal(t, task.SourceConfig.FullMigrateConf.Security.CertAllowedCn, redacted.SourceConfig.FullMigrateConf.Security.CertAllowedCn)
	require.Equal(t, redacted, redactTaskForResponse(redacted))

	// Restoring a response for update must not turn that response into a secret
	// container or mutate the stored task through shared pointers.
	redactedJSON, err := redacted.ToJSON()
	require.NoError(t, err)
	restored := restoreTaskCredentials(redacted, task)
	require.Equal(t, task, restored)
	afterRestore, err := redacted.ToJSON()
	require.NoError(t, err)
	require.Equal(t, redactedJSON, afterRestore)
	afterRedact, err := task.ToJSON()
	require.NoError(t, err)
	require.Equal(t, original, afterRedact)

	// Explicit replacement credentials (including an empty password) are not
	// placeholders. Missing security objects retain the existing disable-TLS
	// semantics rather than silently restoring TLS.
	redacted.TargetConfig.Password = "new-password"
	redacted.TargetConfig.Security.SslCertContent = "new-cert"
	redacted.TargetConfig.Security.SslKeyContent = "new-key"
	redacted.SourceConfig.FullMigrateConf.Security = nil
	restored = restoreTaskCredentials(redacted, task)
	require.Equal(t, "new-password", restored.TargetConfig.Password)
	require.Equal(t, task.TargetConfig.Security.SslCaContent, restored.TargetConfig.Security.SslCaContent)
	require.Equal(t, "new-cert", restored.TargetConfig.Security.SslCertContent)
	require.Equal(t, "new-key", restored.TargetConfig.Security.SslKeyContent)
	require.Nil(t, restored.SourceConfig.FullMigrateConf.Security)

	redacted.TargetConfig.Password = ""
	redacted.TargetConfig.Security = nil
	redacted.SourceConfig.FullMigrateConf = nil
	restored = restoreTaskCredentials(redacted, task)
	require.Empty(t, restored.TargetConfig.Password)
	require.Nil(t, restored.TargetConfig.Security)
	require.Nil(t, restored.SourceConfig.FullMigrateConf)
	require.Nil(t, redactTaskForResponse(restored).TargetConfig.Security)
	require.Nil(t, redactTaskForResponse(restored).SourceConfig.FullMigrateConf)
	require.Equal(t, task, restoreTaskCredentials(task, openapi.Task{}))
}

func TestValidateTaskCredentialReuse(t *testing.T) {
	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	require.NoError(t, err)
	for _, change := range []func(*openapi.Task){
		func(task *openapi.Task) { task.TargetConfig.Host = "other-host" },
		func(task *openapi.Task) { task.TargetConfig.Port++ },
		func(task *openapi.Task) { task.TargetConfig.User = "other-user" },
		func(task *openapi.Task) { task.SourceConfig.FullMigrateConf.PdAddr = nil },
		func(task *openapi.Task) {
			pdAddr := "other-pd:2379"
			task.SourceConfig.FullMigrateConf.PdAddr = &pdAddr
		},
	} {
		redacted := redactTaskForResponse(task)
		require.NoError(t, validateTaskCredentialReuse(redacted, task))
		change(&redacted)
		require.Error(t, validateTaskCredentialReuse(redacted, task))
		explicit := restoreTaskCredentials(redacted, task)
		require.NoError(t, validateTaskCredentialReuse(explicit, task))
	}
}

func (s *OpenAPIViewSuite) TestTaskCredentialRedaction() {
	t := s.T()
	ctx, cancel := context.WithCancel(context.Background())
	server := setupTestServer(ctx, t)
	defer func() {
		cancel()
		server.Close()
	}()

	workerName := "credential-test-worker"
	require.NoError(t, server.scheduler.AddWorker(workerName, "127.0.0.1:8262"))
	server.scheduler.GetWorkerByName(workerName).ToFree()
	source := openapi.Source{SourceName: source1Name, Enable: true, Host: "127.0.0.1", Port: 3306, User: "root"}
	_, err := server.createSource(ctx, openapi.CreateSourceRequest{Source: source, WorkerName: &workerName})
	require.NoError(t, err)
	task, err := fixtures.GenNoShardOpenAPITaskForTest()
	require.NoError(t, err)
	task.TargetConfig.Password = "task-redaction-test-password"

	// Both create and GET -> PUT must pass the original secrets to validation,
	// while all HTTP responses and the request objects remain redacted.
	checkCount := 0
	checker.CheckSyncConfigFunc = func(_ context.Context, cfgs []*config.SubTaskConfig, _, _ int64) (string, error) {
		checkCount++
		require.Len(t, cfgs, 1)
		require.Equal(t, task.TargetConfig.Password, cfgs[0].To.Password)
		require.Equal(t, task.TargetConfig.Security.SslKeyContent, string(cfgs[0].To.Security.SSLKeyBytes))
		require.Equal(t, task.SourceConfig.FullMigrateConf.Security.SslKeyContent, string(cfgs[0].LoaderConfig.Security.SSLKeyBytes))
		return "", nil
	}
	ctrl := gomock.NewController(t)
	workerClient := pbmock.NewMockWorkerClient(ctrl)
	mockTaskQueryStatus(workerClient, task.Name, source1Name, workerName, false)
	server.scheduler.SetWorkerClientForTest(workerName, newMockRPCClient(workerClient))

	request := func(method, path string, payload interface{}, status int) []byte {
		t.Helper()
		body, err := json.Marshal(payload)
		require.NoError(t, err)
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		server.openapiHandles.ServeHTTP(recorder, req)
		require.Equal(t, status, recorder.Code, recorder.Body.String())
		for _, secret := range []string{
			task.TargetConfig.Password,
			task.TargetConfig.Security.SslCaContent, task.TargetConfig.Security.SslCertContent, task.TargetConfig.Security.SslKeyContent,
			task.SourceConfig.FullMigrateConf.Security.SslCaContent,
			task.SourceConfig.FullMigrateConf.Security.SslCertContent,
			task.SourceConfig.FullMigrateConf.Security.SslKeyContent,
		} {
			require.NotContains(t, recorder.Body.String(), secret)
		}
		return recorder.Body.Bytes()
	}
	assertRedacted := func(response openapi.Task) {
		t.Helper()
		require.Equal(t, config.ObfuscatedPasswordForFeedback, response.TargetConfig.Password)
		require.Equal(t, redactTaskSecurity(task.TargetConfig.Security), response.TargetConfig.Security)
		require.Equal(t, redactTaskSecurity(task.SourceConfig.FullMigrateConf.Security), response.SourceConfig.FullMigrateConf.Security)
	}

	var operated openapi.OperateTaskResponse
	body := request(http.MethodPost, "/api/v1/tasks", openapi.CreateTaskRequest{Task: task}, http.StatusCreated)
	require.NoError(t, json.Unmarshal(body, &operated))
	assertRedacted(operated.Task)

	var readTask openapi.Task
	for _, query := range []string{"", "?with_status=true"} {
		body = request(http.MethodGet, "/api/v1/tasks/"+task.Name+query, nil, http.StatusOK)
		require.NoError(t, json.Unmarshal(body, &readTask))
		assertRedacted(readTask)
		body = request(http.MethodGet, "/api/v1/tasks"+query, nil, http.StatusOK)
		var list openapi.GetTaskListResponse
		require.NoError(t, json.Unmarshal(body, &list))
		require.Equal(t, 1, list.Total)
		require.Len(t, list.Data, 1)
		assertRedacted(list.Data[0])
	}

	// Reject reuse of hidden credentials on another endpoint before prechecks
	// can connect to it. A GET also must not have changed the stored credentials.
	redirected := readTask
	redirected.TargetConfig.Host = "other-host"
	request(http.MethodPut, "/api/v1/tasks/"+task.Name, openapi.UpdateTaskRequest{Task: redirected}, http.StatusBadRequest)
	require.Equal(t, 1, checkCount)
	beforeUpdate := server.scheduler.GetSubTaskCfgsByTask(task.Name)[source1Name]
	require.Equal(t, task.TargetConfig.Password, beforeUpdate.To.Password)
	require.Equal(t, task.TargetConfig.Security.SslKeyContent, string(beforeUpdate.To.Security.SSLKeyBytes))
	require.Equal(t, task.SourceConfig.FullMigrateConf.Security.SslKeyContent, string(beforeUpdate.LoaderConfig.Security.SSLKeyBytes))

	// The worker RPC is mocked, but validation, conversion, scheduler persistence
	// and the production HTTP router are exercised by the round trip.
	const updateFailpoint = "github.com/pingcap/tiflow/dm/master/scheduler/operateCheckSubtasksCanUpdate"
	require.NoError(t, failpoint.Enable(updateFailpoint, `return("success")`))
	defer func() { require.NoError(t, failpoint.Disable(updateFailpoint)) }()
	batch := 1000
	readTask.SourceConfig.IncrMigrateConf.ReplBatch = &batch
	body = request(http.MethodPut, "/api/v1/tasks/"+task.Name, openapi.UpdateTaskRequest{Task: readTask}, http.StatusOK)
	require.NoError(t, json.Unmarshal(body, &operated))
	assertRedacted(operated.Task)
	require.Equal(t, 2, checkCount)
	assertRedacted(readTask)

	stored := server.scheduler.GetSubTaskCfgsByTask(task.Name)[source1Name]
	require.Equal(t, batch, stored.SyncerConfig.Batch)
	require.Equal(t, task.TargetConfig.Password, stored.To.Password)
	require.Equal(t, task.TargetConfig.Security.SslKeyContent, string(stored.To.Security.SSLKeyBytes))
	require.Equal(t, task.SourceConfig.FullMigrateConf.Security.SslKeyContent, string(stored.LoaderConfig.Security.SSLKeyBytes))
	internal, err := server.getTask(ctx, task.Name, openapi.DMAPIGetTaskParams{})
	require.NoError(t, err)
	require.Equal(t, task.TargetConfig.Password, internal.TargetConfig.Password)
	require.Equal(t, task.TargetConfig.Security, internal.TargetConfig.Security)
	require.Equal(t, task.SourceConfig.FullMigrateConf.Security, internal.SourceConfig.FullMigrateConf.Security)
}
