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

package openapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestTaskRedacted(t *testing.T) {
	dir := "s3://bucket/path?access-key=private-ak&secret-access-key=private-sk"
	task := &Task{Name: "task-one", SourceConfig: TaskSourceConfig{
		FullMigrateConf: &TaskFullMigrateConf{DataDir: &dir},
	}}
	before, err := json.Marshal(task)
	require.NoError(t, err)
	copyTask := task.Redacted()
	require.NotContains(t, *copyTask.SourceConfig.FullMigrateConf.DataDir, "private-")
	require.Contains(t, *copyTask.SourceConfig.FullMigrateConf.DataDir, "bucket/path")
	after, err := json.Marshal(task)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, "task-one", copyTask.Name)
	require.Nil(t, (*Task)(nil).Redacted())
}

func TestAccessLogURLCredentials(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	router := gin.New()
	router.Use(ZapLogger(zap.New(core)))
	router.GET("/test", func(c *gin.Context) {
		require.Equal(t, "private-ak", c.Query("access-key"))
		_ = c.Error(errors.New("storage denied https://host/path?sig=private-signature"))
		c.Status(http.StatusBadRequest)
	})
	req := httptest.NewRequest(http.MethodGet, "/test?access-key=private-ak&unknown-auth-token=private-token&source_name=source-one", nil)
	router.ServeHTTP(httptest.NewRecorder(), req)
	require.Len(t, logs.All(), 1)
	data, err := json.Marshal(logs.All()[0].ContextMap())
	require.NoError(t, err)
	require.NotContains(t, string(data), "private-")
	require.NotContains(t, string(data), "source-one")
	require.Equal(t, "GET /test", logs.All()[0].ContextMap()["request"])
	require.Contains(t, req.RequestURI, "private-ak")
}
