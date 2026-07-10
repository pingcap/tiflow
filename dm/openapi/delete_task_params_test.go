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
// See the License for the specific language governing permissions and
// limitations under the License.

package openapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type deleteTaskParamsRecorder struct {
	ServerInterface
	called   bool
	taskName string
	params   DMAPIDeleteTaskParams
}

func (r *deleteTaskParamsRecorder) DMAPIDeleteTask(c *gin.Context, taskName string, params DMAPIDeleteTaskParams) {
	r.called = true
	r.taskName = taskName
	r.params = params
	c.Status(http.StatusNoContent)
}

func TestDMAPIDeleteTaskParams(t *testing.T) {
	gin.SetMode(gin.TestMode)

	testCases := []struct {
		name             string
		query            string
		expectedStatus   int
		expectedForce    *bool
		expectedKeepMeta *bool
	}{
		{
			name:           "omitted",
			expectedStatus: http.StatusNoContent,
		},
		{
			name:             "false",
			query:            "?keep_meta=false",
			expectedStatus:   http.StatusNoContent,
			expectedKeepMeta: boolPtr(false),
		},
		{
			name:             "true with force",
			query:            "?force=true&keep_meta=true",
			expectedStatus:   http.StatusNoContent,
			expectedForce:    boolPtr(true),
			expectedKeepMeta: boolPtr(true),
		},
		{
			name:           "invalid",
			query:          "?keep_meta=invalid",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := &deleteTaskParamsRecorder{}
			router := gin.New()
			RegisterHandlers(router, recorder)

			request := httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/test-task"+testCase.query, nil)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			require.Equal(t, testCase.expectedStatus, response.Code)
			if testCase.expectedStatus == http.StatusBadRequest {
				require.False(t, recorder.called)
				return
			}
			require.True(t, recorder.called)
			require.Equal(t, "test-task", recorder.taskName)
			require.Equal(t, testCase.expectedForce, recorder.params.Force)
			require.Equal(t, testCase.expectedKeepMeta, recorder.params.KeepMeta)
		})
	}
}

func boolPtr(value bool) *bool {
	return &value
}
