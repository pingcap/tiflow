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

package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	engineModel "github.com/pingcap/tiflow/engine/model"
)

func TestJobAPIServer(t *testing.T) {
	jobAPISrv := newJobAPIServer()

	jobAPISrv.initialize("job1", func(apiGroup *gin.RouterGroup) {
		apiGroup.GET("/status", func(c *gin.Context) {
			c.String(http.StatusOK, "job1 status")
		})
	})
	jobAPISrv.initialize("job2", func(apiGroup *gin.RouterGroup) {
		apiGroup.GET("/status", func(c *gin.Context) {
			c.String(http.StatusOK, "job2 status")
		})
	})

	// test job1
	{
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/api/v1/jobs/job1/status", nil)
		jobAPISrv.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "job1 status", w.Body.String())
	}
	// test job2
	{
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/api/v1/jobs/job2/status", nil)
		jobAPISrv.ServeHTTP(w, r)
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, "job2 status", w.Body.String())
	}
	// test not found
	{
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/api/v1/jobs/job3/status", nil)
		jobAPISrv.ServeHTTP(w, r)
		require.Equal(t, http.StatusNotFound, w.Code)
	}

	wg := sync.WaitGroup{}
	stoppedJobs := make(chan engineModel.JobID, 16)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := jobAPISrv.listenStoppedJobs(context.Background(), stoppedJobs)
		require.NoError(t, err)
	}()

	stoppedJobs <- "job1"
	require.Eventually(t, func() bool {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/api/v1/jobs/job1/status", nil)
		jobAPISrv.ServeHTTP(w, r)
		return w.Code == http.StatusNotFound
	}, time.Second, time.Millisecond*100)

	stoppedJobs <- "job2"
	require.Eventually(t, func() bool {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/api/v1/jobs/job2/status", nil)
		jobAPISrv.ServeHTTP(w, r)
		return w.Code == http.StatusNotFound
	}, time.Second, time.Millisecond*100)

	close(stoppedJobs)
	wg.Wait()
}
