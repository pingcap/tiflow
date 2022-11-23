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
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/openapi"
	"github.com/pingcap/tiflow/pkg/errors"
)

func jobAPIBasePath(jobID engineModel.JobID) string {
	return openapi.JobAPIPrefix + jobID + "/"
}

type jobAPIServer struct {
	rwm      sync.RWMutex
	handlers map[engineModel.JobID]http.Handler
}

func newJobAPIServer() *jobAPIServer {
	return &jobAPIServer{
		handlers: make(map[engineModel.JobID]http.Handler),
	}
}

func (s *jobAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h, err := s.matchHandler(r.URL.Path)
	if err != nil {
		openapi.WriteHTTPError(w, err)
		return
	}
	h.ServeHTTP(w, r)
}

func (s *jobAPIServer) matchHandler(path string) (http.Handler, error) {
	if !strings.HasPrefix(path, openapi.JobAPIPrefix) {
		return nil, errors.ErrInvalidArgument.GenWithStack("invalid job api path: %s", path)
	}
	path = strings.TrimPrefix(path, openapi.JobAPIPrefix)
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		return nil, errors.ErrInvalidArgument.GenWithStack("invalid job api path: %s", path)
	}
	JobID := parts[0]
	s.rwm.RLock()
	h, ok := s.handlers[JobID]
	s.rwm.RUnlock()
	if !ok {
		// We can't tell whether the job exists or not, but at least the job is not running.
		return nil, errors.ErrJobNotRunning.GenWithStackByArgs(JobID)
	}
	return h, nil
}

func (s *jobAPIServer) initialize(jobID engineModel.JobID, f func(apiGroup *gin.RouterGroup)) {
	engine := gin.New()
	apiGroup := engine.Group(jobAPIBasePath(jobID))
	f(apiGroup)

	s.rwm.Lock()
	s.handlers[jobID] = engine
	s.rwm.Unlock()
}

func (s *jobAPIServer) listenStoppedJobs(ctx context.Context, stoppedJobs <-chan engineModel.JobID) error {
	for {
		select {
		case jobID, ok := <-stoppedJobs:
			if !ok {
				return nil
			}
			s.rwm.Lock()
			delete(s.handlers, jobID)
			s.rwm.Unlock()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
