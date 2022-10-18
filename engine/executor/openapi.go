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
)

const jobAPIPrefix = "/api/v1/jobs/"

func jobAPIBasePath(jobID engineModel.JobID) string {
	return jobAPIPrefix + jobID + "/"
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
	if h, ok := s.match(r.URL.Path); ok {
		h.ServeHTTP(w, r)
	} else {
		http.NotFound(w, r)
	}
}

func (s *jobAPIServer) match(path string) (http.Handler, bool) {
	if !strings.HasPrefix(path, jobAPIPrefix) {
		return nil, false
	}
	path = strings.TrimPrefix(path, jobAPIPrefix)
	fields := strings.SplitN(path, "/", 2)
	if len(fields) != 2 {
		return nil, false
	}
	JobID := fields[0]
	s.rwm.RLock()
	h, ok := s.handlers[JobID]
	s.rwm.RUnlock()
	return h, ok
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
