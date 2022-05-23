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
	"net/http"
	"strings"
	"sync"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
)

const jobAPIPrefix = "/api/v1/jobs/"

func jobAPIBasePath(masterID libModel.MasterID) string {
	return jobAPIPrefix + masterID
}

type jobAPIServer struct {
	rwm      sync.RWMutex
	handlers map[libModel.MasterID]http.Handler
}

func newJobAPIServer() *jobAPIServer {
	return &jobAPIServer{
		handlers: make(map[libModel.MasterID]http.Handler),
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
	masterID := fields[0]
	s.rwm.RLock()
	h, ok := s.handlers[masterID]
	s.rwm.RUnlock()
	return h, ok
}

func (s *jobAPIServer) addHandler(masterID libModel.MasterID, handler http.Handler) {
	s.rwm.Lock()
	s.handlers[masterID] = handler
	s.rwm.Unlock()
}

func (s *jobAPIServer) removeHandler(masterID libModel.MasterID) {
	s.rwm.Lock()
	delete(s.handlers, masterID)
	s.rwm.Unlock()
}
