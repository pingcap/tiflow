// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"net/http"

	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pingcap/errors"
)

type commonResp struct {
	Status  bool   `json:"status"`
	Message string `json:"message"`
}

func (s *Server) handleResignOwner(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, errors.New("this api only supports POST method"))
		return
	}
	err := s.capture.ownerManager.ResignOwner(req.Context())
	if err != nil {
		if errors.Cause(err) == concurrency.ErrElectionNotLeader {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeInternalServerError(w, err)
		return
	}
	writeData(w, commonResp{Status: true})
}
