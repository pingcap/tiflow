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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"go.etcd.io/etcd/clientv3/concurrency"
)

const (
	opVarAdminJob     = "admin-job"
	opVarChangefeedID = "cf-id"
)

type commonResp struct {
	Status  bool   `json:"status"`
	Message string `json:"message"`
}

func handleOwnerResp(w http.ResponseWriter, err error) {
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

func (s *Server) handleResignOwner(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, errors.New("this api only supports POST method"))
		return
	}
	err := s.capture.ownerManager.ResignOwner(req.Context())
	handleOwnerResp(w, err)
}

func (s *Server) handleChangefeedAdmin(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, errors.New("this api only supports POST method"))
		return
	}
	err := req.ParseForm()
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	typeStr := req.Form.Get(opVarAdminJob)
	typ, err := strconv.ParseInt(typeStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.Errorf("invalid admin job type: %s", typeStr))
		return
	}
	job := model.AdminJob{
		CfID: req.Form.Get(opVarChangefeedID),
		Type: model.AdminJobType(typ),
	}
	err = s.capture.ownerWorker.EnqueueJob(job)
	handleOwnerResp(w, err)
}
