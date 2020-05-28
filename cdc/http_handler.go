// Copyright 2020 PingCAP, Inc.
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
	"context"
	"net/http"
	"strconv"

	"github.com/pingcap/ticdc/pkg/util"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"go.etcd.io/etcd/clientv3/concurrency"
)

const (
	// APIOpVarAdminJob is the key of admin job in HTTP API
	APIOpVarAdminJob = "admin-job"
	// APIOpVarChangefeedID is the key of changefeed ID in HTTP API
	APIOpVarChangefeedID = "cf-id"
	// APIOpVarTargetCaptureID is the key of to-capture ID in HTTP API
	APIOpVarTargetCaptureID = "target-cp-id"
	// APIOpVarTableID is the key of table ID in HTTP API
	APIOpVarTableID = "table-id"
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
	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNoLeader)
		return
	}
	// Resign is a complex process that needs to be synchronized because
	// it happens in two separate goroutines
	//
	// Imagine that we have goroutines A and B
	// A1. Notify the owner to exit
	// B1. The owner exits gracefully
	// A2. Delete the leader key until the owner has exited
	// B2. Restart to campaign
	//
	// A2 must occur between B1 and B2, so we register the Resign process
	// as the stepDown function which is called when the owner exited.
	s.owner.Close(req.Context(), func(ctx context.Context) error {
		return s.capture.Resign(ctx)
	})
	s.owner = nil
	handleOwnerResp(w, nil)
}

func (s *Server) handleChangefeedAdmin(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, errors.New("this api only supports POST method"))
		return
	}

	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNoLeader)
	}

	err := req.ParseForm()
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	typeStr := req.Form.Get(APIOpVarAdminJob)
	typ, err := strconv.ParseInt(typeStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.Errorf("invalid admin job type: %s", typeStr))
		return
	}
	job := model.AdminJob{
		CfID: req.Form.Get(APIOpVarChangefeedID),
		Type: model.AdminJobType(typ),
	}
	err = s.owner.EnqueueJob(job)
	handleOwnerResp(w, err)
}

func (s *Server) handleRebanlanceTrigger(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, errors.New("this api only supports POST method"))
		return
	}

	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNoLeader)
	}

	err := req.ParseForm()
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	changefeedID := req.Form.Get(APIOpVarChangefeedID)
	if !util.IsValidUUIDv4(changefeedID) {
		writeError(w, http.StatusBadRequest, errors.Errorf("invalid changefeed id: %s", changefeedID))
		return
	}
	s.owner.TriggerRebanlance(changefeedID)
	handleOwnerResp(w, nil)
}

func (s *Server) handleMoveTable(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, errors.New("this api only supports POST method"))
		return
	}

	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNoLeader)
	}

	err := req.ParseForm()
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	changefeedID := req.Form.Get(APIOpVarChangefeedID)
	if !util.IsValidUUIDv4(changefeedID) {
		writeError(w, http.StatusBadRequest, errors.Errorf("invalid changefeed id: %s", changefeedID))
		return
	}
	to := req.Form.Get(APIOpVarTargetCaptureID)
	if !util.IsValidUUIDv4(to) {
		writeError(w, http.StatusBadRequest, errors.Errorf("invalid target capture id: %s", to))
		return
	}
	tableIDStr := req.Form.Get(APIOpVarTableID)
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.Errorf("invalid tableID: %s", tableIDStr))
		return
	}
	s.owner.ManualSchedule(changefeedID, to, tableID)
	handleOwnerResp(w, nil)
}
