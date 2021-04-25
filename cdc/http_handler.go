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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.uber.org/zap"
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
	// APIOpForceRemoveChangefeed is used when remove a changefeed
	APIOpForceRemoveChangefeed = "force-remove"
)

type commonResp struct {
	Status  bool   `json:"status"`
	Message string `json:"message"`
}

// ChangefeedResp holds the most common usage information for a changefeed
type ChangefeedResp struct {
	FeedState    string              `json:"state"`
	TSO          uint64              `json:"tso"`
	Checkpoint   string              `json:"checkpoint"`
	RunningError *model.RunningError `json:"error"`
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
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}
	s.ownerLock.RLock()
	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		s.ownerLock.RUnlock()
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
	s.ownerLock.RUnlock()
	s.setOwner(nil)
	handleOwnerResp(w, nil)
}

func (s *Server) handleChangefeedAdmin(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}

	s.ownerLock.RLock()
	defer s.ownerLock.RUnlock()
	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}

	err := req.ParseForm()
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	typeStr := req.Form.Get(APIOpVarAdminJob)
	typ, err := strconv.ParseInt(typeStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, cerror.ErrAPIInvalidParam.GenWithStack("invalid admin job type: %s", typeStr))
		return
	}
	opts := &model.AdminJobOption{}
	if forceRemoveStr := req.Form.Get(APIOpForceRemoveChangefeed); forceRemoveStr != "" {
		forceRemoveOpt, err := strconv.ParseBool(forceRemoveStr)
		if err != nil {
			writeError(w, http.StatusBadRequest,
				cerror.ErrAPIInvalidParam.GenWithStack("invalid force remove option: %s", forceRemoveStr))
			return
		}
		opts.ForceRemove = forceRemoveOpt
	}
	job := model.AdminJob{
		CfID: req.Form.Get(APIOpVarChangefeedID),
		Type: model.AdminJobType(typ),
		Opts: opts,
	}
	err = s.owner.EnqueueJob(job)
	handleOwnerResp(w, err)
}

func (s *Server) handleRebalanceTrigger(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}

	s.ownerLock.RLock()
	defer s.ownerLock.RUnlock()
	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}

	err := req.ParseForm()
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	changefeedID := req.Form.Get(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID))
		return
	}
	s.owner.TriggerRebalance(changefeedID)
	handleOwnerResp(w, nil)
}

func (s *Server) handleMoveTable(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}

	s.ownerLock.RLock()
	defer s.ownerLock.RUnlock()
	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}

	err := req.ParseForm()
	if err != nil {
		writeInternalServerError(w, cerror.WrapError(cerror.ErrInternalServerError, err))
		return
	}
	changefeedID := req.Form.Get(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID))
		return
	}
	to := req.Form.Get(APIOpVarTargetCaptureID)
	if err := model.ValidateChangefeedID(to); err != nil {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid target capture id: %s", to))
		return
	}
	tableIDStr := req.Form.Get(APIOpVarTableID)
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid tableID: %s", tableIDStr))
		return
	}
	s.owner.ManualSchedule(changefeedID, to, tableID)
	handleOwnerResp(w, nil)
}

func (s *Server) handleChangefeedQuery(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}
	s.ownerLock.RLock()
	defer s.ownerLock.RUnlock()
	if s.owner == nil {
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}

	err := req.ParseForm()
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	changefeedID := req.Form.Get(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID))
		return
	}
	cf, status, feedState, err := s.owner.collectChangefeedInfo(req.Context(), changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		writeInternalServerError(w, err)
		return
	}
	feedInfo, err := s.owner.etcdClient.GetChangeFeedInfo(req.Context(), changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		writeInternalServerError(w, err)
		return
	}

	resp := &ChangefeedResp{
		FeedState: string(feedState),
	}
	if cf != nil {
		resp.RunningError = cf.info.Error
	} else if feedInfo != nil {
		resp.RunningError = feedInfo.Error
	}
	if status != nil {
		resp.TSO = status.CheckpointTs
		tm := oracle.GetTimeFromTS(status.CheckpointTs)
		resp.Checkpoint = tm.Format("2006-01-02 15:04:05.000")
	}
	writeData(w, resp)
}

func handleAdminLogLevel(w http.ResponseWriter, r *http.Request) {
	var level string
	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		writeInternalServerError(w, err)
		return
	}
	err = json.Unmarshal(data, &level)
	if err != nil {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid log level: %s", err))
		return
	}

	err = logutil.SetLogLevel(level)
	if err != nil {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("fail to change log level: %s", err))
		return
	}
	log.Warn("log level changed", zap.String("level", level))

	writeData(w, struct{}{})
}
