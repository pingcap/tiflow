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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
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
	if s.capture == nil {
		// for test only
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}
	err := s.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.AsyncStop()
		return nil
	})
	handleOwnerResp(w, err)
}

func (s *Server) handleChangefeedAdmin(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}

	if s.capture == nil {
		// for test only
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

	err = s.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.EnqueueJob(job)
		return nil
	})

	handleOwnerResp(w, err)
}

func (s *Server) handleRebalanceTrigger(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}
	if s.capture == nil {
		// for test only
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
	err = s.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.TriggerRebalance(changefeedID)
		return nil
	})

	handleOwnerResp(w, err)
}

func (s *Server) handleMoveTable(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}

	if s.capture == nil {
		// for test only
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

	err = s.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.ManualSchedule(changefeedID, to, tableID)
		return nil
	})

	handleOwnerResp(w, err)
}

func (s *Server) handleChangefeedQuery(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusBadRequest, cerror.ErrSupportPostOnly.GenWithStackByArgs())
		return
	}
	if s.capture == nil {
		// for test only
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfInfo, err := s.etcdClient.GetChangeFeedInfo(ctx, changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID))
		return
	}
	cfStatus, _, err := s.etcdClient.GetChangeFeedStatus(ctx, changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	resp := &ChangefeedResp{}
	if cfInfo != nil {
		resp.FeedState = string(cfInfo.State)
		resp.RunningError = cfInfo.Error
	}
	if cfStatus != nil {
		resp.TSO = cfStatus.CheckpointTs
		tm := oracle.GetTimeFromTS(cfStatus.CheckpointTs)
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
