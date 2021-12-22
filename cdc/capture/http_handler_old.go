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

package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/version"
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/tikv/client-go/v2/oracle"
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

// MarshalJSON use to marshal ChangefeedResp
func (c ChangefeedResp) MarshalJSON() ([]byte, error) {
	// alias the original type to prevent recursive call of MarshalJSON
	type Alias ChangefeedResp
	if c.FeedState == string(model.StateNormal) {
		c.RunningError = nil
	}
	return json.Marshal(struct {
		Alias
	}{
		Alias: Alias(c),
	})
}

func handleOwnerResp(w http.ResponseWriter, err error) {
	if err != nil {
		if errors.Cause(err) == concurrency.ErrElectionNotLeader || cerror.ErrNotOwner.Equal(err) {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeInternalServerError(w, err)
		return
	}
	writeData(w, commonResp{Status: true})
}

func (h *HTTPHandler) handleResignOwner(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
		// for test only
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}
	err := h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.AsyncStop()
		return nil
	})
	handleOwnerResp(w, err)
}

func (h *HTTPHandler) handleChangefeedAdmin(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
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

	err = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.EnqueueJob(job)
		return nil
	})

	handleOwnerResp(w, err)
}

func (h *HTTPHandler) handleRebalanceTrigger(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
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

	err = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.TriggerRebalance(changefeedID)
		return nil
	})

	handleOwnerResp(w, err)
}

func (h *HTTPHandler) handleMoveTable(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
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

	err = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.ManualSchedule(changefeedID, to, tableID)
		return nil
	})

	handleOwnerResp(w, err)
}

func (h *HTTPHandler) handleChangefeedQuery(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
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
	cfInfo, err := h.capture.etcdClient.GetChangeFeedInfo(ctx, changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		writeError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID))
		return
	}
	cfStatus, _, err := h.capture.etcdClient.GetChangeFeedStatus(ctx, changefeedID)
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
	data, err := io.ReadAll(r.Body)
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

// status of cdc server
type status struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	ID      string `json:"id"`
	Pid     int    `json:"pid"`
	IsOwner bool   `json:"is_owner"`
}

func (h *HTTPHandler) writeEtcdInfo(ctx context.Context, cli *etcd.CDCEtcdClient, w io.Writer) {
	resp, err := cli.Client.Get(ctx, etcd.EtcdKeyBase, clientv3.WithPrefix())
	if err != nil {
		fmt.Fprintf(w, "failed to get info: %s\n\n", err.Error())
		return
	}

	for _, kv := range resp.Kvs {
		fmt.Fprintf(w, "%s\n\t%s\n\n", string(kv.Key), string(kv.Value))
	}
}

func (h *HTTPHandler) handleDebugInfo(w http.ResponseWriter, req *http.Request) {
	h.capture.WriteDebugInfo(w)
	fmt.Fprintf(w, "\n\n*** etcd info ***:\n\n")
	h.writeEtcdInfo(req.Context(), h.capture.etcdClient, w)
}

func (h *HTTPHandler) handleStatus(w http.ResponseWriter, req *http.Request) {
	st := status{
		Version: version.ReleaseVersion,
		GitHash: version.GitHash,
		Pid:     os.Getpid(),
	}

	if h.capture != nil {
		st.ID = h.capture.Info().ID
		st.IsOwner = h.capture.IsOwner()
	}
	writeData(w, st)
}

func writeInternalServerError(w http.ResponseWriter, err error) {
	writeError(w, http.StatusInternalServerError, err)
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.WriteHeader(statusCode)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		log.Error("write error", zap.Error(err))
	}
}

func writeData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		log.Error("invalid json data", zap.Reflect("data", data), zap.Error(err))
		writeInternalServerError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		log.Error("fail to write data", zap.Error(err))
	}
}
