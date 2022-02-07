// Copyright 2021 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// apiOpVarChangefeeds is the key of list option in HTTP API
	apiOpVarChangefeeds = "state"
)

// JSONTime used to wrap time into json format
type JSONTime time.Time

// MarshalJSON use to specify the time format
func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02 15:04:05.000"))
	return []byte(stamp), nil
}

// err of cdc http api
type httpError struct {
	Error string `json:"error"`
}

// ChangefeedCommonInfo holds some common usage information of a changefeed and use by RESTful API only.
type ChangefeedCommonInfo struct {
	ID             string              `json:"id"`
	FeedState      model.FeedState     `json:"state"`
	CheckpointTSO  uint64              `json:"checkpoint-tso"`
	CheckpointTime JSONTime            `json:"checkpoint-time"`
	RunningError   *model.RunningError `json:"error"`
}

// handleHealth check if is this server is health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// handleChangefeeds dispatch the request to the specified handleFunc according to the request method.
func (s *Server) handleChangefeeds(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet {
		s.handleChangefeedsList(w, req)
		return
	}
	// Only the get method is allowed at this stage,
	// if it is not the get method, an error will be returned directly
	writeErrorJSON(w, http.StatusBadRequest, *cerror.ErrSupportGetOnly)
}

// handleChangefeedsList will only received request with Get method from dispatcher.
func (s *Server) handleChangefeedsList(w http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		writeInternalServerErrorJSON(w, cerror.WrapError(cerror.ErrInternalServerError, err))
		return
	}
	state := req.Form.Get(apiOpVarChangefeeds)

	statuses, err := s.etcdClient.GetAllChangeFeedStatus(req.Context())
	if err != nil {
		writeInternalServerErrorJSON(w, err)
		return
	}

	changefeedIDs := make(map[string]struct{}, len(statuses))
	for cid := range statuses {
		changefeedIDs[cid] = struct{}{}
	}

	resps := make([]*ChangefeedCommonInfo, 0)
	for changefeedID := range changefeedIDs {
		cfInfo, err := s.etcdClient.GetChangeFeedInfo(req.Context(), changefeedID)
		if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
			writeInternalServerErrorJSON(w, err)
			return
		}
		if !isFiltered(state, cfInfo.State) {
			continue
		}
		cfStatus, _, err := s.etcdClient.GetChangeFeedStatus(req.Context(), changefeedID)
		if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
			writeInternalServerErrorJSON(w, err)
			return
		}
		resp := &ChangefeedCommonInfo{
			ID: changefeedID,
		}

		if cfInfo != nil {
			resp.FeedState = cfInfo.State
			resp.RunningError = cfInfo.Error
		}

		if cfStatus != nil {
			resp.CheckpointTSO = cfStatus.CheckpointTs
			tm := oracle.GetTimeFromTS(cfStatus.CheckpointTs)
			resp.CheckpointTime = JSONTime(tm)
		}
		resps = append(resps, resp)
	}
	writeData(w, resps)
}

func writeInternalServerErrorJSON(w http.ResponseWriter, err error) {
	writeErrorJSON(w, http.StatusInternalServerError, *cerror.ErrInternalServerError.Wrap(err))
}

func writeErrorJSON(w http.ResponseWriter, statusCode int, cerr errors.Error) {
	httpErr := httpError{Error: cerr.Error()}
	jsonStr, err := json.MarshalIndent(httpErr, "", " ")
	if err != nil {
		log.Error("invalid json data", zap.Reflect("data", err), zap.Error(err))
		return
	}
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(jsonStr)
	if err != nil {
		log.Error("fail to write data", zap.Error(err))
	}
}

// isFiltered return true if the given feedState matches the whiteList.
func isFiltered(whiteList string, feedState model.FeedState) bool {
	if whiteList == "all" {
		return true
	}
	if whiteList == "" {
		switch feedState {
		case model.StateNormal:
			return true
		case model.StateStopped:
			return true
		case model.StateFailed:
			return true
		}
	}
	return whiteList == string(feedState)
}
