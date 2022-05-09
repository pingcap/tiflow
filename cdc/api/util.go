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

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// httpBadRequestError is some errors that will cause a BadRequestError in http handler
var httpBadRequestError = []*errors.Error{
	cerror.ErrAPIInvalidParam, cerror.ErrSinkURIInvalid, cerror.ErrStartTsBeforeGC,
	cerror.ErrChangeFeedNotExists, cerror.ErrTargetTsBeforeStartTs, cerror.ErrTableIneligible,
	cerror.ErrFilterRuleInvalid, cerror.ErrChangefeedUpdateRefused, cerror.ErrMySQLConnectionError,
	cerror.ErrMySQLInvalidConfig, cerror.ErrCaptureNotExist,
}

// IsHTTPBadRequestError check if a error is a http bad request error
func IsHTTPBadRequestError(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range httpBadRequestError {
		if e.Equal(err) {
			return true
		}

		rfcCode, ok := cerror.RFCCode(err)
		if ok && e.RFCCode() == rfcCode {
			return true
		}

		if strings.Contains(err.Error(), string(e.RFCCode())) {
			return true
		}
	}
	return false
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
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		log.Error("fail to write data", zap.Error(err))
	}
}

func handleOwnerJob(
	ctx context.Context, capture *capture.Capture, job model.AdminJob,
) error {
	// Use buffered channel to prevernt blocking owner.
	done := make(chan error, 1)
	o, err := capture.GetOwner()
	if err != nil {
		return errors.Trace(err)
	}
	o.EnqueueJob(job, done)
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-done:
		return errors.Trace(err)
	}
}

func handleOwnerRebalance(
	ctx context.Context, capture *capture.Capture, changefeedID model.ChangeFeedID,
) error {
	// Use buffered channel to prevernt blocking owner.
	done := make(chan error, 1)
	o, err := capture.GetOwner()
	if err != nil {
		return errors.Trace(err)
	}
	o.RebalanceTables(changefeedID, done)
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-done:
		return errors.Trace(err)
	}
}

func handleOwnerScheduleTable(
	ctx context.Context, capture *capture.Capture,
	changefeedID model.ChangeFeedID, captureID string, tableID int64,
) error {
	// Use buffered channel to prevernt blocking owner.
	done := make(chan error, 1)
	o, err := capture.GetOwner()
	if err != nil {
		return errors.Trace(err)
	}
	o.ScheduleTable(changefeedID, captureID, tableID, done)
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-done:
		return errors.Trace(err)
	}
}
