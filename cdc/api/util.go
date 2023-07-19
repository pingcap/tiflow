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
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"go.uber.org/zap"
)

// httpBadRequestError is some errors that will cause a BadRequestError in http handler
var httpBadRequestError = []*errors.Error{
	cerror.ErrAPIInvalidParam, cerror.ErrSinkURIInvalid, cerror.ErrStartTsBeforeGC,
	cerror.ErrChangeFeedNotExists, cerror.ErrTargetTsBeforeStartTs, cerror.ErrTableIneligible,
	cerror.ErrFilterRuleInvalid, cerror.ErrChangefeedUpdateRefused, cerror.ErrMySQLConnectionError,
	cerror.ErrMySQLInvalidConfig, cerror.ErrCaptureNotExist, cerror.ErrSchedulerRequestFailed,
}

const (
	// forwardFromCapture is a header to be set when forwarding requests to owner
	forwardFromCapture = "TiCDC-ForwardFromCapture"
	// forwardTimes is a header to identify how many times the request has been forwarded
	forwardTimes = "TiCDC-ForwardTimes"
	// maxForwardTimes is the max time a request can be forwarded,  non-controller->controller->changefeed owner
	maxForwardTimes = 2
)

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

// WriteError write error message to response
func WriteError(w http.ResponseWriter, statusCode int, err error) {
	w.WriteHeader(statusCode)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		log.Error("write error", zap.Error(err))
	}
}

// WriteData write data to response with http status code 200
func WriteData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		log.Error("invalid json data", zap.Any("data", data), zap.Error(err))
		WriteError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		log.Error("fail to write data", zap.Error(err))
	}
}

// HandleOwnerJob enqueue the admin job
func HandleOwnerJob(
	ctx context.Context, capture capture.Capture, job model.AdminJob,
) error {
	// Use buffered channel to prevent blocking owner from happening.
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

// HandleOwnerBalance balance the changefeed tables
func HandleOwnerBalance(
	ctx context.Context, capture capture.Capture, changefeedID model.ChangeFeedID,
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

// HandleOwnerScheduleTable schedule tables
func HandleOwnerScheduleTable(
	ctx context.Context, capture capture.Capture,
	changefeedID model.ChangeFeedID, captureID string, tableID int64,
) error {
	// Use buffered channel to prevent blocking owner.
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

// ForwardToController forwards a request to the controller
func ForwardToController(c *gin.Context, p capture.Capture) {
	ctx := c.Request.Context()
	info, err := p.Info()
	if err != nil {
		_ = c.Error(err)
		return
	}

	var controller *model.CaptureInfo
	// get controller info
	controller, err = p.GetControllerCaptureInfo(ctx)
	if err != nil {
		log.Info("get controller failed", zap.Error(err))
		_ = c.Error(err)
		return
	}
	ForwardToCapture(c, info.ID, controller.AdvertiseAddr)
}

// ForwardToCapture forward request to another
func ForwardToCapture(c *gin.Context, fromID, toAddr string) {
	ctx := c.Request.Context()

	timeStr := c.GetHeader(forwardTimes)
	var (
		err              error
		lastForwardTimes uint64
	)
	if len(timeStr) != 0 {
		lastForwardTimes, err = strconv.ParseUint(timeStr, 10, 64)
		if err != nil {
			_ = c.Error(cerror.ErrRequestForwardErr.FastGenByArgs())
			return
		}
		if lastForwardTimes > maxForwardTimes {
			_ = c.Error(cerror.ErrRequestForwardErr.FastGenByArgs())
			return
		}
	}

	security := config.GetGlobalServerConfig().Security

	// init a request
	req, err := http.NewRequestWithContext(
		ctx, c.Request.Method, c.Request.RequestURI, c.Request.Body)
	if err != nil {
		_ = c.Error(err)
		return
	}

	req.URL.Host = toAddr
	// we should check tls config instead of security here because
	// security will never be nil
	if tls, _ := security.ToTLSConfigWithVerify(); tls != nil {
		req.URL.Scheme = "https"
	} else {
		req.URL.Scheme = "http"
	}
	for k, v := range c.Request.Header {
		for _, vv := range v {
			req.Header.Add(k, vv)
		}
	}
	log.Info("forwarding request to capture",
		zap.String("url", c.Request.RequestURI),
		zap.String("method", c.Request.Method),
		zap.String("fromID", fromID),
		zap.String("toAddr", toAddr),
		zap.String("forwardTimes", timeStr))

	req.Header.Add(forwardFromCapture, fromID)
	lastForwardTimes++
	req.Header.Add(forwardTimes, strconv.Itoa(int(lastForwardTimes)))
	// forward toAddr owner
	cli, err := httputil.NewClient(security)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp, err := cli.Do(req)
	if err != nil {
		_ = c.Error(err)
		return
	}

	// write header
	for k, values := range resp.Header {
		for _, v := range values {
			c.Header(k, v)
		}
	}

	// write status code
	c.Status(resp.StatusCode)

	// write response body
	defer resp.Body.Close()
	_, err = bufio.NewReader(resp.Body).WriteTo(c.Writer)
	if err != nil {
		_ = c.Error(err)
		return
	}
}

// HandleOwnerDrainCapture schedule drain the target capture
func HandleOwnerDrainCapture(
	ctx context.Context, capture capture.Capture, captureID string,
) (*model.DrainCaptureResp, error) {
	// Use buffered channel to prevent blocking owner.
	done := make(chan error, 1)
	o, err := capture.GetOwner()
	if err != nil {
		return nil, errors.Trace(err)
	}

	query := scheduler.Query{
		CaptureID: captureID,
	}

	o.DrainCapture(&query, done)

	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-done:
	}

	return query.Resp.(*model.DrainCaptureResp), errors.Trace(err)
}
