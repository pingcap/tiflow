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

package openapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// JobAPIPrefix is the prefix of the job API.
const JobAPIPrefix = "/api/v1/jobs/"

// JobDetailAPIFormat is the path format for job detail status
// the entire path is: /api/v1/jobs/${jobID}/status
const JobDetailAPIFormat = JobAPIPrefix + "%s/status"

// HTTPError is the error format for http response.
type HTTPError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// NewHTTPError creates a new HTTPError.
func NewHTTPError(err error) *HTTPError {
	rfcCode, ok := errors.RFCCode(err)
	if !ok {
		rfcCode = errors.ErrUnknown.RFCCode()
	}
	return &HTTPError{
		Code:    string(rfcCode),
		Message: strings.TrimPrefix(err.Error(), fmt.Sprintf("[%s]", rfcCode)),
	}
}

// WriteHTTPError writes error to http response with normalized error format.
func WriteHTTPError(w http.ResponseWriter, err error) {
	httpErr := NewHTTPError(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(errors.HTTPStatusCode(err))
	if err := json.NewEncoder(w).Encode(httpErr); err != nil {
		log.Warn("Failed to write error response", zap.Error(err))
	}
}
