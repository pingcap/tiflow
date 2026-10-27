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

package httputil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/openapi"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"go.uber.org/zap"
)

// JobHTTPClient is the http client for job operations, like 'get job detail'
type JobHTTPClient interface {
	// GetJobDetail sends http request to the specific jobmaster to get job detail
	// path format for get job detail openapi is: 'api/v1/jobs/${JobID}/status'
	// If no error happens and the response status code is 2XX, return the response body as the job detail.
	// Otherwise, return nil and an openapi.HTTPError with code and message.
	GetJobDetail(ctx context.Context, jobMasterAddr string, jobID string) ([]byte, *openapi.HTTPError)
	// Close releases the resources
	Close()
}

// jobHTTPClientImpl is the http client for getting job detail
type jobHTTPClientImpl struct {
	cli *httputil.Client
}

// NewJobHTTPClient news a JobHTTPClient.
func NewJobHTTPClient(cli *httputil.Client) JobHTTPClient {
	return &jobHTTPClientImpl{
		cli: cli,
	}
}

// GetJobDetail implements the JobHTTPClient.GetJobDetail
// if we get job detail from jobmaster successfully, returns job detail and nil
// if we get a not 2XX response from jobmaster, return response body and an error
// if we can't get response from jobmaster, return nil and an error
func (c *jobHTTPClientImpl) GetJobDetail(ctx context.Context, jobMasterAddr string, jobID string) ([]byte, *openapi.HTTPError) {
	url := fmt.Sprintf("http://%s%s", jobMasterAddr, fmt.Sprintf(openapi.JobDetailAPIFormat, jobID))
	log.Info("get job detail from job master", zap.String("job-id", jobID))
	resp, err := c.cli.Get(ctx, url)
	if err != nil {
		errOut := errors.ErrJobManagerGetJobDetailFail.Wrap(err).GenWithStackByArgs()
		return nil, openapi.NewHTTPError(errOut)
	}
	log.Debug("job master response", zap.Any("response status", resp.Status), zap.String("job-id", jobID))
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		errOut := errors.ErrJobManagerGetJobDetailFail.Wrap(err).GenWithStackByArgs()
		return nil, openapi.NewHTTPError(errOut)
	}

	// if response code is 2XX, body should be the job detail
	if resp.StatusCode/100 == http.StatusOK/100 {
		log.Debug("job master response", zap.String("job-id", jobID), zap.Int("status-code", resp.StatusCode))
		return body, nil
	}
	log.Warn(
		"failed to get job detail from job master",
		zap.String("job-id", jobID),
		zap.String("status", resp.Status),
	)

	httpErr := &openapi.HTTPError{}
	if err := json.Unmarshal(body, httpErr); err != nil {
		errOut := errors.ErrJobManagerGetJobDetailFail.GenWithStack(
			"get job detail from job master failed with wrong error format, status: %s", resp.Status)
		return nil, openapi.NewHTTPError(errOut)
	}
	return nil, httpErr
}

// Close implements the JobHTTPClient.Close
func (c *jobHTTPClientImpl) Close() {
	if c.cli != nil {
		c.cli.CloseIdleConnections()
	}
}
