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
	// If no error happens and the response status code is 2XX, return the response body as the job detail
	GetJobDetail(ctx context.Context, jobMasterAddr string, jobID string) ([]byte, error)
	// Close releases the resources
	Close()
}

// jobHTTPClientImpl is the http client for getting job detail
type jobHTTPClientImpl struct {
	cli *httputil.Client
}

// NewJobHTTPClient news a jobHTTPClientImpl
func NewJobHTTPClient(cli *httputil.Client) *jobHTTPClientImpl {
	return &jobHTTPClientImpl{
		cli: cli,
	}
}

// GetJobDetail implements the JobHTTPClient.GetJobDetail
// if we get job detail from jobmaster successfully, returns job detail and nil
// if we get a not 2XX response from jobmaster, return response body and an error
// if we can't get response from jobmaster, return nil and an error
func (c *jobHTTPClientImpl) GetJobDetail(ctx context.Context, jobMasterAddr string, jobID string) ([]byte, error) {
	url := fmt.Sprintf("http://%s%s", jobMasterAddr, fmt.Sprintf(openapi.JobDetailAPIFormat, jobID))
	log.Info("get job detail from job master", zap.String("url", url))
	resp, err := c.cli.Get(ctx, url)
	if err != nil {
		return nil, errors.ErrJobManagerGetJobDetailFail.Wrap(err)
	}
	log.Debug("job master response", zap.Any("response status", resp.Status), zap.String("url", url))
	defer resp.Body.Close()

	// many conditions may results 404:
	// 1. jobmaster is failover or no longer exists
	// 2. jobmaster doesn't implements job detail openapi
	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.ErrJobManagerRespStatusCode404.GenWithStackByArgs()
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.ErrJobManagerReadRespBodyFail.Wrap(err)
	}

	log.Debug("job master response", zap.Any("response body", string(body)), zap.String("url", url))
	// if response code is 2XX, body should be the job detail
	if resp.StatusCode/100 == http.StatusOK/100 {
		return body, nil
	}

	// since we don't have the normalized response body format, return body with an error
	log.Warn("get job detail", zap.String("job-id", jobID), zap.Any("body", body))
	return body, errors.ErrJobManagerRespStatusCodeNot2XX.GenWithStackByArgs()
}

// Close implements the JobHTTPClient.Close
func (c *jobHTTPClientImpl) Close() {
	if c.cli != nil {
		c.cli.CloseIdleConnections()
	}
}
