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

package mock

import (
	"context"

	"github.com/pingcap/tiflow/engine/pkg/openapi"
)

// MockNilReturnJobHTTPClient mocks a JobHTTPClient which return (nil, nil)
//
//nolint:revive
type MockNilReturnJobHTTPClient struct{}

// NewMockNilReturnJobHTTPClient news a JobHTTPClient mocker which return (nil, nil)
func NewMockNilReturnJobHTTPClient() *MockNilReturnJobHTTPClient {
	return &MockNilReturnJobHTTPClient{}
}

// GetJobDetail implements the JobHTTPClient.GetJobDetail
func (c *MockNilReturnJobHTTPClient) GetJobDetail(ctx context.Context, address string, jobID string) ([]byte, *openapi.HTTPError) {
	return nil, nil
}

// Close implements the JobHTTPClient.Close
func (c *MockNilReturnJobHTTPClient) Close() {}
