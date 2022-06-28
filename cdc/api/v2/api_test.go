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

package v2

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

// MockPDClient mocks pd.Client to facilitate unit testing.
type MockPDClient struct {
	pd.Client
}

func (m *MockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, nil
}

type testCase struct {
	url    string
	method string
}

func newRouter(c capture.Capture) *gin.Engine {
	router := gin.New()
	RegisterOpenAPIV2Routes(router, NewOpenAPIV2(c))
	return router
}
