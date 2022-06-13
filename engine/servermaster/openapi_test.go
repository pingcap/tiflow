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

package servermaster

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/engine/model"

	"github.com/gin-gonic/gin"

	pb "github.com/pingcap/tiflow/engine/enginepb"
)

func init() {
	gin.SetMode(gin.TestMode)
}

type mockManager struct {
	JobManager
	ExecutorManager
}

func (mockManager) QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse {
	return nil
}

func (mockManager) GetAddr(executorID model.ExecutorID) (string, bool) {
	return "", false
}

func TestOpenAPI(t *testing.T) {
	openapi := NewOpenAPI(mockManager{}, mockManager{})
	router := gin.New()
	RegisterOpenAPIRoutes(router, openapi)

	// TODO: Test forwarding.
}
