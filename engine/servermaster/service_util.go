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
	"fmt"
	"math/rand"
	"time"

	"github.com/pingcap/tiflow/engine/enginepb"
	"google.golang.org/grpc"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// multiClient is an interface that implements all the Client interfaces
// for the individual services running on the server masters.
type multiClient interface {
	enginepb.DiscoveryClient
	enginepb.ResourceManagerClient
	enginepb.TaskSchedulerClient
	enginepb.JobManagerClient
}

type multiClientImpl struct {
	enginepb.DiscoveryClient
	enginepb.ResourceManagerClient
	enginepb.TaskSchedulerClient
	enginepb.JobManagerClient
}

func newMultiClient(conn *grpc.ClientConn) multiClient {
	return &multiClientImpl{
		DiscoveryClient:       enginepb.NewDiscoveryClient(conn),
		ResourceManagerClient: enginepb.NewResourceManagerClient(conn),
		TaskSchedulerClient:   enginepb.NewTaskSchedulerClient(conn),
		JobManagerClient:      enginepb.NewJobManagerClient(conn),
	}
}

func generateNodeID(name string) string {
	val := rand.Uint32()
	id := fmt.Sprintf("%s-%08x", name, val)
	return id
}
