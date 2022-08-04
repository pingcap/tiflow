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
	"github.com/pingcap/tiflow/engine/pb"
	"google.golang.org/grpc"
)

// multiClient is an interface that implements all the Client interfaces
// for the individual services running on the server masters.
type multiClient interface {
	pb.DiscoveryClient
	pb.ResourceManagerClient
	pb.TaskSchedulerClient
	pb.JobManagerClient
}

type multiClientImpl struct {
	pb.DiscoveryClient
	pb.ResourceManagerClient
	pb.TaskSchedulerClient
	pb.JobManagerClient
}

func newMultiClient(conn *grpc.ClientConn) multiClient {
	return &multiClientImpl{
		DiscoveryClient:       pb.NewDiscoveryClient(conn),
		ResourceManagerClient: pb.NewResourceManagerClient(conn),
		TaskSchedulerClient:   pb.NewTaskSchedulerClient(conn),
		JobManagerClient:      pb.NewJobManagerClient(conn),
	}
}
