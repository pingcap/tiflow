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

package client

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/client/internal"
)

// TaskSchedulerClient is an interface for a client to the task scheduler
// in the server master.
type TaskSchedulerClient interface {
	ScheduleTask(
		ctx context.Context,
		request *pb.ScheduleTaskRequest,
	) (*pb.ScheduleTaskResponse, error)

	ReportExecutorWorkload(
		ctx context.Context,
		request *pb.ExecWorkloadRequest,
	) error
}

type taskSchedulerClient struct {
	cli pb.TaskSchedulerClient
}

// NewTaskSchedulerClient returns a TaskSchedulerClient.
func NewTaskSchedulerClient(cli pb.TaskSchedulerClient) TaskSchedulerClient {
	return &taskSchedulerClient{cli: cli}
}

func (c *taskSchedulerClient) ScheduleTask(
	ctx context.Context,
	request *pb.ScheduleTaskRequest,
) (*pb.ScheduleTaskResponse, error) {
	call := internal.NewCall(
		c.cli.ScheduleTask,
		request)
	return call.Do(ctx)
}

func (c *taskSchedulerClient) ReportExecutorWorkload(
	ctx context.Context,
	request *pb.ExecWorkloadRequest,
) error {
	call := internal.NewCall(
		c.cli.ReportExecutorWorkload,
		request)
	resp, err := call.Do(ctx)
	if err != nil {
		return err
	}
	if resp.Err != nil && resp.Err.Code != pb.ErrorCode_None {
		return errors.Errorf("ReportExecutorWorkload: %s",
			resp.String())
	}
	return nil
}
