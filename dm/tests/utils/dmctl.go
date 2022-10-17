// Copyright 2019 PingCAP, Inc.
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

package utils

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pb"
	"google.golang.org/grpc"
)

// CreateDmCtl creates a gRPC client to DM-master.
func CreateDmCtl(addr string) (pb.MasterClient, error) {
	// TODO: use tls, this function is not used
	//nolint:staticcheck
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return pb.NewMasterClient(conn), nil
}

// StartTask starts a task with the specified config.
func StartTask(ctx context.Context, cli pb.MasterClient, configFile string, workers []string) error {
	content, err := os.ReadFile(configFile)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := cli.StartTask(ctx, &pb.StartTaskRequest{
		Task:    string(content),
		Sources: workers,
	})
	if err != nil {
		return errors.Trace(err)
	}

	if !resp.GetResult() {
		return errors.Errorf("start task resp error: %s", resp.GetMsg())
	}

	for _, wp := range resp.GetSources() {
		if !wp.GetResult() && !strings.Contains(wp.GetMsg(), "request is timeout, but request may be successful") {
			return errors.Errorf("fail to start task %v: %s", string(content), wp.GetMsg())
		}
	}

	return nil
}

// OperateTask does operations on the task.
func OperateTask(ctx context.Context, cli pb.MasterClient, op pb.TaskOp, name string, workers []string) error {
	resp, err := cli.OperateTask(ctx, &pb.OperateTaskRequest{
		Op:      op,
		Name:    name,
		Sources: workers,
	})
	if err != nil {
		return errors.Trace(err)
	}

	for _, wp := range resp.GetSources() {
		if !wp.GetResult() {
			return errors.Errorf("fail to do %v operate on task %s: %s", op, name, wp.GetMsg())
		}
	}

	return nil
}
