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

package common

import (
	"context"

	"github.com/pingcap/tiflow/dm/pb"
)

// OperateTask does operation on task.
func OperateTask(op pb.TaskOp, name string, sources []string) (*pb.OperateTaskResponse, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.OperateTaskResponse{}
	err := SendRequest(
		ctx,
		"OperateTask",
		&pb.OperateTaskRequest{
			Op:      op,
			Name:    name,
			Sources: sources,
		},
		&resp,
	)
	return resp, err
}
