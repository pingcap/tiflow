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

	"github.com/gogo/status"
	"github.com/pingcap/errors"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/pkg/retry"
)

// ExecutorClient defines an interface that supports sending gRPC from server
// master to executor.
type ExecutorClient interface {
	baseExecutorClient

	DispatchTask(
		ctx context.Context,
		args *DispatchTaskArgs,
		startWorkerTimer StartWorkerCallback,
		abortWorker AbortWorkerCallback,
	) error

	RemoveLocalResource(
		ctx context.Context,
		req *pb.RemoveLocalResourceRequest,
	) error
}

func newExecutorClient(addr string) (ExecutorClient, error) {
	base, err := newBaseExecutorClient(addr)
	if err != nil {
		return nil, err
	}

	taskDispatcher := newTaskDispatcher(base)
	return &executorClientImpl{
		baseExecutorClientImpl: base,
		TaskDispatcher:         taskDispatcher,
	}, nil
}

type executorClientImpl struct {
	*baseExecutorClientImpl
	*TaskDispatcher
}

func (c *executorClientImpl) RemoveLocalResource(
	ctx context.Context,
	req *pb.RemoveLocalResourceRequest,
) error {
	err := retry.Do(ctx, func() error {
		_, err := c.Send(ctx, &ExecutorRequest{
			Cmd: CmdRemoveLocalResourceRequest,
			Req: req,
		})
		return err
	}, retry.WithIsRetryableErr(func(err error) bool {
		st := status.Convert(err)
		switch st.Code() {
		case codes.NotFound, codes.InvalidArgument, codes.Unknown:
			return false
		default:
			return true
		}
	}))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
