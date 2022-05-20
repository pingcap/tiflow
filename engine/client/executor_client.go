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
