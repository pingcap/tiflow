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
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
)

// ExecutorNotFoundErrInfo provides the details of ErrExecutorNotFound.
type ExecutorNotFoundErrInfo struct {
	rpcerror.Error[rpcerror.Retryable, rpcerror.NotFound]

	ExecutorID  model.ExecutorID
	IsTombstone bool
}

// ErrExecutorNotFound is used when an executor ID is not found for
// a method that requires an executor to exist.
var ErrExecutorNotFound = rpcerror.Normalize[ExecutorNotFoundErrInfo]()

// ExecutorAlreadyExistsErrInfo provides the details of ErrExecutorAlreadyExists.
type ExecutorAlreadyExistsErrInfo struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.AlreadyExists]

	ExecutorID model.ExecutorID
}

// ErrExecutorAlreadyExists is used when an executor ID already exists and causes
// a conflict with a method call.
var ErrExecutorAlreadyExists = rpcerror.Normalize[ExecutorAlreadyExistsErrInfo]()

// CreateWorkerNonTerminateError provides details of ErrCreateWorkerNonTerminate, which
// contains original error returned from CreateWorker
// It means when job master meets this error, the worker should be failovered.
type CreateWorkerNonTerminateError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.Aborted]
	Details string
}

// ErrCreateWorkerNonTerminate indicates the job can be re-created
var ErrCreateWorkerNonTerminate = rpcerror.Normalize[CreateWorkerNonTerminateError]()

// CreateWorkerTerminateError provides details of ErrCreateWorkerTerminate, which
// contains original error returned from CreateWorker
// It means when job master meets this error, the worker should be terminated and
// job master doesn't need to re-create worker.
type CreateWorkerTerminateError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.Aborted]
	Details string
}

// ErrCreateWorkerTerminate indicates the job should be terminated permanently
// from the perspective of business logic.
var ErrCreateWorkerTerminate = rpcerror.Normalize[CreateWorkerTerminateError]()
