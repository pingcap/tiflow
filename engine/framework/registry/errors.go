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
package registry

import "github.com/pingcap/tiflow/engine/pkg/rpcerror"

// CreateWorkerRetryableError provides details of ErrCreateWorkerRetryable, which
// contains original error returned from CreateWorker
// It means when job master meets this error, the worker should be failovered.
type CreateWorkerRetryableError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.Aborted]
	Details string
}

// ErrCreateWorkerRetryable indicates the job can be re-created
var ErrCreateWorkerRetryable = rpcerror.Normalize[CreateWorkerRetryableError]()

// CreateWorkerTerminateError provides details of ErrCreateWorkerTerminate, which
// contains original error returned from CreateWorker
// It means when job master meets this error, the worker should be terminated and
// job master doesn't need to re-create worker.
type CreateWorkerTerminateError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.Aborted]
	Details string
}

// ErrCreateWorkerTerminate indicates the job should be terminated permanently
var ErrCreateWorkerTerminate = rpcerror.Normalize[CreateWorkerTerminateError]()
