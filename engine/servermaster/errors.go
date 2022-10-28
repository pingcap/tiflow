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

import "github.com/pingcap/tiflow/engine/pkg/rpcerror"

// ErrJobNotFound indicates that a given job cannot be found.
var ErrJobNotFound = rpcerror.Normalize[JobNotFoundError](rpcerror.WithMessage("job not found"))

// JobNotFoundError provides details of an ErrJobNotFound.
type JobNotFoundError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.NotFound]

	JobID string
}

// ErrJobAlreadyExists indicates that a given job already exists.
var ErrJobAlreadyExists = rpcerror.Normalize[JobAlreadyExistsError](rpcerror.WithMessage("job already exists"))

// JobAlreadyExistsError provides details of an ErrJobAlreadyExists.
type JobAlreadyExistsError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.AlreadyExists]

	JobID string
}

// ErrJobNotTerminated indicates that a given job is not in terminated state.
var ErrJobNotTerminated = rpcerror.Normalize[JobNotTerminatedError]()

// JobNotTerminatedError provides details of an ErrJobNotTerminated.
type JobNotTerminatedError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.FailedPrecondition]

	JobID string
}

// ErrJobNotRunning indicates that a given job is not running.
// It's usually caused when caller tries to cancel a job that is not running.
var ErrJobNotRunning = rpcerror.Normalize[JobNotRunningError]()

// JobNotRunningError provides details of an ErrJobNotRunning.
type JobNotRunningError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.FailedPrecondition]

	JobID string
}

// ErrMetaStoreNotExists indicates the requested metastore does not exist.
var ErrMetaStoreNotExists = rpcerror.Normalize[MetaStoreNotExistsError]()

// MetaStoreNotExistsError provides details of an ErrMetaStoreNotExists.
type MetaStoreNotExistsError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.NotFound]

	StoreID string
}

// ErrUnknownExecutor indicates that executor is unknown in the executor manager.
var ErrUnknownExecutor = rpcerror.Normalize[UnknownExecutorError]()

// UnknownExecutorError provides details of an ErrUnknownExecutor.
type UnknownExecutorError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.FailedPrecondition]

	ExecutorID string
}

// ErrTombstoneExecutorError indicates that the executor is in tombstone state.
var ErrTombstoneExecutorError = rpcerror.Normalize[TombstoneExecutorError]()

// TombstoneExecutorError provides details of an ErrTombstoneExecutorError.
type TombstoneExecutorError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.FailedPrecondition]

	ExecutorID string
}
