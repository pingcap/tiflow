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

// Copyright 2021 PingCAP, Inc.
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

package errors

import (
	"github.com/pingcap/errors"
)

// all dataflow engine errors
var (
	// general errors
	ErrUnknown = errors.Normalize(
		"unknown error",
		errors.RFCCodeText("DFLOW:ErrUnknown"),
	)
	ErrInvalidArgument = errors.Normalize(
		"invalid argument: %s",
		errors.RFCCodeText("DFLOW:ErrInvalidArgument"),
	)

	ErrClusterResourceNotEnough = errors.Normalize(
		"cluster resource is not enough, please scale out the cluster",
		errors.RFCCodeText("DFLOW:ErrClusterResourceNotEnough"),
	)

	ErrDecodeEtcdKeyFail = errors.Normalize(
		"failed to decode etcd key: %s",
		errors.RFCCodeText("DFLOW:ErrDecodeEtcdKeyFail"),
	)
	ErrEtcdAPIError = errors.Normalize(
		"etcd api returns error",
		errors.RFCCodeText("DFLOW:ErrEtcdAPIError"),
	)

	// master related errors
	ErrMasterNotReady = errors.Normalize(
		"master is not ready",
		errors.RFCCodeText("DFLOW:ErrMasterNotReady"),
	)
	ErrMasterNoLeader = errors.Normalize(
		"master has no leader",
		errors.RFCCodeText("DFLOW:ErrMasterNoLeader"),
	)
	ErrMasterDecodeConfigFile = errors.Normalize(
		"decode config file failed",
		errors.RFCCodeText("DFLOW:ErrMasterDecodeConfigFile"),
	)
	ErrMasterConfigUnknownItem = errors.Normalize(
		"master config contains unknown configuration options: %s",
		errors.RFCCodeText("DFLOW:ErrMasterConfigUnknownItem"),
	)
	ErrMasterNotFound = errors.Normalize(
		"master is not found: master ID %s",
		errors.RFCCodeText("DFLOW:ErrMasterNotFound"),
	)
	ErrMasterClosed = errors.Normalize(
		"master has been closed explicitly: master ID %s",
		errors.RFCCodeText("DFLOW:ErrMasterClosed"),
	)
	ErrMasterConcurrencyExceeded = errors.Normalize(
		"master has reached concurrency quota",
		errors.RFCCodeText("DFLOW:ErrMasterConcurrencyExceeded"),
	)
	ErrMasterCreateWorkerBackoff = errors.Normalize(
		"create worker is being backoff, retry later",
		errors.RFCCodeText("DFLOW:ErrMasterCreateWorkerBackoff"),
	)
	ErrMasterCreateWorkerTerminate = errors.Normalize(
		"create worker is terminated, won't backoff any more",
		errors.RFCCodeText("DFLOW:ErrMasterCreateWorkerTerminate"),
	)
	ErrMasterInvalidMeta = errors.Normalize(
		"invalid master meta data: %s",
		errors.RFCCodeText("DFLOW:ErrMasterInvalidMeta"),
	)
	ErrInvalidMasterMessage = errors.Normalize(
		"invalid master message: %s",
		errors.RFCCodeText("DFLOW:ErrInvalidMasterMessage"),
	)
	ErrSendingMessageToTombstone = errors.Normalize(
		"trying to send message to a tombstone worker handle: %s",
		errors.RFCCodeText("DFLOW:ErrSendingMessageToTombstone"),
	)
	ErrMasterInterfaceNotFound = errors.Normalize(
		"basemaster interface not found",
		errors.RFCCodeText("DFLOW:ErrMasterInterfaceNotFound"),
	)
	ErrExecutorWatcherClosed = errors.Normalize(
		"executor watcher is closed",
		errors.RFCCodeText("DFLOW:ErrExecutorWatcherClosed"),
	)

	ErrWorkerTypeNotFound = errors.Normalize(
		"worker type is not found: type %d",
		errors.RFCCodeText("DFLOW:ErrWorkerTypeNotFound"),
	)
	ErrWorkerNotFound = errors.Normalize(
		"worker is not found: worker ID %s",
		errors.RFCCodeText("DFLOW:ErrWorkerNotFound"),
	)
	ErrWorkerOffline = errors.Normalize(
		"worker is offline: workerID: %s, error message: %s",
		errors.RFCCodeText("DFLOW:ErrWorkerOffline"),
	)
	ErrWorkerSuicide = errors.Normalize(
		"worker has committed suicide due to master(%s) having timed out",
		errors.RFCCodeText("DFLOW:ErrWorkerSuicide"),
	)
	ErrWorkerUpdateStatusTryAgain = errors.Normalize(
		"worker should try again in updating the status",
		errors.RFCCodeText("DFLOW:ErrWorkerUpdateStatusTryAgain"),
	)
	// TODO: unify the following three errors into one ErrWorkerTerminated
	ErrWorkerFinish = errors.Normalize(
		"worker finished and exited",
		errors.RFCCodeText("DFLOW:ErrWorkerFinish"),
	)
	ErrWorkerCancel = errors.Normalize(
		"worker is canceled",
		errors.RFCCodeText("DFLOW:ErrWorkerCancel"),
	)
	ErrWorkerFailed = errors.Normalize(
		"worker is failed permanently",
		errors.RFCCodeText("DFLOW:ErrWorkerFailed"),
	)
	ErrTooManyStatusUpdates = errors.Normalize(
		"there are too many pending worker status updates: %d",
		errors.RFCCodeText("DFLOW:ErrTooManyStatusUpdates"),
	)
	ErrWorkerHalfExit = errors.Normalize(
		"the worker is in half-exited state",
		errors.RFCCodeText("DFLOW:ErrWorkerHalfExit"),
	)
	// ErrCreateWorkerNonTerminate indicates the job can be re-created.
	ErrCreateWorkerNonTerminate = errors.Normalize(
		"create worker is not terminated",
		errors.RFCCodeText("DFLOW:ErrCreateWorkerNonTerminate"),
	)
	// ErrCreateWorkerTerminate indicates the job should be terminated permanently
	// from the perspective of business logic.
	ErrCreateWorkerTerminate = errors.Normalize(
		"create worker is terminated",
		errors.RFCCodeText("DFLOW:ErrCreateWorkerTerminate"),
	)

	// job manager related errors
	ErrJobManagerGetJobDetailFail = errors.Normalize(
		"failed to get job detail from job master",
		errors.RFCCodeText("DFLOW:ErrJobManagerGetJobDetailFail"),
	)

	// master etcd related errors
	ErrMasterEtcdEpochFail = errors.Normalize(
		"server master generate epoch fail",
		errors.RFCCodeText("DFLOW:ErrMasterEtcdEpochFail"),
	)

	// executor related errors
	ErrUnknownExecutor = errors.Normalize(
		"unknown executor: %s",
		errors.RFCCodeText("DFLOW:ErrUnknownExecutor"),
	)
	ErrTombstoneExecutor = errors.Normalize(
		"tombstone executor: %s",
		errors.RFCCodeText("DFLOW:ErrTombstoneExecutor"),
	)
	ErrExecutorNotFound = errors.Normalize(
		"executor %s not found",
		errors.RFCCodeText("DFLOW:ErrExecutorNotFound"),
	)
	ErrExecutorAlreadyExists = errors.Normalize(
		"executor %s already exists",
		errors.RFCCodeText("DFLOW:ErrExecutorAlreadyExists"),
	)
	ErrExecutorDecodeConfigFile = errors.Normalize(
		"decode config file failed",
		errors.RFCCodeText("DFLOW:ErrExecutorDecodeConfigFile"),
	)
	ErrExecutorConfigUnknownItem = errors.Normalize(
		"master config contains unknown configuration options: %s",
		errors.RFCCodeText("DFLOW:ErrExecutorConfigUnknownItem"),
	)
	ErrHeartbeat = errors.Normalize(
		"heartbeat error type: %s",
		errors.RFCCodeText("DFLOW:ErrHeartbeat"),
	)
	ErrRuntimeIncomingQueueFull = errors.Normalize(
		"runtime has too many pending CreateWorker requests",
		errors.RFCCodeText("DFLOW:ErrRuntimeIncomingQueueFull"),
	)
	ErrRuntimeIsClosed = errors.Normalize(
		"runtime has been closed",
		errors.RFCCodeText("DFLOW:ErrRuntimeIsClosed"),
	)
	ErrRuntimeDuplicateTaskID = errors.Normalize(
		"trying to add a task with the same ID as an existing one",
		errors.RFCCodeText("DFLOW:ErrRuntimeDuplicateTaskID %s"),
	)
	ErrRuntimeClosed = errors.Normalize(
		"runtime has been closed",
		errors.RFCCodeText("DFLOW:ErrRuntimeClosed"),
	)
	ErrExecutorNotFoundForMessage = errors.Normalize(
		"cannot find the executor for p2p messaging",
		errors.RFCCodeText("DFLOW:ErrExecutorNotFoundForMessage"),
	)
	ErrMasterTooManyPendingEvents = errors.Normalize(
		"master has too many pending events",
		errors.RFCCodeText("DFLOW:ErrMasterTooManyPendingEvents"),
	)

	// meta related errors
	ErrMetaNewClientFail = errors.Normalize(
		"create meta client fail",
		errors.RFCCodeText("DFLOW:ErrMetaNewClientFail"),
	)
	ErrMetaOpFail = errors.Normalize(
		"meta operation fail",
		errors.RFCCodeText("DFLOW:ErrMetaOpFail"),
	)
	ErrMetaOptionInvalid = errors.Normalize(
		"meta option invalid",
		errors.RFCCodeText("DFLOW:ErrMetaOptionInvalid"),
	)
	ErrMetaOptionConflict = errors.Normalize(
		"WithRange/WithPrefix/WithFromKey, more than one option are used",
		errors.RFCCodeText("DFLOW:ErrMetaOptionConflict"),
	)
	ErrMetaEmptyKey = errors.Normalize(
		"meta empty key",
		errors.RFCCodeText("DFLOW:ErrMetaEmptyKey"),
	)
	ErrMetaNestedTxn = errors.Normalize(
		"meta unsupported nested txn",
		errors.RFCCodeText("DFLOW:ErrMetaNestedTxn"),
	)
	ErrMetaCommittedTxn = errors.Normalize(
		"meta already committed txn",
		errors.RFCCodeText("DFLOW:ErrMetaCommittedTxn"),
	)
	ErrMetaStoreIDDuplicate = errors.Normalize(
		"metastore id duplicated",
		errors.RFCCodeText("DFLOW:ErrMetaStoreIDDuplicate"),
	)
	ErrMetaEntryNotFound = errors.Normalize(
		"meta entry not found",
		errors.RFCCodeText("DFLOW:ErrMetaEntryNotFound"),
	)
	ErrMetaParamsInvalid = errors.Normalize(
		"meta params invalid:%s",
		errors.RFCCodeText("DFLOW:ErrMetaParamsInvalid"),
	)
	ErrMetaClientTypeNotSupport = errors.Normalize(
		"meta client type not support:%s",
		errors.RFCCodeText("DFLOW:ErrMetaClientTypeNotSupport"),
	)

	// Resource related errors
	ErrDuplicateResourceID = errors.Normalize(
		"duplicate resource ID: %s",
		errors.RFCCodeText("DFLOW:ErrDuplicateResourceID"),
	)
	ErrResourceAlreadyExists = errors.Normalize(
		"resource %s already exists",
		errors.RFCCodeText("DFLOW:ErrResourceAlreadyExists"),
	)
	ErrIllegalResourcePath = errors.Normalize(
		"resource path is illegal: %s",
		errors.RFCCodeText("DFLOW:ErrIllegalResourcePath"),
	)
	ErrResourceDoesNotExist = errors.Normalize(
		"resource does not exists: %s",
		errors.RFCCodeText("DFLOW:ErrResourceDoesNotExist"),
	)
	ErrResourceConflict = errors.Normalize(
		"resource % on executor %s conflicts with resource %s on executor %s",
		errors.RFCCodeText("DFLOW:ErrResourceConflict"),
	)
	ErrReadLocalFileDirectoryFailed = errors.Normalize(
		"reading local file resource directory failed",
		errors.RFCCodeText("DFLOW:ErrReadLocalFileDirectoryFailed"),
	)
	ErrCreateLocalFileDirectoryFailed = errors.Normalize(
		"creating local file resource directory failed",
		errors.RFCCodeText("DFLOW:ErrCreateLocalFileDirectoryFailed"),
	)
	ErrCleaningLocalTempFiles = errors.Normalize(
		"errors is encountered when cleaning local temp files",
		errors.RFCCodeText("DFLOW:ErrCleaningLocalTempFiles"),
	)
	ErrRemovingLocalResource = errors.Normalize(
		"removing a local resource file directory has failed",
		errors.RFCCodeText("DFLOW:ErrRemovingLocalResource"),
	)
	ErrFailToCreateExternalStorage = errors.Normalize(
		"failed to create external storage",
		errors.RFCCodeText("DFLOW:ErrFailToCreateExternalStorage"),
	)
	ErrInvalidResourceHandle = errors.Normalize(
		"using an invalid resource handle",
		errors.RFCCodeText("DFLOW:ErrInvalidResourceHandle"),
	)
	ErrLocalFileDirNotWritable = errors.Normalize(
		"local resource directory not writable",
		errors.RFCCodeText("DFLOW:ErrLocalFileDirNotWritable"),
	)
	ErrNoQualifiedExecutor = errors.Normalize(
		"no executor is available for scheduling",
		errors.RFCCodeText("DFLOW:ErrNoQualifiedExecutor"),
	)
	ErrFilterNoResult = errors.Normalize(
		"filter % returns no result",
		errors.RFCCodeText("DFLOW:ErrFilterNoResult"),
	)
	ErrSelectorUnsatisfied = errors.Normalize(
		"selector %v is not satisfied",
		errors.RFCCodeText("DFLOW:ErrSelectorUnsatisfied"),
	)
	ErrResourceFilesNotFound = errors.Normalize(
		"resource files not found",
		errors.RFCCodeText("DFLOW:ErrResourceFilesNotFound"),
	)
	ErrResourceMetastoreError = errors.Normalize(
		"resource metastore error",
		errors.RFCCodeText("DFLOW:ErrResourceMetastoreError"),
	)

	// Job related error
	ErrJobNotFound = errors.Normalize(
		"job %s is not found",
		errors.RFCCodeText("DFLOW:ErrJobNotFound"),
	)
	ErrJobAlreadyExists = errors.Normalize(
		"job %s already exists",
		errors.RFCCodeText("DFLOW:ErrJobAlreadyExists"),
	)
	ErrJobAlreadyCanceled = errors.Normalize(
		"job %s is already canceled",
		errors.RFCCodeText("DFLOW:ErrJobAlreadyCanceled"),
	)
	ErrJobNotTerminated = errors.Normalize(
		"job %s is not terminated",
		errors.RFCCodeText("DFLOW:ErrJobNotTerminated"),
	)
	ErrJobNotRunning = errors.Normalize(
		"job %s is not running",
		errors.RFCCodeText("DFLOW:ErrJobNotRunning"),
	)

	// metastore related errors
	ErrMetaStoreNotExists = errors.Normalize(
		"metastore %s does not exist",
		errors.RFCCodeText("DFLOW:ErrMetaStoreNotExists"),
	)

	// cli related errors
	ErrInvalidCliParameter = errors.Normalize(
		"invalid cli parameters",
		errors.RFCCodeText("DFLOW:ErrInvalidCliParameter"),
	)

	ErrIncompatibleSchedulerRequest = errors.Normalize(
		"incompatible scheduler request: %s",
		errors.RFCCodeText("DFLOW:ErrIncompatibleSchedulerRequest"),
	)
	ErrDispatchTaskRequestIDNotFound = errors.Normalize(
		"dispatch task request id %s not found",
		errors.RFCCodeText("DFLOW:ErrDispatchTaskRequestIDNotFound"),
	)
	ErrElectionRecordConflict = errors.Normalize(
		"election record conflict",
		errors.RFCCodeText("DFLOW:ErrElectionRecordConflict"),
	)
	ErrDeserializeConfig = errors.Normalize(
		"deserialize config failed",
		errors.RFCCodeText("DFLOW:ErrDeserializeConfig"),
	)
)
