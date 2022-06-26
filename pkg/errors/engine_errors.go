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
	// This happens when a unknown executor send requests to master.
	ErrUnknownExecutorID = errors.Normalize(
		"cannot find executor ID: %s",
		errors.RFCCodeText("DFLOW:ErrUnknownExecutorID"),
	)
	ErrTombstoneExecutor = errors.Normalize(
		"executor %s has been dead",
		errors.RFCCodeText("DFLOW:ErrTombstoneExecutor"),
	)
	ErrSubJobFailed = errors.Normalize(
		"executor %s job %d",
		errors.RFCCodeText("DFLOW:ErrSubJobFailed"),
	)
	ErrClusterResourceNotEnough = errors.Normalize(
		"cluster resource is not enough, please scale out the cluster",
		errors.RFCCodeText("DFLOW:ErrClusterResourceNotEnough"),
	)
	ErrBuildJobFailed = errors.Normalize(
		"build job failed",
		errors.RFCCodeText("DFLOW:ErrBuildJobFailed"),
	)

	ErrExecutorDupRegister = errors.Normalize(
		"executor %s has been registered",
		errors.RFCCodeText("DFLOW:ErrExecutorDupRegister"),
	)
	ErrGrpcBuildConn = errors.Normalize(
		"dial grpc connection to %s failed",
		errors.RFCCodeText("DFLOW:ErrGrpcBuildConn"),
	)
	ErrDecodeEtcdKeyFail = errors.Normalize(
		"failed to decode etcd key: %s",
		errors.RFCCodeText("DFLOW:ErrDecodeEtcdKeyFail"),
	)
	ErrDecodeEtcdValueFail = errors.Normalize(
		"failed to decode etcd value: %s",
		errors.RFCCodeText("DFLOW:ErrDecodeEtcdValueFail"),
	)
	ErrInvalidMetaStoreKey = errors.Normalize(
		"invalid metastore key %s",
		errors.RFCCodeText("DFLOW:ErrInvalidMetaStoreKey"),
	)
	ErrInvalidMetaStoreKeyTp = errors.Normalize(
		"invalid metastore key type %s",
		errors.RFCCodeText("DFLOW:ErrInvalidMetaStoreKeyTp"),
	)
	ErrEtcdAPIError = errors.Normalize(
		"etcd api returns error",
		errors.RFCCodeText("DFLOW:ErrEtcdAPIError"),
	)
	ErrNoRPCClient = errors.Normalize(
		"no available RPC client",
		errors.RFCCodeText("DFLOW:ErrNoRPCClient"),
	)

	// master related errors
	ErrMasterConfigParseFlagSet = errors.Normalize(
		"parse config flag set failed",
		errors.RFCCodeText("DFLOW:ErrMasterConfigParseFlagSet"),
	)
	ErrMasterConfigInvalidFlag = errors.Normalize(
		"'%s' is an invalid flag",
		errors.RFCCodeText("DFLOW:ErrMasterConfigInvalidFlag"),
	)
	ErrMasterDecodeConfigFile = errors.Normalize(
		"decode config file failed",
		errors.RFCCodeText("DFLOW:ErrMasterDecodeConfigFile"),
	)
	ErrMasterConfigUnknownItem = errors.Normalize(
		"master config contains unknown configuration options: %s",
		errors.RFCCodeText("DFLOW:ErrMasterConfigUnknownItem"),
	)
	ErrMasterGenEmbedEtcdConfigFail = errors.Normalize(
		"master gen embed etcd config failed: %s",
		errors.RFCCodeText("DFLOW:ErrMasterGenEmbedEtcdConfigFail"),
	)
	ErrMasterJoinEmbedEtcdFail = errors.Normalize(
		"failed to join embed etcd: %s",
		errors.RFCCodeText("DFLOW:ErrMasterJoinEmbedEtcdFail"),
	)
	ErrMasterStartEmbedEtcdFail = errors.Normalize(
		"failed to start embed etcd",
		errors.RFCCodeText("DFLOW:ErrMasterStartEmbedEtcdFail"),
	)
	ErrMasterParseURLFail = errors.Normalize(
		"failed to parse URL %s",
		errors.RFCCodeText("DFLOW:ErrMasterParseURLFail"),
	)
	ErrMasterScheduleMissTask = errors.Normalize(
		"task %d is not found after scheduling",
		errors.RFCCodeText("DFLOW:ErrMasterScheduleMissTask"),
	)
	ErrMasterNewServer = errors.Normalize(
		"master create new server failed",
		errors.RFCCodeText("DFLOW:ErrMasterNewServer"),
	)
	ErrMasterCampaignLeader = errors.Normalize(
		"master campaign to be leader failed",
		errors.RFCCodeText("DFLOW:ErrMasterCampaignLeader"),
	)
	ErrMasterSessionDone = errors.Normalize(
		"master session is done",
		errors.RFCCodeText("DFLOW:ErrMasterSessionDone"),
	)
	ErrMasterRPCNotForward = errors.Normalize(
		"server grpc is not forwarded to leader",
		errors.RFCCodeText("DFLOW:ErrMasterRPCNotForward"),
	)
	ErrLeaderCtxCanceled = errors.Normalize(
		"leader context is canceled",
		errors.RFCCodeText("DFLOW:ErrLeaderCtxCanceled"),
	)
	ErrMessageClientNotFoundForWorker = errors.Normalize(
		"peer message client is not found for worker: worker ID %s",
		errors.RFCCodeText("DFLOW:ErrMessageClientNotFoundForWorker"),
	)
	ErrMasterNotFound = errors.Normalize(
		"master is not found: master ID %s",
		errors.RFCCodeText("DFLOW:ErrMasterNotFound"),
	)
	ErrDuplicateWorkerID = errors.Normalize(
		"duplicate worker ID encountered: %s, report a bug",
		errors.RFCCodeText("DFLOW:ErrDuplicateWorkerID"),
	)
	ErrMasterClosed = errors.Normalize(
		"master has been closed explicitly: master ID %s",
		errors.RFCCodeText("DFLOW:ErrMasterClosed"),
	)
	ErrMasterConcurrencyExceeded = errors.Normalize(
		"master has reached concurrency quota",
		errors.RFCCodeText("DFLOW:ErrMasterConcurrencyExceeded"),
	)
	ErrMasterInvalidMeta = errors.Normalize(
		"invalid master meta data: %s",
		errors.RFCCodeText("DFLOW:ErrMasterInvalidMeta"),
	)
	ErrInvalidServerMasterID = errors.Normalize(
		"invalid server master id: %s",
		errors.RFCCodeText("DFLOW:ErrInvalidServerMasterID"),
	)
	ErrInvalidMasterMessage = errors.Normalize(
		"invalid master message: %s",
		errors.RFCCodeText("DFLOW:ErrInvalidMasterMessage"),
	)
	ErrSendingMessageToTombstone = errors.Normalize(
		"trying to send message to a tombstone worker handle: %s",
		errors.RFCCodeText("DFLOW:ErrSendingMessageToTombstone"),
	)
	ErrMasterNotInitialized = errors.Normalize(
		"master is not initialized",
		errors.RFCCodeText("DFLOW:ErrMasterNotInitialized"),
	)
	ErrMasterInterfaceNotFound = errors.Normalize(
		"basemaster interface not found",
		errors.RFCCodeText("DFLOW:ErrMasterInterfaceNotFound"),
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
	ErrWorkerTimedOut = errors.Normalize(
		"worker heartbeat timed out: workerID %s",
		errors.RFCCodeText("DFLOW:ErrWorkerTimedOut"),
	)
	ErrWorkerSuicide = errors.Normalize(
		"worker has committed suicide due to master(%s) having timed out",
		errors.RFCCodeText("DFLOW:ErrWorkerSuicide"),
	)
	ErrWorkerNoMeta = errors.Normalize(
		"worker metadata does not exist",
		errors.RFCCodeText("DFLOW:ErrWorkerNoMeta"),
	)
	ErrWorkerUpdateStatusTryAgain = errors.Normalize(
		"worker should try again in updating the status",
		errors.RFCCodeText("DFLOW:ErrWorkerUpdateStatusTryAgain"),
	)
	ErrInvalidJobType = errors.Normalize(
		"invalid job type: %s",
		errors.RFCCodeText("DFLOW:ErrInvalidJobType"),
	)
	ErrWorkerFinish = errors.Normalize(
		"worker finished and exited",
		errors.RFCCodeText("DFLOW:ErrWorkerFinish"),
	)
	ErrWorkerStop = errors.Normalize(
		"worker is stopped",
		errors.RFCCodeText("DFLOW:ErrWorkerStop"),
	)
	ErrTooManyStatusUpdates = errors.Normalize(
		"there are too many pending worker status updates: %d",
		errors.RFCCodeText("DFLOW:ErrTooManyStatusUpdates"),
	)
	ErrWorkerHalfExit = errors.Normalize(
		"the worker is in half-exited state",
		errors.RFCCodeText("DFLOW:ErrWorkerHalfExit"),
	)
	ErrInvalidWorkerType = errors.Normalize(
		"invalid worker type: %s",
		errors.RFCCodeText("DFLOW:ErrInvalidWorkerType"),
	)

	// master etcd related errors
	ErrMasterEtcdCreateSessionFail = errors.Normalize(
		"failed to create Etcd session",
		errors.RFCCodeText("DFLOW:ErrMasterEtcdCreateSessionFail"),
	)
	ErrMasterEtcdElectionCampaignFail = errors.Normalize(
		"failed to campaign for leader",
		errors.RFCCodeText("DFLOW:ErrMasterEtcdElectionCampaignFail"),
	)
	ErrMasterNoLeader = errors.Normalize(
		"server master has no leader",
		errors.RFCCodeText("DFLOW:ErrMasterNoLeader"),
	)
	ErrEtcdLeaderChanged = errors.Normalize(
		"etcd leader has changed",
		errors.RFCCodeText("DFLOW:ErrEtcdLeaderChanged"),
	)
	ErrDiscoveryDuplicateWatch = errors.Normalize(
		"service discovery can't be watched multiple times",
		errors.RFCCodeText("DFLOW:ErrDiscoveryDuplicateWatch"),
	)
	ErrMasterEtcdEpochFail = errors.Normalize(
		"server master generate epoch fail",
		errors.RFCCodeText("DFLOW:ErrMasterEtcdEpochFail"),
	)

	// executor related errors
	ErrExecutorConfigParseFlagSet = errors.Normalize(
		"parse config flag set failed",
		errors.RFCCodeText("DFLOW:ErrExecutorConfigParseFlagSet"),
	)
	ErrExecutorConfigInvalidFlag = errors.Normalize(
		"'%s' is an invalid flag",
		errors.RFCCodeText("DFLOW:ErrExecutorConfigInvalidFlag"),
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
	ErrTaskNotFound = errors.Normalize(
		"task %d is not found",
		errors.RFCCodeText("DFLOW:ErrTaskNotFound"),
	)
	ErrExecutorUnknownOperator = errors.Normalize(
		"operator type %d is unknown",
		errors.RFCCodeText("DFLOW:ErrOperatorUnknown"),
	)
	ErrExecutorSessionDone = errors.Normalize(
		"executor %s session done",
		errors.RFCCodeText("DFLOW:ErrExecutorSessionDone"),
	)
	ErrRuntimeIncomingQueueFull = errors.Normalize(
		"runtime has too many pending CreateWorker requests",
		errors.RFCCodeText("DFLOW:ErrRuntimeIncomingQueueFull"),
	)
	ErrRuntimeReachedCapacity = errors.Normalize(
		"runtime has reached its capacity %d",
		errors.RFCCodeText("DFLOW:ErrRuntimeReachedCapacity"),
	)
	ErrRuntimeIsClosed = errors.Normalize(
		"runtime has been closed",
		errors.RFCCodeText("DFLOW:ErrRuntimeIsClosed"),
	)
	ErrRuntimeInitQueuingTimeOut = errors.Normalize(
		"a task has waited too long to be initialized",
		errors.RFCCodeText("DFLOW:ErrRuntimeInitQueuingTimeOut"),
	)
	ErrRuntimeDuplicateTaskID = errors.Normalize(
		"trying to add a task with the same ID as an existing one",
		errors.RFCCodeText("DFLOW:ErrRuntimeDuplicateTaskID %s"),
	)
	ErrRuntimeClosed = errors.Normalize(
		"runtime has been closed",
		errors.RFCCodeText("DFLOW:ErrRuntimeClosed"),
	)
	ErrExecutorEtcdConnFail = errors.Normalize(
		"executor conn inner etcd fail",
		errors.RFCCodeText("DFLOW:ErrExecutorEtcdConnFail"),
	)
	ErrExecutorNotFoundForMessage = errors.Normalize(
		"cannot find the executor for p2p messaging",
		errors.RFCCodeText("DFLOW:ErrExecutorNotFoundForMessage"),
	)
	ErrMasterTooManyPendingEvents = errors.Normalize(
		"master has too many pending events",
		errors.RFCCodeText("DFLOW:ErrMasterTooManyPendingEvents"),
	)

	// Two-Phase Task Dispatching errors
	ErrExecutorPreDispatchFailed = errors.Normalize(
		"PreDispatchTask failed",
		errors.RFCCodeText("DFLOW:ErrExecutorPreDispatchFailed"),
	)
	ErrExecutorConfirmDispatchFailed = errors.Normalize(
		"ConfirmDispatch failed",
		errors.RFCCodeText("DFLOW:ErrExecutorConfirmDispatchFailed"),
	)

	// planner related errors
	ErrPlannerDAGDepthExceeded = errors.Normalize(
		"dag depth exceeded: %d",
		errors.RFCCodeText("DFLOW:ErrPlannerDAGDepthExceeded"),
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
	ErrMetaRevisionUnmatch = errors.Normalize(
		"meta revision unmatch",
		errors.RFCCodeText("DFLOW:ErrMetaRevisionUnmatch"),
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
	ErrMetaStoreUnfounded = errors.Normalize(
		"metastore unfounded:%s",
		errors.RFCCodeText("DFLOW:ErrMetaStoreUnfounded"),
	)
	ErrMetaEntryNotFound = errors.Normalize(
		"meta entry not found",
		errors.RFCCodeText("DFLOW:ErrMetaEntryNotFound"),
	)
	ErrMetaParamsInvalid = errors.Normalize(
		"meta params invalid:%s",
		errors.RFCCodeText("DFLOW:ErrMetaParamsInvalid"),
	)
	ErrMetaEntryAlreadyExists = errors.Normalize(
		"meta entry already exists",
		errors.RFCCodeText("DFLOW:ErrMetaEntryAlreadyExists"),
	)

	// DataSet errors
	ErrDatasetEntryNotFound = errors.Normalize(
		"dataset entry not found. Key: %s",
		errors.RFCCodeText("DFLOW:ErrDatasetEntryNotFound"),
	)

	// Resource related errors
	ErrUnexpectedResourcePath = errors.Normalize(
		"unexpected resource path: %s",
		errors.RFCCodeText("DFLOW:ErrUnexpectedResourcePath"),
	)
	ErrDuplicateResourceID = errors.Normalize(
		"duplicate resource ID: %s",
		errors.RFCCodeText("DFLOW:ErrDuplicateResourceID"),
	)
	ErrIllegalResourcePath = errors.Normalize(
		"resource path is illegal: %s",
		errors.RFCCodeText("DFLOW:ErrIllegalResourcePath"),
	)
	ErrResourceDoesNotExist = errors.Normalize(
		"resource does not exists: %s",
		errors.RFCCodeText("DFLOW:ErrResourceDoesNotExist"),
	)
	ErrResourceManagerNotReady = errors.Normalize(
		"resource manager is not ready",
		errors.RFCCodeText("DLFOW:ErrResourceManagerNotReady"),
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
)
