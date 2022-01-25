// Copyright 2020 PingCAP, Inc.
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

// errors
var (
	// kv related errors
	ErrWriteTsConflict         = errors.Normalize("write ts conflict", errors.RFCCodeText("CDC:ErrWriteTsConflict"))
	ErrChangeFeedNotExists     = errors.Normalize("changefeed not exists, key: %s", errors.RFCCodeText("CDC:ErrChangeFeedNotExists"))
	ErrChangeFeedAlreadyExists = errors.Normalize("changefeed already exists, key: %s", errors.RFCCodeText("CDC:ErrChangeFeedAlreadyExists"))
	ErrTaskStatusNotExists     = errors.Normalize("task status not exists, key: %s", errors.RFCCodeText("CDC:ErrTaskStatusNotExists"))
	ErrTaskPositionNotExists   = errors.Normalize("task position not exists, key: %s", errors.RFCCodeText("CDC:ErrTaskPositionNotExists"))
	ErrCaptureNotExist         = errors.Normalize("capture not exists, key: %s", errors.RFCCodeText("CDC:ErrCaptureNotExist"))
	ErrGetAllStoresFailed      = errors.Normalize("get stores from pd failed", errors.RFCCodeText("CDC:ErrGetAllStoresFailed"))
	ErrMetaListDatabases       = errors.Normalize("meta store list databases", errors.RFCCodeText("CDC:ErrMetaListDatabases"))
	ErrGRPCDialFailed          = errors.Normalize("grpc dial failed", errors.RFCCodeText("CDC:ErrGRPCDialFailed"))
	ErrTiKVEventFeed           = errors.Normalize("tikv event feed failed", errors.RFCCodeText("CDC:ErrTiKVEventFeed"))
	ErrPDBatchLoadRegions      = errors.Normalize("pd batch load regions failed", errors.RFCCodeText("CDC:ErrPDBatchLoadRegions"))
	ErrMetaNotInRegion         = errors.Normalize("meta not exists in region", errors.RFCCodeText("CDC:ErrMetaNotInRegion"))
	ErrRegionsNotCoverSpan     = errors.Normalize("regions not completely left cover span, span %v regions: %v", errors.RFCCodeText("CDC:ErrRegionsNotCoverSpan"))
	ErrGetTiKVRPCContext       = errors.Normalize("get tikv grpc context failed", errors.RFCCodeText("CDC:ErrGetTiKVRPCContext"))
	ErrPendingRegionCancel     = errors.Normalize("pending region cancelled due to stream disconnecting", errors.RFCCodeText("CDC:ErrPendingRegionCancel"))
	ErrEventFeedAborted        = errors.Normalize("single event feed aborted", errors.RFCCodeText("CDC:ErrEventFeedAborted"))
	ErrUnknownKVEventType      = errors.Normalize("unknown kv optype: %s, entry: %v", errors.RFCCodeText("CDC:ErrUnknownKVEventType"))
	ErrNoPendingRegion         = errors.Normalize("received event regionID %v, requestID %v from %v,"+
		" but neither pending region nor running region was found", errors.RFCCodeText("CDC:ErrNoPendingRegion"))
	ErrPrewriteNotMatch       = errors.Normalize("prewrite not match, key: %s, start-ts: %d, commit-ts: %d, type: %s, optype: %s", errors.RFCCodeText("CDC:ErrPrewriteNotMatch"))
	ErrGetRegionFailed        = errors.Normalize("get region failed", errors.RFCCodeText("CDC:ErrGetRegionFailed"))
	ErrScanLockFailed         = errors.Normalize("scan lock failed", errors.RFCCodeText("CDC:ErrScanLockFailed"))
	ErrResolveLocks           = errors.Normalize("resolve locks failed", errors.RFCCodeText("CDC:ErrResolveLocks"))
	ErrLocateRegion           = errors.Normalize("locate region by id", errors.RFCCodeText("CDC:ErrLocateRegion"))
	ErrKVStorageSendReq       = errors.Normalize("send req to kv storage", errors.RFCCodeText("CDC:ErrKVStorageSendReq"))
	ErrKVStorageRegionError   = errors.Normalize("req with region error", errors.RFCCodeText("CDC:ErrKVStorageRegionError"))
	ErrKVStorageBackoffFailed = errors.Normalize("backoff failed", errors.RFCCodeText("CDC:ErrKVStorageBackoffFailed"))
	ErrKVStorageRespEmpty     = errors.Normalize("tikv response body missing", errors.RFCCodeText("CDC:ErrKVStorageRespEmpty"))
	ErrEventFeedEventError    = errors.Normalize("eventfeed returns event error", errors.RFCCodeText("CDC:ErrEventFeedEventError"))
	ErrPDEtcdAPIError         = errors.Normalize("etcd api call error", errors.RFCCodeText("CDC:ErrPDEtcdAPIError"))
	ErrCachedTSONotExists     = errors.Normalize("GetCachedCurrentVersion: cache entry does not exist", errors.RFCCodeText("CDC:ErrCachedTSONotExists"))
	ErrGetStoreSnapshot       = errors.Normalize("get snapshot failed", errors.RFCCodeText("CDC:ErrGetStoreSnapshot"))
	ErrNewStore               = errors.Normalize("new store failed", errors.RFCCodeText("CDC:ErrNewStore"))
	ErrRegionWorkerExit       = errors.Normalize("region worker exited", errors.RFCCodeText("CDC:ErrRegionWorkerExit"))

	// rule related errors
	ErrEncodeFailed      = errors.Normalize("encode failed: %s", errors.RFCCodeText("CDC:ErrEncodeFailed"))
	ErrDecodeFailed      = errors.Normalize("decode failed: %s", errors.RFCCodeText("CDC:ErrDecodeFailed"))
	ErrFilterRuleInvalid = errors.Normalize("filter rule is invalid", errors.RFCCodeText("CDC:ErrFilterRuleInvalid"))

	// internal errors
	ErrAdminStopProcessor = errors.Normalize("stop processor by admin command", errors.RFCCodeText("CDC:ErrAdminStopProcessor"))
	// ErrVersionIncompatible is an error for running CDC on an incompatible Cluster.
	ErrVersionIncompatible   = errors.Normalize("version is incompatible: %s", errors.RFCCodeText("CDC:ErrVersionIncompatible"))
	ErrClusterIDMismatch     = errors.Normalize("cluster ID mismatch, tikv cluster ID is %d and request cluster ID is %d", errors.RFCCodeText("CDC:ErrClusterIDMismatch"))
	ErrCreateMarkTableFailed = errors.Normalize("create mark table failed", errors.RFCCodeText("CDC:ErrCreateMarkTableFailed"))

	// sink related errors
	ErrExecDDLFailed            = errors.Normalize("exec DDL failed", errors.RFCCodeText("CDC:ErrExecDDLFailed"))
	ErrDDLEventIgnored          = errors.Normalize("ddl event is ignored", errors.RFCCodeText("CDC:ErrDDLEventIgnored"))
	ErrKafkaSendMessage         = errors.Normalize("kafka send message failed", errors.RFCCodeText("CDC:ErrKafkaSendMessage"))
	ErrKafkaAsyncSendMessage    = errors.Normalize("kafka async send message failed", errors.RFCCodeText("CDC:ErrKafkaAsyncSendMessage"))
	ErrKafkaFlushUnfinished     = errors.Normalize("flush not finished before producer close", errors.RFCCodeText("CDC:ErrKafkaFlushUnfinished"))
	ErrKafkaInvalidPartitionNum = errors.Normalize("invalid partition num %d", errors.RFCCodeText("CDC:ErrKafkaInvalidPartitionNum"))
	ErrKafkaNewSaramaProducer   = errors.Normalize("new sarama producer", errors.RFCCodeText("CDC:ErrKafkaNewSaramaProducer"))
	ErrKafkaInvalidClientID     = errors.Normalize("invalid kafka client ID '%s'", errors.RFCCodeText("CDC:ErrKafkaInvalidClientID"))
	ErrKafkaInvalidVersion      = errors.Normalize("invalid kafka version", errors.RFCCodeText("CDC:ErrKafkaInvalidVersion"))
	ErrKafkaInvalidConfig       = errors.Normalize("kafka config invalid", errors.RFCCodeText("CDC:ErrKafkaInvalidConfig"))
	ErrPulsarNewProducer        = errors.Normalize("new pulsar producer", errors.RFCCodeText("CDC:ErrPulsarNewProducer"))
	ErrPulsarSendMessage        = errors.Normalize("pulsar send message failed", errors.RFCCodeText("CDC:ErrPulsarSendMessage"))
	ErrRedoConfigInvalid        = errors.Normalize("redo log config invalid", errors.RFCCodeText("CDC:ErrRedoConfigInvalid"))
	ErrRedoDownloadFailed       = errors.Normalize("redo log down load to local failed", errors.RFCCodeText("CDC:ErrRedoDownloadFailed"))
	ErrRedoWriterStopped        = errors.Normalize("redo log writer stopped", errors.RFCCodeText("CDC:ErrRedoWriterStopped"))
	ErrRedoFileOp               = errors.Normalize("redo file operation", errors.RFCCodeText("CDC:ErrRedoFileOp"))
	ErrRedoMetaFileNotFound     = errors.Normalize("no redo meta file found in dir: %s", errors.RFCCodeText("CDC:ErrRedoMetaFileNotFound"))
	ErrRedoMetaInitialize       = errors.Normalize("initialize meta for redo log", errors.RFCCodeText("CDC:ErrRedoMetaInitialize"))
	ErrFileSizeExceed           = errors.Normalize("rawData size %d exceeds maximum file size %d", errors.RFCCodeText("CDC:ErrFileSizeExceed"))
	ErrS3StorageAPI             = errors.Normalize("s3 storage api", errors.RFCCodeText("CDC:ErrS3StorageAPI"))
	ErrS3StorageInitialize      = errors.Normalize("new s3 storage for redo log", errors.RFCCodeText("CDC:ErrS3StorageInitialize"))
	ErrPrepareAvroFailed        = errors.Normalize("prepare avro failed", errors.RFCCodeText("CDC:ErrPrepareAvroFailed"))
	ErrAsyncBroadcastNotSupport = errors.Normalize("Async broadcasts not supported", errors.RFCCodeText("CDC:ErrAsyncBroadcastNotSupport"))
	ErrSinkURIInvalid           = errors.Normalize("sink uri invalid", errors.RFCCodeText("CDC:ErrSinkURIInvalid"))
	ErrMQSinkUnknownProtocol    = errors.Normalize("unknown '%s' protocol for Message Queue sink", errors.RFCCodeText("CDC:ErrMQSinkUnknownProtocol"))
	ErrMySQLTxnError            = errors.Normalize("MySQL txn error", errors.RFCCodeText("CDC:ErrMySQLTxnError"))
	ErrMySQLQueryError          = errors.Normalize("MySQL query error", errors.RFCCodeText("CDC:ErrMySQLQueryError"))
	ErrMySQLConnectionError     = errors.Normalize("MySQL connection error", errors.RFCCodeText("CDC:ErrMySQLConnectionError"))
	ErrMySQLInvalidConfig       = errors.Normalize("MySQL config invalid", errors.RFCCodeText("CDC:ErrMySQLInvalidConfig"))
	ErrMySQLWorkerPanic         = errors.Normalize("MySQL worker panic", errors.RFCCodeText("CDC:ErrMySQLWorkerPanic"))
	ErrAvroToEnvelopeError      = errors.Normalize("to envelope failed", errors.RFCCodeText("CDC:ErrAvroToEnvelopeError"))
	ErrAvroUnknownType          = errors.Normalize("unknown type for Avro: %v", errors.RFCCodeText("CDC:ErrAvroUnknownType"))
	ErrAvroMarshalFailed        = errors.Normalize("json marshal failed", errors.RFCCodeText("CDC:ErrAvroMarshalFailed"))
	ErrAvroEncodeFailed         = errors.Normalize("encode to avro native data", errors.RFCCodeText("CDC:ErrAvroEncodeFailed"))
	ErrAvroEncodeToBinary       = errors.Normalize("encode to binray from native", errors.RFCCodeText("CDC:ErrAvroEncodeToBinary"))
	ErrAvroSchemaAPIError       = errors.Normalize("schema manager API error", errors.RFCCodeText("CDC:ErrAvroSchemaAPIError"))
	ErrMaxwellEncodeFailed      = errors.Normalize("maxwell encode failed", errors.RFCCodeText("CDC:ErrMaxwellEncodeFailed"))
	ErrMaxwellDecodeFailed      = errors.Normalize("maxwell decode failed", errors.RFCCodeText("CDC:ErrMaxwellDecodeFailed"))
	ErrMaxwellInvalidData       = errors.Normalize("maxwell invalid data", errors.RFCCodeText("CDC:ErrMaxwellInvalidData"))
	ErrJSONCodecInvalidData     = errors.Normalize("json codec invalid data", errors.RFCCodeText("CDC:ErrJSONCodecInvalidData"))
	ErrJSONCodecRowTooLarge     = errors.Normalize("json codec single row too large", errors.RFCCodeText("CDC:ErrJSONCodecRowTooLarge"))
	ErrCanalDecodeFailed        = errors.Normalize("canal decode failed", errors.RFCCodeText("CDC:ErrCanalDecodeFailed"))
	ErrCanalEncodeFailed        = errors.Normalize("canal encode failed", errors.RFCCodeText("CDC:ErrCanalEncodeFailed"))
	ErrOldValueNotEnabled       = errors.Normalize("old value is not enabled", errors.RFCCodeText("CDC:ErrOldValueNotEnabled"))
	ErrSinkInvalidConfig        = errors.Normalize("sink config invalid", errors.RFCCodeText("CDC:ErrSinkInvalidConfig"))
	ErrCraftCodecInvalidData    = errors.Normalize("craft codec invalid data", errors.RFCCodeText("CDC:ErrCraftCodecInvalidData"))

	// utilities related errors
	ErrToTLSConfigFailed         = errors.Normalize("generate tls config failed", errors.RFCCodeText("CDC:ErrToTLSConfigFailed"))
	ErrCheckClusterVersionFromPD = errors.Normalize("failed to request PD %s, please try again later", errors.RFCCodeText("CDC:ErrCheckClusterVersionFromPD"))
	ErrNewSemVersion             = errors.Normalize("create sem version", errors.RFCCodeText("CDC:ErrNewSemVersion"))
	ErrCheckDirWritable          = errors.Normalize("check dir writable failed", errors.RFCCodeText("CDC:ErrCheckDirWritable"))
	ErrCheckDirReadable          = errors.Normalize("check dir readable failed", errors.RFCCodeText("CDC:ErrCheckDirReadable"))
	ErrCheckDirValid             = errors.Normalize("check dir valid failed", errors.RFCCodeText("CDC:ErrCheckDirValid"))
	ErrGetDiskInfo               = errors.Normalize("get dir disk info failed", errors.RFCCodeText("CDC:ErrGetDiskInfo"))
	ErrLoadTimezone              = errors.Normalize("load timezone", errors.RFCCodeText("CDC:ErrLoadTimezone"))
	ErrURLFormatInvalid          = errors.Normalize("url format is invalid", errors.RFCCodeText("CDC:ErrURLFormatInvalid"))
	ErrIntersectNoOverlap        = errors.Normalize("span doesn't overlap: %+v vs %+v", errors.RFCCodeText("CDC:ErrIntersectNoOverlap"))
	ErrOperateOnClosedNotifier   = errors.Normalize("operate on a closed notifier", errors.RFCCodeText("CDC:ErrOperateOnClosedNotifier"))

	// encode/decode, data format and data integrity errors
	ErrInvalidRecordKey      = errors.Normalize("invalid record key - %q", errors.RFCCodeText("CDC:ErrInvalidRecordKey"))
	ErrCodecDecode           = errors.Normalize("codec decode error", errors.RFCCodeText("CDC:ErrCodecDecode"))
	ErrUnknownMetaType       = errors.Normalize("unknown meta type %v", errors.RFCCodeText("CDC:ErrUnknownMetaType"))
	ErrFetchHandleValue      = errors.Normalize("can't find handle column, please check if the pk is handle", errors.RFCCodeText("CDC:ErrFetchHandleValue"))
	ErrDatumUnflatten        = errors.Normalize("unflatten datume data", errors.RFCCodeText("CDC:ErrDatumUnflatten"))
	ErrWrongTableInfo        = errors.Normalize("wrong table info in unflatten, table id %d, index table id: %d", errors.RFCCodeText("CDC:ErrWrongTableInfo"))
	ErrIndexKeyTableNotFound = errors.Normalize("table not found with index ID %d in index kv", errors.RFCCodeText("CDC:ErrIndexKeyTableNotFound"))
	ErrDecodeRowToDatum      = errors.Normalize("decode row data to datum failed", errors.RFCCodeText("CDC:ErrDecodeRowToDatum"))
	ErrMarshalFailed         = errors.Normalize("marshal failed", errors.RFCCodeText("CDC:ErrMarshalFailed"))
	ErrUnmarshalFailed       = errors.Normalize("unmarshal failed", errors.RFCCodeText("CDC:ErrUnmarshalFailed"))
	ErrInvalidChangefeedID   = errors.Normalize(`bad changefeed id, please match the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$, the length should no more than %d", eg, "simple-changefeed-task"`, errors.RFCCodeText("CDC:ErrInvalidChangefeedID"))
	ErrInvalidEtcdKey        = errors.Normalize("invalid key: %s", errors.RFCCodeText("CDC:ErrInvalidEtcdKey"))

	// schema storage errors
	ErrSchemaStorageUnresolved = errors.Normalize("can not found schema snapshot, the specified ts(%d) is more than resolvedTs(%d)", errors.RFCCodeText("CDC:ErrSchemaStorageUnresolved"))
	ErrSchemaStorageGCed       = errors.Normalize("can not found schema snapshot, the specified ts(%d) is less than gcTS(%d)", errors.RFCCodeText("CDC:ErrSchemaStorageGCed"))
	ErrSchemaSnapshotNotFound  = errors.Normalize("can not found schema snapshot, ts: %d", errors.RFCCodeText("CDC:ErrSchemaSnapshotNotFound"))
	ErrSchemaStorageTableMiss  = errors.Normalize("table %d not found", errors.RFCCodeText("CDC:ErrSchemaStorageTableMiss"))
	ErrSnapshotSchemaNotFound  = errors.Normalize("schema %d not found in schema snapshot", errors.RFCCodeText("CDC:ErrSnapshotSchemaNotFound"))
	ErrSnapshotTableNotFound   = errors.Normalize("table %d not found in schema snapshot", errors.RFCCodeText("CDC:ErrSnapshotTableNotFound"))
	ErrSnapshotSchemaExists    = errors.Normalize("schema %s(%d) already exists", errors.RFCCodeText("CDC:ErrSnapshotSchemaExists"))
	ErrSnapshotTableExists     = errors.Normalize("table %s.%s already exists", errors.RFCCodeText("CDC:ErrSnapshotTableExists"))
	ErrInvalidDDLJob           = errors.Normalize("invalid ddl job(%d)", errors.RFCCodeText("CDC:ErrInvalidDDLJob"))

	// puller related errors
	ErrBufferReachLimit = errors.Normalize("puller mem buffer reach size limit", errors.RFCCodeText("CDC:ErrBufferReachLimit"))

	// server related errors
	ErrCaptureSuicide               = errors.Normalize("capture suicide", errors.RFCCodeText("CDC:ErrCaptureSuicide"))
	ErrNewCaptureFailed             = errors.Normalize("new capture failed", errors.RFCCodeText("CDC:ErrNewCaptureFailed"))
	ErrCaptureRegister              = errors.Normalize("capture register to etcd failed", errors.RFCCodeText("CDC:ErrCaptureRegister"))
	ErrNewProcessorFailed           = errors.Normalize("new processor failed", errors.RFCCodeText("CDC:ErrNewProcessorFailed"))
	ErrProcessorUnknown             = errors.Normalize("processor running unknown error", errors.RFCCodeText("CDC:ErrProcessorUnknown"))
	ErrOwnerUnknown                 = errors.Normalize("owner running unknown error", errors.RFCCodeText("CDC:ErrOwnerUnknown"))
	ErrProcessorTableNotFound       = errors.Normalize("table not found in processor cache", errors.RFCCodeText("CDC:ErrProcessorTableNotFound"))
	ErrProcessorEtcdWatch           = errors.Normalize("etcd watch returns error", errors.RFCCodeText("CDC:ErrProcessorEtcdWatch"))
	ErrProcessorSortDir             = errors.Normalize("sort dir error", errors.RFCCodeText("CDC:ErrProcessorSortDir"))
	ErrUnknownSortEngine            = errors.Normalize("unknown sort engine %s", errors.RFCCodeText("CDC:ErrUnknownSortEngine"))
	ErrInvalidTaskKey               = errors.Normalize("invalid task key: %s", errors.RFCCodeText("CDC:ErrInvalidTaskKey"))
	ErrInvalidServerOption          = errors.Normalize("invalid server option", errors.RFCCodeText("CDC:ErrInvalidServerOption"))
	ErrServerNewPDClient            = errors.Normalize("server creates pd client failed", errors.RFCCodeText("CDC:ErrServerNewPDClient"))
	ErrServeHTTP                    = errors.Normalize("serve http error", errors.RFCCodeText("CDC:ErrServeHTTP"))
	ErrCaptureCampaignOwner         = errors.Normalize("campaign owner failed", errors.RFCCodeText("CDC:ErrCaptureCampaignOwner"))
	ErrCaptureResignOwner           = errors.Normalize("resign owner failed", errors.RFCCodeText("CDC:ErrCaptureResignOwner"))
	ErrWaitHandleOperationTimeout   = errors.Normalize("waiting processor to handle the operation finished timeout", errors.RFCCodeText("CDC:ErrWaitHandleOperationTimeout"))
	ErrSupportPostOnly              = errors.Normalize("this api supports POST method only", errors.RFCCodeText("CDC:ErrSupportPostOnly"))
	ErrSupportGetOnly               = errors.Normalize("this api supports GET method only", errors.RFCCodeText("CDC:ErrSupportGetOnly"))
	ErrAPIInvalidParam              = errors.Normalize("invalid api parameter", errors.RFCCodeText("CDC:ErrAPIInvalidParam"))
	ErrRequestForwardErr            = errors.Normalize("request forward error, an request can only forward to owner one time ", errors.RFCCodeText("ErrRequestForwardErr"))
	ErrInternalServerError          = errors.Normalize("internal server error", errors.RFCCodeText("CDC:ErrInternalServerError"))
	ErrOwnerSortDir                 = errors.Normalize("owner sort dir", errors.RFCCodeText("CDC:ErrOwnerSortDir"))
	ErrOwnerChangefeedNotFound      = errors.Normalize("changefeed %s not found in owner cache", errors.RFCCodeText("CDC:ErrOwnerChangefeedNotFound"))
	ErrChangefeedUpdateRefused      = errors.Normalize("changefeed update error: %s", errors.RFCCodeText("CDC:ErrChangefeedUpdateRefused"))
	ErrChangefeedAbnormalState      = errors.Normalize("changefeed in abnormal state: %s, replication status: %+v", errors.RFCCodeText("CDC:ErrChangefeedAbnormalState"))
	ErrInvalidAdminJobType          = errors.Normalize("invalid admin job type: %d", errors.RFCCodeText("CDC:ErrInvalidAdminJobType"))
	ErrOwnerEtcdWatch               = errors.Normalize("etcd watch returns error", errors.RFCCodeText("CDC:ErrOwnerEtcdWatch"))
	ErrOwnerCampaignKeyDeleted      = errors.Normalize("owner campaign key deleted", errors.RFCCodeText("CDC:ErrOwnerCampaignKeyDeleted"))
	ErrServiceSafepointLost         = errors.Normalize("service safepoint lost. current safepoint is %d, please remove all changefeed(s) whose checkpoints are behind the current safepoint", errors.RFCCodeText("CDC:ErrServiceSafepointLost"))
	ErrUpdateServiceSafepointFailed = errors.Normalize("updating service safepoint failed", errors.RFCCodeText("CDC:ErrUpdateServiceSafepointFailed"))
	ErrStartTsBeforeGC              = errors.Normalize("fail to create changefeed because start-ts %d is earlier than GC safepoint at %d", errors.RFCCodeText("CDC:ErrStartTsBeforeGC"))
	ErrTargetTsBeforeStartTs        = errors.Normalize("fail to create changefeed because target-ts %d is earlier than start-ts %d", errors.RFCCodeText("CDC:ErrTargetTsBeforeStartTs"))
	ErrSnapshotLostByGC             = errors.Normalize("fail to create or maintain changefeed due to snapshot loss caused by GC. checkpoint-ts %d is earlier than or equal to GC safepoint at %d", errors.RFCCodeText("CDC:ErrSnapshotLostByGC"))
	ErrGCTTLExceeded                = errors.Normalize("the checkpoint-ts(%d) lag of the changefeed(%s) has exceeded the GC TTL", errors.RFCCodeText("CDC:ErrGCTTLExceeded"))
	ErrNotOwner                     = errors.Normalize("this capture is not a owner", errors.RFCCodeText("CDC:ErrNotOwner"))
	ErrOwnerNotFound                = errors.Normalize("owner not found", errors.RFCCodeText("CDC:ErrOwnerNotFound"))
	ErrTableListenReplicated        = errors.Normalize("A table(%d) is being replicated by at least two processors(%s, %s), please report a bug", errors.RFCCodeText("CDC:ErrTableListenReplicated"))
	ErrTableIneligible              = errors.Normalize("some tables are not eligible to replicate(%v), if you want to ignore these tables, please set ignore_ineligible_table to true", errors.RFCCodeText("CDC:ErrTableIneligible"))

	// EtcdWorker related errors. Internal use only.
	// ErrEtcdTryAgain is used by a PatchFunc to force a transaction abort.
	ErrEtcdTryAgain = errors.Normalize("the etcd txn should be aborted and retried immediately", errors.RFCCodeText("CDC:ErrEtcdTryAgain"))
	// ErrEtcdIgnore is used by a PatchFunc to signal that the reactor no longer wishes to update Etcd.
	ErrEtcdIgnore = errors.Normalize("this patch should be excluded from the current etcd txn", errors.RFCCodeText("CDC:ErrEtcdIgnore"))
	// ErrEtcdSessionDone is used by etcd worker to signal a session done
	ErrEtcdSessionDone = errors.Normalize("the etcd session is done", errors.RFCCodeText("CDC:ErrEtcdSessionDone"))
	// ErrReactorFinished is used by reactor to signal a **normal** exit.
	ErrReactorFinished   = errors.Normalize("the reactor has done its job and should no longer be executed", errors.RFCCodeText("CDC:ErrReactorFinished"))
	ErrLeaseTimeout      = errors.Normalize("owner lease timeout", errors.RFCCodeText("CDC:ErrLeaseTimeout"))
	ErrLeaseExpired      = errors.Normalize("owner lease expired ", errors.RFCCodeText("CDC:ErrLeaseExpired"))
	ErrEtcdTxnSizeExceed = errors.Normalize("patch size:%d of a single changefeed exceed etcd txn max size:%d", errors.RFCCodeText("CDC:ErrEtcdTxnSizeExceed"))
	ErrEtcdTxnOpsExceed  = errors.Normalize("patch ops:%d of a single changefeed exceed etcd txn max ops:%d", errors.RFCCodeText("CDC:ErrEtcdTxnOpsExceed"))

	// pipeline errors
	ErrSendToClosedPipeline = errors.Normalize("pipeline is closed, cannot send message", errors.RFCCodeText("CDC:ErrSendToClosedPipeline"))
	ErrPipelineTryAgain     = errors.Normalize("pipeline is full, please try again. Internal use only, report a bug if seen externally", errors.RFCCodeText("CDC:ErrPipelineTryAgain"))

	// actor errors
	ErrActorDuplicate = errors.Normalize("duplicated actor, already in use", errors.RFCCodeText("CDC:ErrActorDuplicate"))
	ErrActorNotFound  = errors.Normalize("actor not found", errors.RFCCodeText("CDC:ErrActorNotFound"))
	ErrActorStopped   = errors.Normalize("actor stopped", errors.RFCCodeText("CDC:ErrActorStopped"))
	ErrMailboxFull    = errors.Normalize("mailbox is full, please try again. Internal use only, report a bug if seen externally", errors.RFCCodeText("CDC:ErrMailboxFull"))

	// leveldb sorter errors
	ErrStartAStoppedLevelDBSystem = errors.Normalize("start a stopped leveldb system", errors.RFCCodeText("CDC:ErrStartAStoppedLevelDBSystem"))
	ErrUnexpectedSnapshot         = errors.Normalize("unexpected snapshot, table %d", errors.RFCCodeText("CDC:ErrUnexpectedSnapshot"))

	// workerpool errors
	ErrWorkerPoolHandleCancelled            = errors.Normalize("workerpool handle is cancelled", errors.RFCCodeText("CDC:ErrWorkerPoolHandleCancelled"))
	ErrAsyncPoolExited                      = errors.Normalize("asyncPool has exited. Report a bug if seen externally.", errors.RFCCodeText("CDC:ErrAsyncPoolExited"))
	ErrWorkerPoolGracefulUnregisterTimedOut = errors.Normalize("workerpool handle graceful unregister timed out", errors.RFCCodeText("CDC:ErrWorkerPoolGracefulUnregisterTimedOut"))

	// redo log related errors
	ErrConsistentLevel   = errors.Normalize("consistent level (%s) not support", errors.RFCCodeText("CDC:ErrConsistentLevel"))
	ErrConsistentStorage = errors.Normalize("consistent storage (%s) not support", errors.RFCCodeText("CDC:ErrConsistentStorage"))
	ErrInvalidS3URI      = errors.Normalize("invalid s3 uri: %s", errors.RFCCodeText("CDC:ErrInvalidS3URI"))
	ErrBufferLogTimeout  = errors.Normalize("send row changed events to log buffer timeout", errors.RFCCodeText("CDC:ErrBufferLogTimeout"))

	// sorter errors
	ErrCheckDataDirSatisfied           = errors.Normalize("check data dir satisfied failed", errors.RFCCodeText("CDC:ErrCheckDataDirSatisfied"))
	ErrUnifiedSorterBackendTerminating = errors.Normalize("unified sorter backend is terminating", errors.RFCCodeText("CDC:ErrUnifiedSorterBackendTerminating"))
	ErrUnifiedSorterIOError            = errors.Normalize("unified sorter IO error. Make sure your sort-dir is configured correctly by passing a valid argument or toml file to `cdc server`, or if you use TiUP, review the settings in `tiup cluster edit-config`. Details: %s", errors.RFCCodeText("CDC:ErrUnifiedSorterIOError"))
	ErrIllegalSorterParameter          = errors.Normalize("illegal parameter for sorter: %s", errors.RFCCodeText("CDC:ErrIllegalSorterParameter"))
	ErrAsyncIOCancelled                = errors.Normalize("asynchronous IO operation is cancelled. Internal use only, report a bug if seen in log", errors.RFCCodeText("CDC:ErrAsyncIOCancelled"))
	ErrConflictingFileLocks            = errors.Normalize("file lock conflict: %s", errors.RFCCodeText("ErrConflictingFileLocks"))
	ErrSortDirLockError                = errors.Normalize("error encountered when locking sort-dir", errors.RFCCodeText("ErrSortDirLockError"))
	ErrLevelDBSorterError              = errors.Normalize("leveldb error: %s", errors.RFCCodeText("CDC:ErrLevelDBSorterError"))
	ErrSorterClosed                    = errors.Normalize("sorter is closed", errors.RFCCodeText("CDC:ErrSorterClosed"))

	// processor errors
	ErrTableProcessorStoppedSafely  = errors.Normalize("table processor stopped safely", errors.RFCCodeText("CDC:ErrTableProcessorStoppedSafely"))
	ErrProcessorDuplicateOperations = errors.Normalize("table processor duplicate operation, table-id: %d", errors.RFCCodeText("CDC:ErrProcessorDuplicateOperations"))

	// owner errors
	ErrOwnerChangedUnexpectedly = errors.Normalize("owner changed unexpectedly", errors.RFCCodeText("CDC:ErrOwnerChangedUnexpectedly"))
	// owner related errors
	ErrOwnerInconsistentStates = errors.Normalize("owner encountered inconsistent state. report a bug if this happens frequently. %s", errors.RFCCodeText("CDC:ErrOwnerInconsistentStates"))

	// miscellaneous internal errors
	ErrFlowControllerAborted              = errors.Normalize("flow controller is aborted", errors.RFCCodeText("CDC:ErrFlowControllerAborted"))
	ErrFlowControllerEventLargerThanQuota = errors.Normalize("event is larger than the total memory quota, size: %d, quota: %d", errors.RFCCodeText("CDC:ErrFlowControllerEventLargerThanQuota"))

	// retry error
	ErrReachMaxTry = errors.Normalize("reach maximum try: %d", errors.RFCCodeText("CDC:ErrReachMaxTry"))

	// tcp server error
	ErrTCPServerClosed = errors.Normalize("The TCP server has been closed", errors.RFCCodeText("CDC:ErrTCPServerClosed"))

	// p2p error
	ErrPeerMessageIllegalMeta           = errors.Normalize("peer-to-peer message server received an RPC call with illegal metadata", errors.RFCCodeText("CDC:ErrPeerMessageIllegalMeta"))
	ErrPeerMessageClientPermanentFail   = errors.Normalize("peer-to-peer message client has failed permanently, no need to reconnect: %s", errors.RFCCodeText("CDC:ErrPeerMessageClientPermanentFail"))
	ErrPeerMessageClientClosed          = errors.Normalize("peer-to-peer message client has been closed", errors.RFCCodeText("CDC:ErrPeerMessageClientClosed"))
	ErrPeerMessageSendTryAgain          = errors.Normalize("peer-to-peer message client has too many pending messages to send, try again later", errors.RFCCodeText("CDC:ErrPeerMessageSendTryAgain"))
	ErrPeerMessageEncodeError           = errors.Normalize("failed to encode peer-to-peer message", errors.RFCCodeText("CDC:ErrPeerMessageEncodeError"))
	ErrPeerMessageInternalSenderClosed  = errors.Normalize("peer-to-peer message server tries to send to a closed stream. Internal only.", errors.RFCCodeText("CDC:ErrPeerMessageInternalSenderClosed"))
	ErrPeerMessageStaleConnection       = errors.Normalize("peer-to-peer message stale connection: old-epoch %d, new-epoch %d", errors.RFCCodeText("CDC:ErrPeerMessageStaleConnection"))
	ErrPeerMessageDuplicateConnection   = errors.Normalize("peer-to-peer message duplicate connection: epoch %d", errors.RFCCodeText("CDC:ErrPeerMessageDuplicateConnection"))
	ErrPeerMessageServerClosed          = errors.Normalize("peer-to-peer message server has closed connection: %s.", errors.RFCCodeText("CDC:ErrPeerMessageServerClosed"))
	ErrPeerMessageDataLost              = errors.Normalize("peer-to-peer message data lost, topic: %s, seq: %s", errors.RFCCodeText("CDC:ErrPeerMessageDataLost"))
	ErrPeerMessageToManyPeers           = errors.Normalize("peer-to-peer message server got too many peers: %d peers", errors.RFCCodeText("CDC:ErrPeerMessageToManyPeers"))
	ErrPeerMessageDecodeError           = errors.Normalize("failed to decode peer-to-peer message", errors.RFCCodeText("CDC:ErrPeerMessageDecodeError"))
	ErrPeerMessageTaskQueueCongested    = errors.Normalize("peer-to-peer message server has too many pending tasks", errors.RFCCodeText("CDC:ErrPeerMessageTaskQueueCongested"))
	ErrPeerMessageReceiverMismatch      = errors.Normalize("peer-to-peer message receiver is a mismatch: expected %s, got %s", errors.RFCCodeText("CDC:ErrPeerMessageReceiverMismatch"))
	ErrPeerMessageIllegalClientVersion  = errors.Normalize("peer-to-peer message client reported illegal version: %s", errors.RFCCodeText("CDC:ErrPeerMessageIllegalClientVersion"))
	ErrPeerMessageTopicCongested        = errors.Normalize("peer-to-peer message topic has congested, aborting all connections", errors.RFCCodeText("CDC:ErrPeerMessageTopicCongested"))
	ErrPeerMessageInjectedServerRestart = errors.Normalize("peer-to-peer message server injected error", errors.RFCCodeText("CDC:ErrPeerMessageInjectedServerRestart"))

	// RESTful client error
	ErrRewindRequestBodyError = errors.Normalize("failed to seek to the beginning of request body", errors.RFCCodeText("CDC:ErrRewindRequestBodyError"))
	ErrZeroLengthResponseBody = errors.Normalize("0-length response with status code: %d", errors.RFCCodeText("CDC:ErrZeroLengthResponseBody"))
	ErrInvalidHost            = errors.Normalize("host must be a URL or a host:port pair: %q", errors.RFCCodeText("CDC:ErrInvalidHost"))
)
