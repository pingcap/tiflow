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
	ErrUnknownKVEventType      = errors.Normalize("unknown kv event type: %v, entry: %v", errors.RFCCodeText("CDC:ErrUnknownKVEventType"))
	ErrNoPendingRegion         = errors.Normalize("received event regionID %v, requestID %v from %v,"+
		" but neither pending region nor running region was found", errors.RFCCodeText("CDC:ErrNoPendingRegion"))
	ErrPrewriteNotMatch       = errors.Normalize("prewrite not match, key: %b, start-ts: %d", errors.RFCCodeText("CDC:ErrPrewriteNotMatch"))
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
	ErrNewStore               = errors.Normalize("new store faile", errors.RFCCodeText("CDC:ErrNewStore"))

	// rule related errors
	ErrEncodeFailed      = errors.Normalize("encode failed: %s", errors.RFCCodeText("CDC:ErrEncodeFailed"))
	ErrDecodeFailed      = errors.Normalize("decode failed: %s", errors.RFCCodeText("CDC:ErrDecodeFailed"))
	ErrFilterRuleInvalid = errors.Normalize("filter rule is invalid", errors.RFCCodeText("CDC:ErrFilterRuleInvalid"))

	// internal errors
	ErrAdminStopProcessor = errors.Normalize("stop processor by admin command", errors.RFCCodeText("CDC:ErrAdminStopProcessor"))
	// ErrVersionIncompatible is an error for running CDC on an incompatible Cluster.
	ErrVersionIncompatible   = errors.Normalize("version is incompatible: %s", errors.RFCCodeText("CDC:ErrVersionIncompatible"))
	ErrCreateMarkTableFailed = errors.Normalize("create mark table failed", errors.RFCCodeText("CDC:ErrCreateMarkTableFailed"))

	// sink related errors
	ErrExecDDLFailed             = errors.Normalize("exec DDL failed", errors.RFCCodeText("CDC:ErrExecDDLFailed"))
	ErrDDLEventIgnored           = errors.Normalize("ddl event is ignored", errors.RFCCodeText("CDC:ErrDDLEventIgnored"))
	ErrKafkaSendMessage          = errors.Normalize("kafka send message failed", errors.RFCCodeText("CDC:ErrKafkaSendMessage"))
	ErrKafkaAsyncSendMessage     = errors.Normalize("kafka async send message failed", errors.RFCCodeText("CDC:ErrKafkaAsyncSendMessage"))
	ErrKafkaFlushUnfished        = errors.Normalize("flush not finished before producer close", errors.RFCCodeText("CDC:ErrKafkaFlushUnfished"))
	ErrKafkaInvalidPartitionNum  = errors.Normalize("invalid partition num %d", errors.RFCCodeText("CDC:ErrKafkaInvalidPartitionNum"))
	ErrKafkaNewSaramaProducer    = errors.Normalize("new sarama producer", errors.RFCCodeText("CDC:ErrKafkaNewSaramaProducer"))
	ErrKafkaInvalidClientID      = errors.Normalize("invalid kafka client ID '%s'", errors.RFCCodeText("CDC:ErrKafkaInvalidClientID"))
	ErrKafkaInvalidVersion       = errors.Normalize("invalid kafka version", errors.RFCCodeText("CDC:ErrKafkaInvalidVersion"))
	ErrPulsarNewProducer         = errors.Normalize("new pulsar producer", errors.RFCCodeText("CDC:ErrPulsarNewProducer"))
	ErrPulsarSendMessage         = errors.Normalize("pulsar send message failed", errors.RFCCodeText("CDC:ErrPulsarSendMessage"))
	ErrFileSinkCreateDir         = errors.Normalize("file sink create dir", errors.RFCCodeText("CDC:ErrFileSinkCreateDir"))
	ErrFileSinkFileOp            = errors.Normalize("file sink file operation", errors.RFCCodeText("CDC:ErrFileSinkFileOp"))
	ErrFileSinkMetaAlreadyExists = errors.Normalize("file sink meta file already exists", errors.RFCCodeText("CDC:ErrFileSinkMetaAlreadyExists"))
	ErrS3SinkWriteStorage        = errors.Normalize("write to storage", errors.RFCCodeText("CDC:ErrS3SinkWriteStorage"))
	ErrS3SinkInitialzie          = errors.Normalize("new s3 sink", errors.RFCCodeText("CDC:ErrS3SinkInitialzie"))
	ErrS3SinkStorageAPI          = errors.Normalize("s3 sink storage api", errors.RFCCodeText("CDC:ErrS3SinkStorageAPI"))

	// utilities related errors
	ErrToTLSConfigFailed         = errors.Normalize("generate tls config failed", errors.RFCCodeText("CDC:ErrToTLSConfigFailed"))
	ErrCheckClusterVersionFromPD = errors.Normalize("failed to request PD", errors.RFCCodeText("CDC:ErrCheckClusterVersionFromPD"))
	ErrNewSemVersion             = errors.Normalize("create sem version", errors.RFCCodeText("CDC:ErrNewSemVersion"))
	ErrCheckDirWritable          = errors.Normalize("check dir writable failed", errors.RFCCodeText("CDC:ErrCheckDirWritable"))
	ErrLoadTimezone              = errors.Normalize("load timezone", errors.RFCCodeText("CDC:ErrLoadTimezone"))
	ErrURLFormatInvalid          = errors.Normalize("url format is invalid", errors.RFCCodeText("CDC:ErrURLFormatInvalid"))
	ErrIntersectNoOverlap        = errors.Normalize("span doesn't overlap: %+v vs %+v", errors.RFCCodeText("CDC:ErrIntersectNoOverlap"))

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
	ErrInvalidChangefeedID   = errors.Normalize(`bad changefeed id, please match the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$", eg, "simple-changefeed-task"`, errors.RFCCodeText("CDC:ErrInvalidChangefeedID"))
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

	// puller related errors
	ErrBufferReachLimit      = errors.Normalize("puller mem buffer reach size limit", errors.RFCCodeText("CDC:ErrBufferReachLimit"))
	ErrFileSorterOpenFile    = errors.Normalize("open file failed", errors.RFCCodeText("CDC:ErrFileSorterOpenFile"))
	ErrFileSorterReadFile    = errors.Normalize("read file failed", errors.RFCCodeText("CDC:ErrFileSorterReadFile"))
	ErrFileSorterWriteFile   = errors.Normalize("write file failed", errors.RFCCodeText("CDC:ErrFileSorterWriteFile"))
	ErrFileSorterEncode      = errors.Normalize("encode failed", errors.RFCCodeText("CDC:ErrFileSorterEncode"))
	ErrFileSorterDecode      = errors.Normalize("decode failed", errors.RFCCodeText("CDC:ErrFileSorterDecode"))
	ErrFileSorterInvalidData = errors.Normalize("invalid data", errors.RFCCodeText("CDC:ErrFileSorterInvalidData"))
)
