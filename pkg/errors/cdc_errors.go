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
	"fmt"

	"github.com/pingcap/errors"
)

// errors
var (
	// kv related errors
	ErrChangeFeedNotExists = errors.Normalize(
		"changefeed not exists, %s",
		errors.RFCCodeText("CDC:ErrChangeFeedNotExists"),
	)
	ErrChangeFeedAlreadyExists = errors.Normalize(
		"changefeed already exists, %s",
		errors.RFCCodeText("CDC:ErrChangeFeedAlreadyExists"),
	)
	ErrChangeFeedDeletionUnfinished = errors.Normalize(
		"changefeed exists after deletion, %s",
		errors.RFCCodeText("CDC:ErrChangeFeedDeletionUnfinished"),
	)
	ErrCaptureNotExist = errors.Normalize(
		"capture not exists, %s",
		errors.RFCCodeText("CDC:ErrCaptureNotExist"),
	)
	ErrSchedulerRequestFailed = errors.Normalize(
		"scheduler request failed, %s",
		errors.RFCCodeText("CDC:ErrSchedulerRequestFailed"),
	)
	ErrGetAllStoresFailed = errors.Normalize(
		"get stores from pd failed",
		errors.RFCCodeText("CDC:ErrGetAllStoresFailed"),
	)
	ErrMetaListDatabases = errors.Normalize(
		"meta store list databases",
		errors.RFCCodeText("CDC:ErrMetaListDatabases"),
	)
	ErrDDLSchemaNotFound = errors.Normalize(
		"cannot find mysql.tidb_ddl_job schema",
		errors.RFCCodeText("CDC:ErrDDLSchemaNotFound"),
	)
	ErrGRPCDialFailed = errors.Normalize(
		"grpc dial failed",
		errors.RFCCodeText("CDC:ErrGRPCDialFailed"),
	)
	ErrTiKVEventFeed = errors.Normalize(
		"tikv event feed failed",
		errors.RFCCodeText("CDC:ErrTiKVEventFeed"),
	)
	ErrPDBatchLoadRegions = errors.Normalize(
		"pd batch load regions failed",
		errors.RFCCodeText("CDC:ErrPDBatchLoadRegions"),
	)
	ErrMetaNotInRegion = errors.Normalize(
		"meta not exists in region",
		errors.RFCCodeText("CDC:ErrMetaNotInRegion"),
	)
	ErrRegionsNotCoverSpan = errors.Normalize(
		"regions not completely left cover span, span %v regions: %v",
		errors.RFCCodeText("CDC:ErrRegionsNotCoverSpan"),
	)
	ErrGetTiKVRPCContext = errors.Normalize(
		"get tikv grpc context failed",
		errors.RFCCodeText("CDC:ErrGetTiKVRPCContext"),
	)
	ErrPendingRegionCancel = errors.Normalize(
		"pending region cancelled due to stream disconnecting",
		errors.RFCCodeText("CDC:ErrPendingRegionCancel"),
	)
	ErrEventFeedAborted = errors.Normalize(
		"single event feed aborted",
		errors.RFCCodeText("CDC:ErrEventFeedAborted"),
	)
	ErrUnknownKVEventType = errors.Normalize(
		"unknown kv optype: %s, entry: %v",
		errors.RFCCodeText("CDC:ErrUnknownKVEventType"),
	)
	ErrPrewriteNotMatch = errors.Normalize(
		"prewrite not match, key: %s, start-ts: %d, commit-ts: %d, type: %s, optype: %s",
		errors.RFCCodeText("CDC:ErrPrewriteNotMatch"),
	)
	ErrEventFeedEventError = errors.Normalize(
		"eventfeed returns event error",
		errors.RFCCodeText("CDC:ErrEventFeedEventError"),
	)
	ErrPDEtcdAPIError = errors.Normalize(
		"etcd api call error",
		errors.RFCCodeText("CDC:ErrPDEtcdAPIError"),
	)
	ErrNewStore = errors.Normalize(
		"new store failed",
		errors.RFCCodeText("CDC:ErrNewStore"),
	)
	ErrRegionWorkerExit = errors.Normalize(
		"region worker exited",
		errors.RFCCodeText("CDC:ErrRegionWorkerExit"),
	)

	// codec related errors
	ErrEncodeFailed = errors.Normalize(
		"encode failed: %s",
		errors.RFCCodeText("CDC:ErrEncodeFailed"),
	)
	ErrDecodeFailed = errors.Normalize(
		"decode failed: %s",
		errors.RFCCodeText("CDC:ErrDecodeFailed"),
	)
	ErrFilterRuleInvalid = errors.Normalize(
		"filter rule is invalid %v",
		errors.RFCCodeText("CDC:ErrFilterRuleInvalid"),
	)

	ErrDispatcherFailed = errors.Normalize(
		"dispatcher failed",
		errors.RFCCodeText("CDC:ErrDispatcherFailed"),
	)

	ErrColumnSelectorFailed = errors.Normalize(
		"column selector failed",
		errors.RFCCodeText("CDC:ErrColumnSelectorFailed"),
	)

	// internal errors
	ErrAdminStopProcessor = errors.Normalize(
		"stop processor by admin command",
		errors.RFCCodeText("CDC:ErrAdminStopProcessor"),
	)
	// ErrVersionIncompatible is an error for running CDC on an incompatible Cluster.
	ErrVersionIncompatible = errors.Normalize(
		"version is incompatible: %s",
		errors.RFCCodeText("CDC:ErrVersionIncompatible"),
	)
	ErrClusterIDMismatch = errors.Normalize(
		"cluster ID mismatch, tikv cluster ID is %d and request cluster ID is %d",
		errors.RFCCodeText("CDC:ErrClusterIDMismatch"),
	)
	ErrMultipleCDCClustersExist = errors.Normalize(
		"multiple TiCDC clusters exist while using --pd",
		errors.RFCCodeText("CDC:ErrMultipleCDCClustersExist"),
	)

	// sink related errors
	ErrExecDDLFailed = errors.Normalize(
		"exec DDL failed",
		errors.RFCCodeText("CDC:ErrExecDDLFailed"),
	)
	ErrKafkaSendMessage = errors.Normalize(
		"kafka send message failed",
		errors.RFCCodeText("CDC:ErrKafkaSendMessage"),
	)
	ErrKafkaProducerClosed = errors.Normalize(
		"kafka producer closed",
		errors.RFCCodeText("CDC:ErrKafkaProducerClosed"),
	)
	ErrKafkaAsyncSendMessage = errors.Normalize(
		"kafka async send message failed",
		errors.RFCCodeText("CDC:ErrKafkaAsyncSendMessage"),
	)
	ErrKafkaInvalidPartitionNum = errors.Normalize(
		"invalid partition num %d",
		errors.RFCCodeText("CDC:ErrKafkaInvalidPartitionNum"),
	)
	ErrKafkaInvalidRequiredAcks = errors.Normalize(
		"invalid required acks %d, "+
			"only support these values: 0(NoResponse),1(WaitForLocal) and -1(WaitForAll)",
		errors.RFCCodeText("CDC:ErrKafkaInvalidRequiredAcks"),
	)
	ErrKafkaNewProducer = errors.Normalize(
		"new kafka producer",
		errors.RFCCodeText("CDC:ErrKafkaNewProducer"),
	)
	ErrKafkaInvalidClientID = errors.Normalize(
		"invalid kafka client ID '%s'",
		errors.RFCCodeText("CDC:ErrKafkaInvalidClientID"),
	)
	ErrKafkaInvalidVersion = errors.Normalize(
		"invalid kafka version",
		errors.RFCCodeText("CDC:ErrKafkaInvalidVersion"),
	)
	ErrKafkaInvalidConfig = errors.Normalize(
		"kafka config invalid",
		errors.RFCCodeText("CDC:ErrKafkaInvalidConfig"),
	)
	ErrKafkaCreateTopic = errors.Normalize(
		"kafka create topic failed",
		errors.RFCCodeText("CDC:ErrKafkaCreateTopic"),
	)
	ErrKafkaInvalidTopicExpression = errors.Normalize(
		"invalid topic expression",
		errors.RFCCodeText("CDC:ErrKafkaTopicExprInvalid"),
	)
	ErrKafkaConfigNotFound = errors.Normalize(
		"kafka config item not found",
		errors.RFCCodeText("CDC:ErrKafkaConfigNotFound"),
	)
	// for pulsar
	ErrPulsarSendMessage = errors.Normalize(
		"pulsar send message failed",
		errors.RFCCodeText("CDC:ErrPulsarSendMessage"),
	)
	ErrPulsarProducerClosed = errors.Normalize(
		"pulsar producer closed",
		errors.RFCCodeText("CDC:ErrPulsarProducerClosed"),
	)
	ErrPulsarAsyncSendMessage = errors.Normalize(
		"pulsar async send message failed",
		errors.RFCCodeText("CDC:ErrPulsarAsyncSendMessage"),
	)
	ErrPulsarFlushUnfinished = errors.Normalize(
		"flush not finished before producer close",
		errors.RFCCodeText("CDC:ErrPulsarFlushUnfinished"),
	)
	ErrPulsarInvalidPartitionNum = errors.Normalize(
		"invalid partition num %d",
		errors.RFCCodeText("CDC:ErrPulsarInvalidPartitionNum"),
	)
	ErrPulsarNewClient = errors.Normalize(
		"new pulsar client",
		errors.RFCCodeText("CDC:ErrPulsarNewClient"),
	)
	ErrPulsarNewProducer = errors.Normalize(
		"new pulsar producer",
		errors.RFCCodeText("CDC:ErrPulsarNewProducer"),
	)
	ErrPulsarInvalidClientID = errors.Normalize(
		"invalid pulsar client ID '%s'",
		errors.RFCCodeText("CDC:ErrPulsarInvalidClientID"),
	)
	ErrPulsarInvalidVersion = errors.Normalize(
		"invalid pulsar version",
		errors.RFCCodeText("CDC:ErrPulsarInvalidVersion"),
	)
	ErrPulsarInvalidConfig = errors.Normalize(
		"pulsar config invalid %s",
		errors.RFCCodeText("CDC:ErrPulsarInvalidConfig"),
	)
	ErrPulsarCreateTopic = errors.Normalize(
		"pulsar create topic failed",
		errors.RFCCodeText("CDC:ErrPulsarCreateTopic"),
	)
	ErrPulsarInvalidTopicExpression = errors.Normalize(
		"invalid topic expression",
		errors.RFCCodeText("CDC:ErrPulsarTopicExprInvalid"),
	)
	ErrPulsarBrokerConfigNotFound = errors.Normalize(
		"pulsar broker config item not found",
		errors.RFCCodeText("CDC:ErrPulsarBrokerConfigNotFound"),
	)
	ErrPulsarTopicNotExists = errors.Normalize("pulsar topic not exists after creation",
		errors.RFCCodeText("CDC:ErrPulsarTopicNotExists"),
	)

	ErrRedoConfigInvalid = errors.Normalize(
		"redo log config invalid",
		errors.RFCCodeText("CDC:ErrRedoConfigInvalid"),
	)
	ErrRedoDownloadFailed = errors.Normalize(
		"redo log down load to local failed",
		errors.RFCCodeText("CDC:ErrRedoDownloadFailed"),
	)
	ErrRedoWriterStopped = errors.Normalize(
		"redo log writer stopped",
		errors.RFCCodeText("CDC:ErrRedoWriterStopped"),
	)
	ErrRedoFileOp = errors.Normalize(
		"redo file operation",
		errors.RFCCodeText("CDC:ErrRedoFileOp"),
	)
	ErrRedoMetaFileNotFound = errors.Normalize(
		"no redo meta file found in dir: %s",
		errors.RFCCodeText("CDC:ErrRedoMetaFileNotFound"),
	)
	ErrRedoMetaInitialize = errors.Normalize(
		"initialize meta for redo log",
		errors.RFCCodeText("CDC:ErrRedoMetaInitialize"),
	)
	ErrFileSizeExceed = errors.Normalize(
		"rawData size %d exceeds maximum file size %d",
		errors.RFCCodeText("CDC:ErrFileSizeExceed"),
	)
	ErrExternalStorageAPI = errors.Normalize(
		"external storage api",
		errors.RFCCodeText("CDC:ErrS3StorageAPI"),
	)
	ErrStorageInitialize = errors.Normalize(
		"fail to open storage for redo log",
		errors.RFCCodeText("CDC:ErrStorageInitialize"),
	)
	ErrCodecInvalidConfig = errors.Normalize(
		"Codec invalid config",
		errors.RFCCodeText("CDC:ErrCodecInvalidConfig"),
	)

	ErrCompressionFailed = errors.Normalize(
		"Compression failed",
		errors.RFCCodeText("CDC:ErrCompressionFailed"),
	)

	ErrSinkURIInvalid = errors.Normalize(
		"sink uri invalid '%s'",
		errors.RFCCodeText("CDC:ErrSinkURIInvalid"),
	)
	ErrIncompatibleSinkConfig = errors.Normalize(
		"incompatible configuration in sink uri(%s) and config file(%s), "+
			"please try to update the configuration only through sink uri",
		errors.RFCCodeText("CDC:ErrIncompatibleSinkConfig"),
	)
	ErrSinkUnknownProtocol = errors.Normalize(
		"unknown '%s' message protocol for sink",
		errors.RFCCodeText("CDC:ErrSinkUnknownProtocol"),
	)
	ErrMySQLTxnError = errors.Normalize(
		"MySQL txn error",
		errors.RFCCodeText("CDC:ErrMySQLTxnError"),
	)
	ErrMySQLQueryError = errors.Normalize(
		"MySQL query error",
		errors.RFCCodeText("CDC:ErrMySQLQueryError"),
	)
	ErrMySQLConnectionError = errors.Normalize(
		"MySQL connection error",
		errors.RFCCodeText("CDC:ErrMySQLConnectionError"),
	)
	ErrMySQLInvalidConfig = errors.Normalize(
		"MySQL config invalid",
		errors.RFCCodeText("CDC:ErrMySQLInvalidConfig"),
	)
	ErrMySQLWorkerPanic = errors.Normalize(
		"MySQL worker panic",
		errors.RFCCodeText("CDC:ErrMySQLWorkerPanic"),
	)
	ErrAvroToEnvelopeError = errors.Normalize(
		"to envelope failed",
		errors.RFCCodeText("CDC:ErrAvroToEnvelopeError"),
	)
	ErrAvroMarshalFailed = errors.Normalize(
		"json marshal failed",
		errors.RFCCodeText("CDC:ErrAvroMarshalFailed"),
	)
	ErrAvroEncodeFailed = errors.Normalize(
		"encode to avro native data",
		errors.RFCCodeText("CDC:ErrAvroEncodeFailed"),
	)
	ErrAvroEncodeToBinary = errors.Normalize(
		"encode to binray from native",
		errors.RFCCodeText("CDC:ErrAvroEncodeToBinary"),
	)
	ErrAvroSchemaAPIError = errors.Normalize(
		"schema manager API error, %s",
		errors.RFCCodeText("CDC:ErrAvroSchemaAPIError"),
	)
	ErrAvroInvalidMessage = errors.Normalize(
		"avro invalid message format, %s",
		errors.RFCCodeText("CDC:ErrAvroInvalidMessage"),
	)
	ErrMaxwellEncodeFailed = errors.Normalize(
		"maxwell encode failed",
		errors.RFCCodeText("CDC:ErrMaxwellEncodeFailed"),
	)
	ErrMaxwellInvalidData = errors.Normalize(
		"maxwell invalid data",
		errors.RFCCodeText("CDC:ErrMaxwellInvalidData"),
	)
	ErrOpenProtocolCodecInvalidData = errors.Normalize(
		"open-protocol codec invalid data",
		errors.RFCCodeText("CDC:ErrOpenProtocolCodecInvalidData"),
	)
	ErrCanalDecodeFailed = errors.Normalize(
		"canal decode failed",
		errors.RFCCodeText("CDC:ErrCanalDecodeFailed"),
	)
	ErrCanalEncodeFailed = errors.Normalize(
		"canal encode failed",
		errors.RFCCodeText("CDC:ErrCanalEncodeFailed"),
	)
	ErrOldValueNotEnabled = errors.Normalize(
		"old value is not enabled",
		errors.RFCCodeText("CDC:ErrOldValueNotEnabled"),
	)
	ErrSinkInvalidConfig = errors.Normalize(
		"sink config invalid",
		errors.RFCCodeText("CDC:ErrSinkInvalidConfig"),
	)
	ErrCraftCodecInvalidData = errors.Normalize(
		"craft codec invalid data",
		errors.RFCCodeText("CDC:ErrCraftCodecInvalidData"),
	)
	ErrMessageTooLarge = errors.Normalize(
		"message is too large",
		errors.RFCCodeText("CDC:ErrMessageTooLarge"),
	)
	ErrStorageSinkInvalidDateSeparator = errors.Normalize(
		"date separator in storage sink is invalid",
		errors.RFCCodeText("CDC:ErrStorageSinkInvalidDateSeparator"),
	)
	ErrCSVEncodeFailed = errors.Normalize(
		"csv encode failed",
		errors.RFCCodeText("CDC:ErrCSVEncodeFailed"),
	)
	ErrCSVDecodeFailed = errors.Normalize(
		"csv decode failed",
		errors.RFCCodeText("CDC:ErrCSVDecodeFailed"),
	)
	ErrStorageSinkInvalidConfig = errors.Normalize(
		"storage sink config invalid",
		errors.RFCCodeText("CDC:ErrStorageSinkInvalidConfig"),
	)
	ErrStorageSinkInvalidFileName = errors.Normalize(
		"filename in storage sink is invalid",
		errors.RFCCodeText("CDC:ErrStorageSinkInvalidFileName"),
	)

	// utilities related errors
	ErrToTLSConfigFailed = errors.Normalize(
		"generate tls config failed",
		errors.RFCCodeText("CDC:ErrToTLSConfigFailed"),
	)
	ErrCheckClusterVersionFromPD = errors.Normalize(
		"failed to request PD %s, please try again later",
		errors.RFCCodeText("CDC:ErrCheckClusterVersionFromPD"),
	)
	ErrNewSemVersion = errors.Normalize(
		"create sem version",
		errors.RFCCodeText("CDC:ErrNewSemVersion"),
	)
	ErrCheckDirWritable = errors.Normalize(
		"check dir writable failed",
		errors.RFCCodeText("CDC:ErrCheckDirWritable"),
	)
	ErrCheckDirValid = errors.Normalize(
		"check dir valid failed",
		errors.RFCCodeText("CDC:ErrCheckDirValid"),
	)
	ErrGetDiskInfo = errors.Normalize(
		"get dir disk info failed",
		errors.RFCCodeText("CDC:ErrGetDiskInfo"),
	)
	ErrLoadTimezone = errors.Normalize(
		"load timezone",
		errors.RFCCodeText("CDC:ErrLoadTimezone"),
	)
	ErrURLFormatInvalid = errors.Normalize(
		"url format is invalid",
		errors.RFCCodeText("CDC:ErrURLFormatInvalid"),
	)
	ErrIntersectNoOverlap = errors.Normalize(
		"span doesn't overlap: %+v vs %+v",
		errors.RFCCodeText("CDC:ErrIntersectNoOverlap"),
	)
	ErrOperateOnClosedNotifier = errors.Normalize(
		"operate on a closed notifier",
		errors.RFCCodeText("CDC:ErrOperateOnClosedNotifier"),
	)
	ErrDiskFull = errors.Normalize(
		"failed to preallocate file because disk is full",
		errors.RFCCodeText("CDC:ErrDiskFull"))
	ErrWaitFreeMemoryTimeout = errors.Normalize(
		"wait free memory timeout",
		errors.RFCCodeText("CDC:ErrWaitFreeMemoryTimeout"),
	)

	// encode/decode, data format and data integrity errors
	ErrInvalidRecordKey = errors.Normalize(
		"invalid record key - %q",
		errors.RFCCodeText("CDC:ErrInvalidRecordKey"),
	)
	ErrCodecDecode = errors.Normalize(
		"codec decode error",
		errors.RFCCodeText("CDC:ErrCodecDecode"),
	)
	ErrUnknownMetaType = errors.Normalize(
		"unknown meta type %v",
		errors.RFCCodeText("CDC:ErrUnknownMetaType"),
	)
	ErrDatumUnflatten = errors.Normalize(
		"unflatten datume data",
		errors.RFCCodeText("CDC:ErrDatumUnflatten"),
	)
	ErrDecodeRowToDatum = errors.Normalize(
		"decode row data to datum failed",
		errors.RFCCodeText("CDC:ErrDecodeRowToDatum"),
	)
	ErrMarshalFailed = errors.Normalize(
		"marshal failed",
		errors.RFCCodeText("CDC:ErrMarshalFailed"),
	)
	ErrUnmarshalFailed = errors.Normalize(
		"unmarshal failed",
		errors.RFCCodeText("CDC:ErrUnmarshalFailed"),
	)
	ErrInvalidChangefeedID = errors.Normalize(
		fmt.Sprintf("%s, %s, %s, %s,",
			"bad changefeed id",
			`please match the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$"`,
			"the length should no more than %d",
			`eg, "simple-changefeed-task"`),
		errors.RFCCodeText("CDC:ErrInvalidChangefeedID"),
	)
	ErrInvalidNamespace = errors.Normalize(
		fmt.Sprintf("%s, %s, %s, %s,",
			"bad namespace",
			`please match the pattern "^[a-zA-Z0-9]+(\-[a-zA-Z0-9]+)*$"`,
			"the length should no more than %d",
			`eg, "simple-namespace-test"`),
		errors.RFCCodeText("CDC:ErrInvalidNamespace"),
	)
	ErrInvalidEtcdKey = errors.Normalize(
		"invalid key: %s",
		errors.RFCCodeText("CDC:ErrInvalidEtcdKey"),
	)

	// schema storage errors
	ErrSchemaStorageUnresolved = errors.Normalize(
		"can not found schema snapshot, the specified ts(%d) is more than resolvedTs(%d)",
		errors.RFCCodeText("CDC:ErrSchemaStorageUnresolved"),
	)
	ErrSchemaStorageGCed = errors.Normalize(
		"can not found schema snapshot, the specified ts(%d) is less than gcTS(%d)",
		errors.RFCCodeText("CDC:ErrSchemaStorageGCed"),
	)
	ErrSchemaSnapshotNotFound = errors.Normalize(
		"can not found schema snapshot, ts: %d",
		errors.RFCCodeText("CDC:ErrSchemaSnapshotNotFound"),
	)
	ErrSchemaStorageTableMiss = errors.Normalize(
		"table %d not found",
		errors.RFCCodeText("CDC:ErrSchemaStorageTableMiss"),
	)
	ErrSnapshotSchemaNotFound = errors.Normalize(
		"schema %d not found in schema snapshot",
		errors.RFCCodeText("CDC:ErrSnapshotSchemaNotFound"),
	)
	ErrSnapshotTableNotFound = errors.Normalize(
		"table %d not found in schema snapshot",
		errors.RFCCodeText("CDC:ErrSnapshotTableNotFound"),
	)
	ErrSnapshotSchemaExists = errors.Normalize(
		"schema %s(%d) already exists",
		errors.RFCCodeText("CDC:ErrSnapshotSchemaExists"),
	)
	ErrSnapshotTableExists = errors.Normalize(
		"table %s.%s already exists",
		errors.RFCCodeText("CDC:ErrSnapshotTableExists"),
	)
	ErrInvalidDDLJob = errors.Normalize(
		"invalid ddl job(%d)",
		errors.RFCCodeText("CDC:ErrInvalidDDLJob"),
	)
	ErrExchangePartition = errors.Normalize(
		"exchange partition failed, %s",
		errors.RFCCodeText("CDC:ErrExchangePartition"),
	)

	ErrCorruptedDataMutation = errors.Normalize(
		"Changefeed %s.%s stopped due to corrupted data mutation received. "+
			"Corrupted mutation detail information %+v",
		errors.RFCCodeText("CDC:ErrCorruptedDataMutation"))

	// server related errors
	ErrCaptureSuicide = errors.Normalize(
		"capture suicide",
		errors.RFCCodeText("CDC:ErrCaptureSuicide"),
	)
	ErrCaptureRegister = errors.Normalize(
		"capture register to etcd failed",
		errors.RFCCodeText("CDC:ErrCaptureRegister"),
	)
	ErrCaptureNotInitialized = errors.Normalize(
		"capture has not been initialized yet",
		errors.RFCCodeText("CDC:ErrCaptureNotInitialized"),
	)
	ErrProcessorUnknown = errors.Normalize(
		"processor running unknown error",
		errors.RFCCodeText("CDC:ErrProcessorUnknown"),
	)
	ErrOwnerUnknown = errors.Normalize(
		"owner running unknown error",
		errors.RFCCodeText("CDC:ErrOwnerUnknown"),
	)
	ErrProcessorTableNotFound = errors.Normalize(
		"table not found in processor cache",
		errors.RFCCodeText("CDC:ErrProcessorTableNotFound"),
	)
	ErrInvalidServerOption = errors.Normalize(
		"invalid server option",
		errors.RFCCodeText("CDC:ErrInvalidServerOption"),
	)
	ErrServeHTTP = errors.Normalize(
		"serve http error",
		errors.RFCCodeText("CDC:ErrServeHTTP"),
	)
	ErrCaptureCampaignOwner = errors.Normalize(
		"campaign owner failed",
		errors.RFCCodeText("CDC:ErrCaptureCampaignOwner"),
	)
	ErrCaptureResignOwner = errors.Normalize(
		"resign owner failed",
		errors.RFCCodeText("CDC:ErrCaptureResignOwner"),
	)
	ErrClusterIsUnhealthy = errors.Normalize(
		"TiCDC cluster is unhealthy",
		errors.RFCCodeText("CDC:ErrClusterIsUnhealthy"),
	)
	ErrAPIInvalidParam = errors.Normalize(
		"invalid api parameter",
		errors.RFCCodeText("CDC:ErrAPIInvalidParam"),
	)
	ErrAPIGetPDClientFailed = errors.Normalize(
		"failed to get PDClient to connect PD, please recheck",
		errors.RFCCodeText("CDC:ErrAPIGetPDClientFailed"),
	)
	ErrRequestForwardErr = errors.Normalize(
		"request forward error, an request can only forward to owner one time",
		errors.RFCCodeText("ErrRequestForwardErr"),
	)
	ErrInternalServerError = errors.Normalize(
		"internal server error",
		errors.RFCCodeText("CDC:ErrInternalServerError"),
	)
	ErrChangefeedUpdateRefused = errors.Normalize(
		"changefeed update error: %s",
		errors.RFCCodeText("CDC:ErrChangefeedUpdateRefused"),
	)
	ErrChangefeedUpdateFailedTransaction = errors.Normalize(
		"changefeed update failed due to unexpected etcd transaction failure: %s",
		errors.RFCCodeText("CDC:ErrChangefeedUpdateFailed"),
	)
	ErrUpdateServiceSafepointFailed = errors.Normalize(
		"updating service safepoint failed",
		errors.RFCCodeText("CDC:ErrUpdateServiceSafepointFailed"),
	)
	ErrStartTsBeforeGC = errors.Normalize(
		"fail to create or maintain changefeed because start-ts %d "+
			"is earlier than or equal to GC safepoint at %d",
		errors.RFCCodeText("CDC:ErrStartTsBeforeGC"),
	)
	ErrTargetTsBeforeStartTs = errors.Normalize(
		"fail to create changefeed because target-ts %d is earlier than start-ts %d",
		errors.RFCCodeText("CDC:ErrTargetTsBeforeStartTs"),
	)
	ErrSnapshotLostByGC = errors.Normalize(
		"fail to create or maintain changefeed due to snapshot loss"+
			" caused by GC. checkpoint-ts %d is earlier than or equal to GC safepoint at %d",
		errors.RFCCodeText("CDC:ErrSnapshotLostByGC"),
	)
	ErrGCTTLExceeded = errors.Normalize(
		"the checkpoint-ts(%d) lag of the changefeed(%s) has exceeded "+
			"the GC TTL and the changefeed is blocking global GC progression",
		errors.RFCCodeText("CDC:ErrGCTTLExceeded"),
	)
	ErrNotOwner = errors.Normalize(
		"this capture is not a owner",
		errors.RFCCodeText("CDC:ErrNotOwner"),
	)
	ErrOwnerNotFound = errors.Normalize(
		"owner not found",
		errors.RFCCodeText("CDC:ErrOwnerNotFound"),
	)
	ErrTableIneligible = errors.Normalize(
		"some tables are not eligible to replicate(%v), "+
			"if you want to ignore these tables, please set ignore_ineligible_table to true",
		errors.RFCCodeText("CDC:ErrTableIneligible"),
	)
	ErrInvalidCheckpointTs = errors.Normalize(
		"checkpointTs(%v) should not larger than resolvedTs(%v)",
		errors.RFCCodeText("CDC:ErrInvalidCheckpointTs"),
	)

	// EtcdWorker related errors. Internal use only.
	// ErrEtcdTryAgain is used by a PatchFunc to force a transaction abort.
	ErrEtcdTryAgain = errors.Normalize(
		"the etcd txn should be aborted and retried immediately",
		errors.RFCCodeText("CDC:ErrEtcdTryAgain"),
	)
	// ErrEtcdIgnore is used by a PatchFunc to signal that the reactor no longer wishes to update Etcd.
	ErrEtcdIgnore = errors.Normalize(
		"this patch should be excluded from the current etcd txn",
		errors.RFCCodeText("CDC:ErrEtcdIgnore"),
	)
	// ErrEtcdSessionDone is used by etcd worker to signal a session done
	ErrEtcdSessionDone = errors.Normalize(
		"the etcd session is done",
		errors.RFCCodeText("CDC:ErrEtcdSessionDone"),
	)
	// ErrReactorFinished is used by reactor to signal a **normal** exit.
	ErrReactorFinished = errors.Normalize(
		"the reactor has done its job and should no longer be executed",
		errors.RFCCodeText("CDC:ErrReactorFinished"),
	)
	ErrLeaseExpired = errors.Normalize(
		"owner lease expired ",
		errors.RFCCodeText("CDC:ErrLeaseExpired"),
	)
	ErrEtcdTxnSizeExceed = errors.Normalize(
		"patch size:%d of a single changefeed exceed etcd txn max size:%d",
		errors.RFCCodeText("CDC:ErrEtcdTxnSizeExceed"),
	)
	ErrEtcdTxnOpsExceed = errors.Normalize(
		"patch ops:%d of a single changefeed exceed etcd txn max ops:%d",
		errors.RFCCodeText("CDC:ErrEtcdTxnOpsExceed"),
	)
	ErrEtcdMigrateFailed = errors.Normalize(
		"etcd meta data migrate failed:%s",
		errors.RFCCodeText("CDC:ErrEtcdMigrateFailed"),
	)
	ErrChangefeedUnretryable = errors.Normalize(
		"changefeed is in unretryable state, please check the error message"+
			", and you should manually handle it",
		errors.RFCCodeText("CDC:ErrChangefeedUnretryable"),
	)

	// workerpool errors
	ErrWorkerPoolHandleCancelled = errors.Normalize(
		"workerpool handle is cancelled",
		errors.RFCCodeText("CDC:ErrWorkerPoolHandleCancelled"),
	)
	ErrAsyncPoolExited = errors.Normalize(
		"asyncPool has exited. Report a bug if seen externally.",
		errors.RFCCodeText("CDC:ErrAsyncPoolExited"),
	)
	ErrWorkerPoolGracefulUnregisterTimedOut = errors.Normalize(
		"workerpool handle graceful unregister timed out",
		errors.RFCCodeText("CDC:ErrWorkerPoolGracefulUnregisterTimedOut"),
	)

	// redo log related errors
	ErrConsistentStorage = errors.Normalize(
		"consistent storage (%s) not support",
		errors.RFCCodeText("CDC:ErrConsistentStorage"),
	)
	// sorter errors
	ErrIllegalSorterParameter = errors.Normalize(
		"illegal parameter for sorter: %s",
		errors.RFCCodeText("CDC:ErrIllegalSorterParameter"),
	)
	ErrConflictingFileLocks = errors.Normalize(
		"file lock conflict: %s",
		errors.RFCCodeText("ErrConflictingFileLocks"),
	)

	// retry error
	ErrReachMaxTry = errors.Normalize("reach maximum try: %s, error: %s",
		errors.RFCCodeText("CDC:ErrReachMaxTry"),
	)

	// tcp server error
	ErrTCPServerClosed = errors.Normalize("The TCP server has been closed",
		errors.RFCCodeText("CDC:ErrTCPServerClosed"),
	)

	// p2p error
	ErrPeerMessageIllegalMeta = errors.Normalize(
		"peer-to-peer message server received an RPC call with illegal metadata",
		errors.RFCCodeText("CDC:ErrPeerMessageIllegalMeta"),
	)
	ErrPeerMessageClientPermanentFail = errors.Normalize(
		"peer-to-peer message client has failed permanently, no need to reconnect: %s",
		errors.RFCCodeText("CDC:ErrPeerMessageClientPermanentFail"),
	)
	ErrPeerMessageClientClosed = errors.Normalize(
		"peer-to-peer message client has been closed",
		errors.RFCCodeText("CDC:ErrPeerMessageClientClosed"),
	)
	ErrPeerMessageSendTryAgain = errors.Normalize(
		"peer-to-peer message client has too many pending messages to send,"+
			" try again later",
		errors.RFCCodeText("CDC:ErrPeerMessageSendTryAgain"),
	)
	ErrPeerMessageEncodeError = errors.Normalize(
		"failed to encode peer-to-peer message",
		errors.RFCCodeText("CDC:ErrPeerMessageEncodeError"),
	)
	ErrPeerMessageInternalSenderClosed = errors.Normalize(
		"peer-to-peer message server tries to send to a closed stream. Internal only.",
		errors.RFCCodeText("CDC:ErrPeerMessageInternalSenderClosed"),
	)
	ErrPeerMessageStaleConnection = errors.Normalize(
		"peer-to-peer message stale connection: old-epoch %d, new-epoch %d",
		errors.RFCCodeText("CDC:ErrPeerMessageStaleConnection"),
	)
	ErrPeerMessageDuplicateConnection = errors.Normalize(
		"peer-to-peer message duplicate connection: epoch %d",
		errors.RFCCodeText("CDC:ErrPeerMessageDuplicateConnection"),
	)
	ErrPeerMessageServerClosed = errors.Normalize(
		"peer-to-peer message server has closed connection: %s.",
		errors.RFCCodeText("CDC:ErrPeerMessageServerClosed"),
	)
	ErrPeerMessageDataLost = errors.Normalize(
		"peer-to-peer message data lost, topic: %s, seq: %d",
		errors.RFCCodeText("CDC:ErrPeerMessageDataLost"),
	)
	ErrPeerMessageToManyPeers = errors.Normalize(
		"peer-to-peer message server got too many peers: %d peers",
		errors.RFCCodeText("CDC:ErrPeerMessageToManyPeers"),
	)
	ErrPeerMessageDecodeError = errors.Normalize(
		"failed to decode peer-to-peer message",
		errors.RFCCodeText("CDC:ErrPeerMessageDecodeError"),
	)
	ErrPeerMessageTaskQueueCongested = errors.Normalize(
		"peer-to-peer message server has too many pending tasks",
		errors.RFCCodeText("CDC:ErrPeerMessageTaskQueueCongested"),
	)
	ErrPeerMessageReceiverMismatch = errors.Normalize(
		"peer-to-peer message receiver is a mismatch: expected %s, got %s",
		errors.RFCCodeText("CDC:ErrPeerMessageReceiverMismatch"),
	)
	ErrPeerMessageTopicCongested = errors.Normalize(
		"peer-to-peer message topic has congested, aborting all connections",
		errors.RFCCodeText("CDC:ErrPeerMessageTopicCongested"),
	)
	ErrPeerMessageInjectedServerRestart = errors.Normalize(
		"peer-to-peer message server injected error",
		errors.RFCCodeText("CDC:ErrPeerMessageInjectedServerRestart"),
	)

	// RESTful client error
	ErrRewindRequestBodyError = errors.Normalize(
		"failed to seek to the beginning of request body",
		errors.RFCCodeText("CDC:ErrRewindRequestBodyError"),
	)
	ErrZeroLengthResponseBody = errors.Normalize(
		"0-length response with status code: %d",
		errors.RFCCodeText("CDC:ErrZeroLengthResponseBody"),
	)
	ErrInvalidHost = errors.Normalize(
		"host must be a URL or a host:port pair: %q",
		errors.RFCCodeText("CDC:ErrInvalidHost"),
	)

	// Upstream error
	ErrUpstreamNotFound = errors.Normalize(
		"upstream not found, cluster-id: %d",
		errors.RFCCodeText("CDC:ErrUpstreamNotFound"),
	)
	ErrUpstreamManagerNotReady = errors.Normalize(
		"upstream manager not ready",
		errors.RFCCodeText("CDC:ErrUpstreamManagerNotReady"),
	)
	ErrUpstreamClosed = errors.Normalize(
		"upstream has been closed",
		errors.RFCCodeText("CDC:ErrUpstreamClosed"),
	)
	ErrUpstreamHasRunningImport = errors.Normalize(
		"upstream has running import tasks, upstream-id: %d",
		errors.RFCCodeText("CDC:ErrUpstreamHasRunningImport"),
	)

	// ReplicationSet error
	ErrReplicationSetInconsistent = errors.Normalize(
		"replication set inconsistent: %s",
		errors.RFCCodeText("CDC:ErrReplicationSetInconsistent"),
	)
	ErrReplicationSetMultiplePrimaryError = errors.Normalize(
		"replication set multiple primary: %s",
		errors.RFCCodeText("CDC:ErrReplicationSetMultiplePrimaryError"),
	)

	ErrUpstreamMissMatch = errors.Normalize(
		"upstream missmatch,old: %d, new %d",
		errors.RFCCodeText("CDC:ErrUpstreamMissMatch"),
	)

	ErrServerIsNotReady = errors.Normalize(
		"cdc server is not ready",
		errors.RFCCodeText("CDC:ErrServerIsNotReady"),
	)

	// cli error
	ErrCliInvalidCheckpointTs = errors.Normalize(
		"invalid overwrite-checkpoint-ts %s, "+
			"overwrite-checkpoint-ts only accept 'now' or a valid timestamp in integer",
		errors.RFCCodeText("CDC:ErrCliInvalidCheckpointTs"),
	)
	ErrCliCheckpointTsIsInFuture = errors.Normalize(
		"the overwrite-checkpoint-ts %d must be smaller than current TSO",
		errors.RFCCodeText("CDC:ErrCliCheckpointTsIsInFuture"),
	)
	ErrCliAborted = errors.Normalize(
		"command '%s' is aborted by user",
		errors.RFCCodeText("CDC:ErrCliAborted"),
	)
	// Filter error
	ErrFailedToFilterDML = errors.Normalize(
		"failed to filter dml event: %v, please report a bug",
		errors.RFCCodeText("CDC:ErrFailedToFilterDML"),
	)
	ErrExpressionParseFailed = errors.Normalize(
		"invalid filter expressions. There is a syntax error in: '%s'",
		errors.RFCCodeText("CDC:ErrInvalidFilterExpression"),
	)
	ErrExpressionColumnNotFound = errors.Normalize(
		"invalid filter expression(s). Cannot find column '%s' from table '%s' in: %s",
		errors.RFCCodeText("CDC:ErrExpressionColumnNotFound"),
	)
	ErrInvalidIgnoreEventType = errors.Normalize(
		"invalid ignore event type: '%s'",
		errors.RFCCodeText("CDC:ErrInvalidIgnoreEventType"),
	)
	ErrConvertDDLToEventTypeFailed = errors.Normalize(
		"failed to convert ddl '%s' to filter event type",
		errors.RFCCodeText("CDC:ErrConvertDDLToEventTypeFailed"),
	)
	ErrSyncRenameTableFailed = errors.Normalize(
		"table's old name is not in filter rule, and its new name in filter rule "+
			"table id '%d', ddl query: [%s], it's an unexpected behavior, "+
			"if you want to replicate this table, please add its old name to filter rule.",
		errors.RFCCodeText("CDC:ErrSyncRenameTableFailed"),
	)

	// changefeed config error
	ErrInvalidReplicaConfig = errors.Normalize(
		"invalid replica config, %s",
		errors.RFCCodeText("CDC:ErrInvalidReplicaConfig"),
	)
	ErrInternalCheckFailed = errors.Normalize(
		"internal check failed, %s",
		errors.RFCCodeText("CDC:ErrInternalCheckFailed"),
	)

	ErrHandleDDLFailed = errors.Normalize(
		"handle ddl failed, job: %s, query: %s, startTs: %d. "+
			"If you want to skip this DDL and continue with replication, "+
			"you can manually execute this DDL downstream. Afterwards, "+
			"add `ignore-txn-start-ts=[%d]` to the changefeed in the filter configuration.",
		errors.RFCCodeText("CDC:ErrHandleDDLFailed"),
	)

	ErrInvalidGlueSchemaRegistryConfig = errors.Normalize(
		"invalid glue schema registry config, %s",
		errors.RFCCodeText("CDC:ErrInvalidGlueSchemaRegistryConfig"),
	)

	// cdc v2
	// TODO(CharlesCheung): refactor this errors
	ErrElectorNotLeader = errors.Normalize(
		"%s is not leader",
		errors.RFCCodeText("CDC:ErrNotLeader"),
	)
	ErrNotController = errors.Normalize(
		"not controller",
		errors.RFCCodeText("CDC:ErrNotController"),
	)
	ErrMetaRowsAffectedNotMatch = errors.Normalize(
		"rows affected by the operation %s is unexpected: expected %d, got %d",
		errors.RFCCodeText("CDC:ErrMetaOpIgnored"),
	)
	ErrMetaOpFailed = errors.Normalize(
		"unexpected meta operation failure: %s",
		errors.RFCCodeText("DFLOW:ErrMetaOpFailed"),
	)
	ErrMetaInvalidState = errors.Normalize(
		"meta state is invalid: %s",
		errors.RFCCodeText("DFLOW:ErrMetaInvalidState"),
	)
	ErrInconsistentMetaCache = errors.Normalize(
		"meta cache is inconsistent: %s",
		errors.RFCCodeText("DFLOW:ErrInconsistentMetaCache"),
	)
)
