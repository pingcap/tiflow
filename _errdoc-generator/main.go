
package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"os"
	"reflect"
	"fmt"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	pkg_errors "github.com/pingcap/ticdc/pkg/errors"
	proto_benchmark "github.com/pingcap/ticdc/proto/benchmark"
	proto_canal "github.com/pingcap/ticdc/proto/canal"
)

func main() {
	var outpath string
	flag.StringVar(&outpath, "output", "", "Specify the error documentation output file path")
	flag.Parse()
	if outpath == "" {
		println("Usage: ./_errdoc-generator --output /path/to/errors.toml")
		os.Exit(1)
	}

	// Read-in the exists file and merge the description/workaround from exists file
	existDefinition := map[string]spec{}
	if file, err := ioutil.ReadFile(outpath); err == nil {
		err = toml.Unmarshal(file, &existDefinition)
		if err != nil {
			println(fmt.Sprintf("Invalid toml file %s when merging exists description/workaround: %v", outpath, err))
			os.Exit(1)
		}
	}
	
	var allErrors []error
	allErrors = append(allErrors, pkg_errors.ErrWriteTsConflict)
	allErrors = append(allErrors, pkg_errors.ErrChangeFeedNotExists)
	allErrors = append(allErrors, pkg_errors.ErrChangeFeedAlreadyExists)
	allErrors = append(allErrors, pkg_errors.ErrTaskStatusNotExists)
	allErrors = append(allErrors, pkg_errors.ErrTaskPositionNotExists)
	allErrors = append(allErrors, pkg_errors.ErrCaptureNotExist)
	allErrors = append(allErrors, pkg_errors.ErrGetAllStoresFailed)
	allErrors = append(allErrors, pkg_errors.ErrMetaListDatabases)
	allErrors = append(allErrors, pkg_errors.ErrGRPCDialFailed)
	allErrors = append(allErrors, pkg_errors.ErrTiKVEventFeed)
	allErrors = append(allErrors, pkg_errors.ErrPDBatchLoadRegions)
	allErrors = append(allErrors, pkg_errors.ErrMetaNotInRegion)
	allErrors = append(allErrors, pkg_errors.ErrRegionsNotCoverSpan)
	allErrors = append(allErrors, pkg_errors.ErrGetTiKVRPCContext)
	allErrors = append(allErrors, pkg_errors.ErrPendingRegionCancel)
	allErrors = append(allErrors, pkg_errors.ErrEventFeedAborted)
	allErrors = append(allErrors, pkg_errors.ErrUnknownKVEventType)
	allErrors = append(allErrors, pkg_errors.ErrNoPendingRegion)
	allErrors = append(allErrors, pkg_errors.ErrPrewriteNotMatch)
	allErrors = append(allErrors, pkg_errors.ErrGetRegionFailed)
	allErrors = append(allErrors, pkg_errors.ErrScanLockFailed)
	allErrors = append(allErrors, pkg_errors.ErrResolveLocks)
	allErrors = append(allErrors, pkg_errors.ErrLocateRegion)
	allErrors = append(allErrors, pkg_errors.ErrKVStorageSendReq)
	allErrors = append(allErrors, pkg_errors.ErrKVStorageRegionError)
	allErrors = append(allErrors, pkg_errors.ErrKVStorageBackoffFailed)
	allErrors = append(allErrors, pkg_errors.ErrKVStorageRespEmpty)
	allErrors = append(allErrors, pkg_errors.ErrEventFeedEventError)
	allErrors = append(allErrors, pkg_errors.ErrPDEtcdAPIError)
	allErrors = append(allErrors, pkg_errors.ErrCachedTSONotExists)
	allErrors = append(allErrors, pkg_errors.ErrGetStoreSnapshot)
	allErrors = append(allErrors, pkg_errors.ErrNewStore)
	allErrors = append(allErrors, pkg_errors.ErrRegionWorkerExit)
	allErrors = append(allErrors, pkg_errors.ErrEncodeFailed)
	allErrors = append(allErrors, pkg_errors.ErrDecodeFailed)
	allErrors = append(allErrors, pkg_errors.ErrFilterRuleInvalid)
	allErrors = append(allErrors, pkg_errors.ErrAdminStopProcessor)
	allErrors = append(allErrors, pkg_errors.ErrVersionIncompatible)
	allErrors = append(allErrors, pkg_errors.ErrClusterIDMismatch)
	allErrors = append(allErrors, pkg_errors.ErrCreateMarkTableFailed)
	allErrors = append(allErrors, pkg_errors.ErrExecDDLFailed)
	allErrors = append(allErrors, pkg_errors.ErrEmitCheckpointTsFailed)
	allErrors = append(allErrors, pkg_errors.ErrDDLEventIgnored)
	allErrors = append(allErrors, pkg_errors.ErrKafkaSendMessage)
	allErrors = append(allErrors, pkg_errors.ErrKafkaAsyncSendMessage)
	allErrors = append(allErrors, pkg_errors.ErrKafkaFlushUnfinished)
	allErrors = append(allErrors, pkg_errors.ErrKafkaInvalidPartitionNum)
	allErrors = append(allErrors, pkg_errors.ErrKafkaNewSaramaProducer)
	allErrors = append(allErrors, pkg_errors.ErrKafkaInvalidClientID)
	allErrors = append(allErrors, pkg_errors.ErrKafkaInvalidVersion)
	allErrors = append(allErrors, pkg_errors.ErrPulsarNewProducer)
	allErrors = append(allErrors, pkg_errors.ErrPulsarSendMessage)
	allErrors = append(allErrors, pkg_errors.ErrFileSinkCreateDir)
	allErrors = append(allErrors, pkg_errors.ErrFileSinkFileOp)
	allErrors = append(allErrors, pkg_errors.ErrRedoConfigInvalid)
	allErrors = append(allErrors, pkg_errors.ErrRedoWriterStopped)
	allErrors = append(allErrors, pkg_errors.ErrRedoFileOp)
	allErrors = append(allErrors, pkg_errors.ErrRedoMetaInitialize)
	allErrors = append(allErrors, pkg_errors.ErrFileSizeExceed)
	allErrors = append(allErrors, pkg_errors.ErrFileSinkMetaAlreadyExists)
	allErrors = append(allErrors, pkg_errors.ErrS3SinkWriteStorage)
	allErrors = append(allErrors, pkg_errors.ErrS3SinkInitialize)
	allErrors = append(allErrors, pkg_errors.ErrS3SinkStorageAPI)
	allErrors = append(allErrors, pkg_errors.ErrS3StorageAPI)
	allErrors = append(allErrors, pkg_errors.ErrS3StorageInitialize)
	allErrors = append(allErrors, pkg_errors.ErrPrepareAvroFailed)
	allErrors = append(allErrors, pkg_errors.ErrAsyncBroadcastNotSupport)
	allErrors = append(allErrors, pkg_errors.ErrKafkaInvalidConfig)
	allErrors = append(allErrors, pkg_errors.ErrSinkURIInvalid)
	allErrors = append(allErrors, pkg_errors.ErrMySQLTxnError)
	allErrors = append(allErrors, pkg_errors.ErrMySQLQueryError)
	allErrors = append(allErrors, pkg_errors.ErrMySQLConnectionError)
	allErrors = append(allErrors, pkg_errors.ErrMySQLInvalidConfig)
	allErrors = append(allErrors, pkg_errors.ErrMySQLWorkerPanic)
	allErrors = append(allErrors, pkg_errors.ErrAvroToEnvelopeError)
	allErrors = append(allErrors, pkg_errors.ErrAvroUnknownType)
	allErrors = append(allErrors, pkg_errors.ErrAvroMarshalFailed)
	allErrors = append(allErrors, pkg_errors.ErrAvroEncodeFailed)
	allErrors = append(allErrors, pkg_errors.ErrAvroEncodeToBinary)
	allErrors = append(allErrors, pkg_errors.ErrAvroSchemaAPIError)
	allErrors = append(allErrors, pkg_errors.ErrMaxwellEncodeFailed)
	allErrors = append(allErrors, pkg_errors.ErrMaxwellDecodeFailed)
	allErrors = append(allErrors, pkg_errors.ErrMaxwellInvalidData)
	allErrors = append(allErrors, pkg_errors.ErrJSONCodecInvalidData)
	allErrors = append(allErrors, pkg_errors.ErrCanalDecodeFailed)
	allErrors = append(allErrors, pkg_errors.ErrCanalEncodeFailed)
	allErrors = append(allErrors, pkg_errors.ErrOldValueNotEnabled)
	allErrors = append(allErrors, pkg_errors.ErrSinkInvalidConfig)
	allErrors = append(allErrors, pkg_errors.ErrCraftCodecInvalidData)
	allErrors = append(allErrors, pkg_errors.ErrToTLSConfigFailed)
	allErrors = append(allErrors, pkg_errors.ErrCheckClusterVersionFromPD)
	allErrors = append(allErrors, pkg_errors.ErrNewSemVersion)
	allErrors = append(allErrors, pkg_errors.ErrCheckDirWritable)
	allErrors = append(allErrors, pkg_errors.ErrCheckDirReadable)
	allErrors = append(allErrors, pkg_errors.ErrCheckDirValid)
	allErrors = append(allErrors, pkg_errors.ErrGetDiskInfo)
	allErrors = append(allErrors, pkg_errors.ErrCheckDataDirSatisfied)
	allErrors = append(allErrors, pkg_errors.ErrLoadTimezone)
	allErrors = append(allErrors, pkg_errors.ErrURLFormatInvalid)
	allErrors = append(allErrors, pkg_errors.ErrIntersectNoOverlap)
	allErrors = append(allErrors, pkg_errors.ErrOperateOnClosedNotifier)
	allErrors = append(allErrors, pkg_errors.ErrInvalidRecordKey)
	allErrors = append(allErrors, pkg_errors.ErrCodecDecode)
	allErrors = append(allErrors, pkg_errors.ErrUnknownMetaType)
	allErrors = append(allErrors, pkg_errors.ErrFetchHandleValue)
	allErrors = append(allErrors, pkg_errors.ErrDatumUnflatten)
	allErrors = append(allErrors, pkg_errors.ErrWrongTableInfo)
	allErrors = append(allErrors, pkg_errors.ErrIndexKeyTableNotFound)
	allErrors = append(allErrors, pkg_errors.ErrDecodeRowToDatum)
	allErrors = append(allErrors, pkg_errors.ErrMarshalFailed)
	allErrors = append(allErrors, pkg_errors.ErrUnmarshalFailed)
	allErrors = append(allErrors, pkg_errors.ErrInvalidChangefeedID)
	allErrors = append(allErrors, pkg_errors.ErrInvalidEtcdKey)
	allErrors = append(allErrors, pkg_errors.ErrSchemaStorageUnresolved)
	allErrors = append(allErrors, pkg_errors.ErrSchemaStorageGCed)
	allErrors = append(allErrors, pkg_errors.ErrSchemaSnapshotNotFound)
	allErrors = append(allErrors, pkg_errors.ErrSchemaStorageTableMiss)
	allErrors = append(allErrors, pkg_errors.ErrSnapshotSchemaNotFound)
	allErrors = append(allErrors, pkg_errors.ErrSnapshotTableNotFound)
	allErrors = append(allErrors, pkg_errors.ErrSnapshotSchemaExists)
	allErrors = append(allErrors, pkg_errors.ErrSnapshotTableExists)
	allErrors = append(allErrors, pkg_errors.ErrBufferReachLimit)
	allErrors = append(allErrors, pkg_errors.ErrCaptureSuicide)
	allErrors = append(allErrors, pkg_errors.ErrNewCaptureFailed)
	allErrors = append(allErrors, pkg_errors.ErrCaptureRegister)
	allErrors = append(allErrors, pkg_errors.ErrNewProcessorFailed)
	allErrors = append(allErrors, pkg_errors.ErrProcessorUnknown)
	allErrors = append(allErrors, pkg_errors.ErrOwnerUnknown)
	allErrors = append(allErrors, pkg_errors.ErrProcessorTableNotFound)
	allErrors = append(allErrors, pkg_errors.ErrProcessorEtcdWatch)
	allErrors = append(allErrors, pkg_errors.ErrProcessorSortDir)
	allErrors = append(allErrors, pkg_errors.ErrUnknownSortEngine)
	allErrors = append(allErrors, pkg_errors.ErrInvalidTaskKey)
	allErrors = append(allErrors, pkg_errors.ErrInvalidServerOption)
	allErrors = append(allErrors, pkg_errors.ErrServerNewPDClient)
	allErrors = append(allErrors, pkg_errors.ErrServeHTTP)
	allErrors = append(allErrors, pkg_errors.ErrCaptureCampaignOwner)
	allErrors = append(allErrors, pkg_errors.ErrCaptureResignOwner)
	allErrors = append(allErrors, pkg_errors.ErrWaitHandleOperationTimeout)
	allErrors = append(allErrors, pkg_errors.ErrSupportPostOnly)
	allErrors = append(allErrors, pkg_errors.ErrSupportGetOnly)
	allErrors = append(allErrors, pkg_errors.ErrAPIInvalidParam)
	allErrors = append(allErrors, pkg_errors.ErrRequestForwardErr)
	allErrors = append(allErrors, pkg_errors.ErrInternalServerError)
	allErrors = append(allErrors, pkg_errors.ErrOwnerSortDir)
	allErrors = append(allErrors, pkg_errors.ErrOwnerChangefeedNotFound)
	allErrors = append(allErrors, pkg_errors.ErrChangefeedUpdateRefused)
	allErrors = append(allErrors, pkg_errors.ErrChangefeedAbnormalState)
	allErrors = append(allErrors, pkg_errors.ErrInvalidAdminJobType)
	allErrors = append(allErrors, pkg_errors.ErrOwnerEtcdWatch)
	allErrors = append(allErrors, pkg_errors.ErrOwnerCampaignKeyDeleted)
	allErrors = append(allErrors, pkg_errors.ErrServiceSafepointLost)
	allErrors = append(allErrors, pkg_errors.ErrUpdateServiceSafepointFailed)
	allErrors = append(allErrors, pkg_errors.ErrStartTsBeforeGC)
	allErrors = append(allErrors, pkg_errors.ErrTargetTsBeforeStartTs)
	allErrors = append(allErrors, pkg_errors.ErrSnapshotLostByGC)
	allErrors = append(allErrors, pkg_errors.ErrGCTTLExceeded)
	allErrors = append(allErrors, pkg_errors.ErrNotOwner)
	allErrors = append(allErrors, pkg_errors.ErrOwnerNotFound)
	allErrors = append(allErrors, pkg_errors.ErrTableListenReplicated)
	allErrors = append(allErrors, pkg_errors.ErrTableIneligible)
	allErrors = append(allErrors, pkg_errors.ErrEtcdTryAgain)
	allErrors = append(allErrors, pkg_errors.ErrEtcdIgnore)
	allErrors = append(allErrors, pkg_errors.ErrEtcdSessionDone)
	allErrors = append(allErrors, pkg_errors.ErrReactorFinished)
	allErrors = append(allErrors, pkg_errors.ErrLeaseTimeout)
	allErrors = append(allErrors, pkg_errors.ErrLeaseExpired)
	allErrors = append(allErrors, pkg_errors.ErrSendToClosedPipeline)
	allErrors = append(allErrors, pkg_errors.ErrPipelineTryAgain)
	allErrors = append(allErrors, pkg_errors.ErrActorDuplicate)
	allErrors = append(allErrors, pkg_errors.ErrActorNotFound)
	allErrors = append(allErrors, pkg_errors.ErrActorStopped)
	allErrors = append(allErrors, pkg_errors.ErrMailboxFull)
	allErrors = append(allErrors, pkg_errors.ErrWorkerPoolHandleCancelled)
	allErrors = append(allErrors, pkg_errors.ErrAsyncPoolExited)
	allErrors = append(allErrors, pkg_errors.ErrUnifiedSorterBackendTerminating)
	allErrors = append(allErrors, pkg_errors.ErrIllegalUnifiedSorterParameter)
	allErrors = append(allErrors, pkg_errors.ErrAsyncIOCancelled)
	allErrors = append(allErrors, pkg_errors.ErrUnifiedSorterIOError)
	allErrors = append(allErrors, pkg_errors.ErrConflictingFileLocks)
	allErrors = append(allErrors, pkg_errors.ErrSortDirLockError)
	allErrors = append(allErrors, pkg_errors.ErrTableProcessorStoppedSafely)
	allErrors = append(allErrors, pkg_errors.ErrOwnerChangedUnexpectedly)
	allErrors = append(allErrors, pkg_errors.ErrOwnerInconsistentStates)
	allErrors = append(allErrors, pkg_errors.ErrFlowControllerAborted)
	allErrors = append(allErrors, pkg_errors.ErrFlowControllerEventLargerThanQuota)
	allErrors = append(allErrors, pkg_errors.ErrReachMaxTry)
	allErrors = append(allErrors, proto_benchmark.ErrInvalidLengthCraftBenchmark)
	allErrors = append(allErrors, proto_benchmark.ErrIntOverflowCraftBenchmark)
	allErrors = append(allErrors, proto_benchmark.ErrUnexpectedEndOfGroupCraftBenchmark)
	allErrors = append(allErrors, proto_canal.ErrInvalidLengthCanalProtocol)
	allErrors = append(allErrors, proto_canal.ErrIntOverflowCanalProtocol)
	allErrors = append(allErrors, proto_canal.ErrUnexpectedEndOfGroupCanalProtocol)
	allErrors = append(allErrors, proto_canal.ErrInvalidLengthEntryProtocol)
	allErrors = append(allErrors, proto_canal.ErrIntOverflowEntryProtocol)
	allErrors = append(allErrors, proto_canal.ErrUnexpectedEndOfGroupEntryProtocol)

	var dedup = map[string]spec{}
	for _, e := range allErrors {
		terr, ok := e.(*errors.Error)
		if !ok {
			println("Non-normalized error:", e.Error())
		} else {
			val := reflect.ValueOf(terr).Elem()
			codeText := val.FieldByName("codeText")
			message := val.FieldByName("message")
			if previous, found := dedup[codeText.String()]; found {
				println("Duplicated error code:", codeText.String())
				if message.String() < previous.Error {
					continue
				}
			}
			s := spec{
				Code:  codeText.String(),
				Error: message.String(),
			}
			if exist, found := existDefinition[s.Code]; found {
				s.Description = strings.TrimSpace(exist.Description)
				s.Workaround = strings.TrimSpace(exist.Workaround)
			}
			dedup[codeText.String()] = s
		}
	}

	var sorted []spec
	for _, item := range dedup {
		sorted = append(sorted, item)
	}
	sort.Slice(sorted, func(i, j int) bool {
		// TiDB exits duplicated code
		if sorted[i].Code == sorted[j].Code {
			return sorted[i].Error < sorted[j].Error
		}
		return sorted[i].Code < sorted[j].Code
	})

	// We don't use toml library to serialize it due to cannot reserve the order for map[string]spec
	buffer := bytes.NewBufferString("# AUTOGENERATED BY github.com/pingcap/errors/errdoc-gen\n" +
		"# YOU CAN CHANGE THE 'description'/'workaround' FIELDS IF THEM ARE IMPROPER.\n\n")
	for _, item := range sorted {
		buffer.WriteString(fmt.Sprintf("[\"%s\"]\nerror = '''\n%s\n'''\n", item.Code, item.Error))
		if item.Description != "" {
			buffer.WriteString(fmt.Sprintf("description = '''\n%s\n'''\n", item.Description))
		}
		if item.Workaround != "" {
			buffer.WriteString(fmt.Sprintf("workaround = '''\n%s\n'''\n", item.Workaround))
		}
		buffer.WriteString("\n")
	}
	if err := ioutil.WriteFile(outpath, buffer.Bytes(), os.ModePerm); err != nil {
		panic(err)
	}
}
type spec struct {
Code        string
Error       string `toml:"error"`
Description string `toml:"description"`
Workaround  string `toml:"workaround"`
}