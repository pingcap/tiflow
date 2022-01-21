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

package entry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// The buffer size of input channel of each mounter worker.
	// 16 is large enough, because a channel exclusively belongs to a worker.
	defaultInputChanSize = 16
)

type baseKVEntry struct {
	StartTs uint64
	// Commit or resolved TS
	CRTs uint64

	PhysicalTableID int64
	RecordID        kv.Handle
	Delete          bool
}

type rowKVEntry struct {
	baseKVEntry
	Row    map[int64]types.Datum
	PreRow map[int64]types.Datum

	// In some cases, row data may exist but not contain any Datum,
	// use this RowExist/PreRowExist variable to distinguish between row data that does not exist
	// or row data that does not contain any Datum.
	RowExist    bool
	PreRowExist bool
}

// Mounter is used to parse SQL events from KV events
type Mounter interface {
	Run(ctx context.Context) error
	// AddEntry accepts `model.PolymorphicEvent` with `RawKVEntry` filled and
	// decodes `RawKVEntry` into `RowChangedEvent`.
	// It also close `model.PolymorphicEvent.finished` channel to notify callers
	// that decoding is done.
	AddEntry(ctx context.Context, event *model.PolymorphicEvent) error
}

type mounterImpl struct {
	schemaStorage    SchemaStorage
	rawRowChangedChs []chan *model.PolymorphicEvent
	tz               *time.Location
	workerNum        int
	enableOldValue   bool

	// index is an atomic variable to dispatch input events to workers.
	index int64
}

// NewMounter creates a mounter
func NewMounter(schemaStorage SchemaStorage, workerNum int, enableOldValue bool) Mounter {
	if workerNum <= 0 {
		workerNum = defaultMounterWorkerNum
	}
	chs := make([]chan *model.PolymorphicEvent, workerNum)
	for i := 0; i < workerNum; i++ {
		chs[i] = make(chan *model.PolymorphicEvent, defaultInputChanSize)
	}
	return &mounterImpl{
		schemaStorage:    schemaStorage,
		rawRowChangedChs: chs,
		workerNum:        workerNum,
		enableOldValue:   enableOldValue,
	}
}

const defaultMounterWorkerNum = 32

func (m *mounterImpl) Run(ctx context.Context) error {
	m.tz = util.TimezoneFromCtx(ctx)
	errg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < m.workerNum; i++ {
		index := i
		errg.Go(func() error {
			return m.codecWorker(ctx, index)
		})
	}

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricMounterInputChanSize := mounterInputChanSizeGauge.WithLabelValues(captureAddr, changefeedID)

	flushMetricsInterval := 15 * time.Second
	timer := time.NewTimer(flushMetricsInterval)
	defer timer.Stop()
	for {
		select {
		// ctx.Done returns when parent ctx done or error occurs in errg.
		case <-ctx.Done():
			return errg.Wait()
		case <-timer.C:
			chSize := 0
			for _, ch := range m.rawRowChangedChs {
				chSize += len(ch)
			}
			metricMounterInputChanSize.Set(float64(chSize))
			timer.Reset(flushMetricsInterval)
		}
	}
}

func (m *mounterImpl) codecWorker(ctx context.Context, index int) error {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricMountDuration := mountDuration.WithLabelValues(captureAddr, changefeedID)
	metricTotalRows := totalRowsCountGauge.WithLabelValues(captureAddr, changefeedID)
	defer func() {
		mountDuration.DeleteLabelValues(captureAddr, changefeedID)
		totalRowsCountGauge.DeleteLabelValues(captureAddr, changefeedID)
	}()

	for {
		var pEvent *model.PolymorphicEvent
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case pEvent = <-m.rawRowChangedChs[index]:
		}
		if pEvent.RawKV.OpType == model.OpTypeResolved {
			pEvent.PrepareFinished()
			continue
		}
		startTime := time.Now()
		rowEvent, err := m.unmarshalAndMountRowChanged(ctx, pEvent.RawKV)
		if err != nil {
			return errors.Trace(err)
		}
		pEvent.Row = rowEvent
		pEvent.RawKV.Value = nil
		pEvent.RawKV.OldValue = nil
		pEvent.PrepareFinished()
		metricMountDuration.Observe(time.Since(startTime).Seconds())
		metricTotalRows.Inc()
	}
}

func (m *mounterImpl) AddEntry(ctx context.Context, event *model.PolymorphicEvent) error {
	index := atomic.AddInt64(&m.index, 1) % int64(m.workerNum)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.rawRowChangedChs[index] <- event:
		return nil
	}
}

func (m *mounterImpl) unmarshalAndMountRowChanged(ctx context.Context, raw *model.RawKVEntry) (*model.RowChangedEvent, error) {
	if !bytes.HasPrefix(raw.Key, tablePrefix) {
		return nil, nil
	}
	key, physicalTableID, err := decodeTableID(raw.Key)
	if err != nil {
		return nil, err
	}
	baseInfo := baseKVEntry{
		StartTs:         raw.StartTs,
		CRTs:            raw.CRTs,
		PhysicalTableID: physicalTableID,
		Delete:          raw.OpType == model.OpTypeDelete,
	}
	// when async commit is enabled, the commitTs of DMLs may be equals with DDL finishedTs
	// a DML whose commitTs is equal to a DDL finishedTs using the schema info before the DDL
	snap, err := m.schemaStorage.GetSnapshot(ctx, raw.CRTs-1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	row, err := func() (*model.RowChangedEvent, error) {
		if snap.IsIneligibleTableID(physicalTableID) {
			log.Debug("skip the DML of ineligible table", zap.Uint64("ts", raw.CRTs), zap.Int64("tableID", physicalTableID))
			return nil, nil
		}
		tableInfo, exist := snap.PhysicalTableByID(physicalTableID)
		if !exist {
			if snap.IsTruncateTableID(physicalTableID) {
				log.Debug("skip the DML of truncated table", zap.Uint64("ts", raw.CRTs), zap.Int64("tableID", physicalTableID))
				return nil, nil
			}
			return nil, cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(physicalTableID)
		}
		if bytes.HasPrefix(key, recordPrefix) {
			rowKV, err := m.unmarshalRowKVEntry(tableInfo, raw.Key, raw.Value, raw.OldValue, baseInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if rowKV == nil {
				return nil, nil
			}
			return m.mountRowKVEntry(tableInfo, rowKV, raw.ApproximateDataSize())
		}
		return nil, nil
	}()
	if err != nil {
		log.Error("failed to mount and unmarshals entry, start to print debug info", zap.Error(err))
		snap.PrintStatus(log.Error)
	}
	return row, err
}

func (m *mounterImpl) unmarshalRowKVEntry(tableInfo *model.TableInfo, rawKey []byte, rawValue []byte, rawOldValue []byte, base baseKVEntry) (*rowKVEntry, error) {
	recordID, err := tablecodec.DecodeRowKey(rawKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	decodeRow := func(rawColValue []byte) (map[int64]types.Datum, bool, error) {
		if len(rawColValue) == 0 {
			return nil, false, nil
		}
		row, err := decodeRow(rawColValue, recordID, tableInfo, m.tz)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		return row, true, nil
	}

	row, rowExist, err := decodeRow(rawValue)
	if err != nil {
		return nil, errors.Trace(err)
	}
	preRow, preRowExist, err := decodeRow(rawOldValue)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if base.Delete && !m.enableOldValue && (tableInfo.PKIsHandle || tableInfo.IsCommonHandle) {
		handleColIDs, fieldTps, _ := tableInfo.GetRowColInfos()
		preRow, err = tablecodec.DecodeHandleToDatumMap(recordID, handleColIDs, fieldTps, m.tz, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		preRowExist = true
	}

	base.RecordID = recordID
	return &rowKVEntry{
		baseKVEntry: base,
		Row:         row,
		PreRow:      preRow,
		RowExist:    rowExist,
		PreRowExist: preRowExist,
	}, nil
}

const (
	ddlJobListKey         = "DDLJobList"
	ddlAddIndexJobListKey = "DDLJobAddIdxList"
)

// UnmarshalDDL unmarshals the ddl job from RawKVEntry
func UnmarshalDDL(raw *model.RawKVEntry) (*timodel.Job, error) {
	if raw.OpType != model.OpTypePut || !bytes.HasPrefix(raw.Key, metaPrefix) {
		return nil, nil
	}
	meta, err := decodeMetaKey(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if meta.getType() != ListData {
		return nil, nil
	}
	k := meta.(metaListData)
	if k.key != ddlJobListKey && k.key != ddlAddIndexJobListKey {
		return nil, nil
	}
	job := &timodel.Job{}
	err = json.Unmarshal(raw.Value, job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get new DDL job", zap.String("detail", job.String()))
	if !job.IsDone() && !job.IsSynced() {
		return nil, nil
	}
	// FinishedTS is only set when the job is synced,
	// but we can use the entry's ts here
	job.StartTS = raw.StartTs
	job.BinlogInfo.FinishedTS = raw.CRTs
	return job, nil
}

func datum2Column(tableInfo *model.TableInfo, datums map[int64]types.Datum, fillWithDefaultValue bool) ([]*model.Column, error) {
	cols := make([]*model.Column, len(tableInfo.RowColumnsOffset))
	for _, colInfo := range tableInfo.Columns {
		colSize := 0
		if !model.IsColCDCVisible(colInfo) {
			continue
		}
		colName := colInfo.Name.O
		colDatums, exist := datums[colInfo.ID]
		var colValue interface{}
		if !exist && !fillWithDefaultValue {
			continue
		}
		var err error
		var warn string
		var size int
		if exist {
			colValue, size, warn, err = formatColVal(colDatums, colInfo.Tp)
		} else if fillWithDefaultValue {
			colValue, size, warn, err = getDefaultOrZeroValue(colInfo)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if warn != "" {
			log.Warn(warn, zap.String("table", tableInfo.TableName.String()), zap.String("column", colInfo.Name.String()))
		}
		colSize += size
		cols[tableInfo.RowColumnsOffset[colInfo.ID]] = &model.Column{
			Name:  colName,
			Type:  colInfo.Tp,
			Value: colValue,
			Flag:  tableInfo.ColumnsFlag[colInfo.ID],
			// ApproximateBytes = column data size + column struct size
			ApproximateBytes: colSize + sizeOfEmptyColumn,
		}
	}
	return cols, nil
}

func (m *mounterImpl) mountRowKVEntry(tableInfo *model.TableInfo, row *rowKVEntry, dataSize int64) (*model.RowChangedEvent, error) {
	var err error
	// Decode previous columns.
	var preCols []*model.Column
	// Since we now always use old value internally,
	// we need to control the output(sink will use the PreColumns field to determine whether to output old value).
	// Normally old value is output when only enableOldValue is on,
	// but for the Delete event, when the old value feature is off,
	// the HandleKey column needs to be included as well. So we need to do the following filtering.
	if row.PreRowExist {
		// FIXME(leoppro): using pre table info to mounter pre column datum
		// the pre column and current column in one event may using different table info
		preCols, err = datum2Column(tableInfo, row.PreRow, m.enableOldValue)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// NOTICE: When the old Value feature is off,
		// the Delete event only needs to keep the handle key column.
		if row.Delete && !m.enableOldValue {
			for i := range preCols {
				col := preCols[i]
				if col != nil && !col.Flag.IsHandleKey() {
					preCols[i] = nil
				}
			}
		}
	}

	var cols []*model.Column
	if row.RowExist {
		cols, err = datum2Column(tableInfo, row.Row, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	schemaName := tableInfo.TableName.Schema
	tableName := tableInfo.TableName.Table
	var intRowID int64
	if row.RecordID.IsInt() {
		intRowID = row.RecordID.IntValue()
	}

	var tableInfoVersion uint64
	// Align with the old format if old value disabled.
	if row.Delete && !m.enableOldValue {
		tableInfoVersion = 0
	} else {
		tableInfoVersion = tableInfo.TableInfoVersion
	}

	return &model.RowChangedEvent{
		StartTs:          row.StartTs,
		CommitTs:         row.CRTs,
		RowID:            intRowID,
		TableInfoVersion: tableInfoVersion,
		Table: &model.TableName{
			Schema:      schemaName,
			Table:       tableName,
			TableID:     row.PhysicalTableID,
			IsPartition: tableInfo.GetPartitionInfo() != nil,
		},
		Columns:             cols,
		PreColumns:          preCols,
		IndexColumns:        tableInfo.IndexColumnsOffset,
		ApproximateDataSize: dataSize,
	}, nil
}

var emptyBytes = make([]byte, 0)

const (
	sizeOfEmptyColumn = int(unsafe.Sizeof(model.Column{}))
	sizeOfEmptyBytes  = int(unsafe.Sizeof(emptyBytes))
	sizeOfEmptyString = int(unsafe.Sizeof(""))
)

func sizeOfDatum(d types.Datum) int {
	array := [...]types.Datum{d}
	return int(types.EstimatedMemUsage(array[:], 1))
}

func sizeOfString(s string) int {
	// string data size + string struct size.
	return len(s) + sizeOfEmptyString
}

func sizeOfBytes(b []byte) int {
	// bytes data size + bytes struct size.
	return len(b) + sizeOfEmptyBytes
}

// formatColVal return interface{} need to meet the same requirement as getDefaultOrZeroValue
func formatColVal(datum types.Datum, tp byte) (
	value interface{}, size int, warn string, err error,
) {
	if datum.IsNull() {
		return nil, 0, "", nil
	}
	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		v := datum.GetMysqlTime().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeDuration:
		v := datum.GetMysqlDuration().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeJSON:
		v := datum.GetMysqlJSON().String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeNewDecimal:
		d := datum.GetMysqlDecimal()
		if d == nil {
			// nil takes 0 byte.
			return nil, 0, "", nil
		}
		v := d.String()
		return v, sizeOfString(v), "", nil
	case mysql.TypeEnum:
		v := datum.GetMysqlEnum().Value
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", nil
	case mysql.TypeSet:
		v := datum.GetMysqlSet().Value
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", nil
	case mysql.TypeBit:
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		v, err := datum.GetBinaryLiteral().ToInt(nil)
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), "", err
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		b := datum.GetBytes()
		if b == nil {
			b = emptyBytes
		}
		return b, sizeOfBytes(b), "", nil
	case mysql.TypeFloat, mysql.TypeDouble:
		v := datum.GetFloat64()
		if math.IsNaN(v) || math.IsInf(v, 1) || math.IsInf(v, -1) {
			warn = fmt.Sprintf("the value is invalid in column: %f", v)
			v = 0
		}
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), warn, nil
	default:
		// NOTICE: GetValue() may return some types that go sql not support, which will cause sink DML fail
		// Make specified convert upper if you need
		// Go sql support type ref to: https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
		return datum.GetValue(), sizeOfDatum(datum), "", nil
	}
}

// Scenarios when call this function:
// (1) column define default null at creating + insert without explicit column
// (2) alter table add column default xxx + old existing data
// (3) amend + insert without explicit column + alter table add column default xxx
// (4) online DDL drop column + data insert at state delete-only
//
// getDefaultOrZeroValue return interface{} need to meet to require type in
// https://github.com/golang/go/blob/go1.17.4/src/database/sql/driver/types.go#L236
// Supported type is: nil, basic type(Int, Int8,..., Float32, Float64, String), Slice(uint8), other types not support
// TODO: Check default expr support
func getDefaultOrZeroValue(col *timodel.ColumnInfo) (interface{}, int, string, error) {
	var d types.Datum
	// NOTICE: SHOULD use OriginDefaultValue here, more info pls ref to
	// https://github.com/pingcap/tiflow/issues/4048
	// FIXME: Too many corner cases may hit here, like type truncate, timezone
	// (1) If this column is uk(no pk), will cause data inconsistency in Scenarios(2)
	// (2) If not fix here, will cause data inconsistency in Scenarios(3) directly
	// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
	if col.GetOriginDefaultValue() != nil {
		d = types.NewDatum(col.GetOriginDefaultValue())
		return d.GetValue(), sizeOfDatum(d), "", nil
	}

	if !mysql.HasNotNullFlag(col.Flag) {
		// NOTICE: NotNullCheck need do after OriginDefaultValue check, as when TiDB meet "amend + add column default xxx",
		// ref: https://github.com/pingcap/ticdc/issues/3929
		// must use null if TiDB not write the column value when default value is null
		// and the value is null, see https://github.com/pingcap/tidb/issues/9304
		d = types.NewDatum(nil)
	} else {
		switch col.Tp {
		case mysql.TypeEnum:
			// For enum type, if no default value and not null is set,
			// the default value is the first element of the enum list
			d = types.NewDatum(col.FieldType.Elems[0])
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
			return emptyBytes, sizeOfEmptyBytes, "", nil
		default:
			d = table.GetZeroValue(col)
			if d.IsNull() {
				log.Error("meet unsupported column type", zap.String("columnInfo", col.String()))
			}
		}
	}

	return formatColVal(d, col.Tp)
}

// DecodeTableID decodes the raw key to a table ID
func DecodeTableID(key []byte) (model.TableID, error) {
	_, physicalTableID, err := decodeTableID(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return physicalTableID, nil
}
