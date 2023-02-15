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
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pfilter "github.com/pingcap/tiflow/pkg/filter"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type baseKVEntry struct {
	StartTs uint64
	// Commit or resolved TS
	CRTs uint64

	PhysicalTableID int64
	IntRowID        int64 // Valid if the handle is row id.
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
	// DecodeEvent accepts `model.PolymorphicEvent` with `RawKVEntry` filled and
	// decodes `RawKVEntry` into `RowChangedEvent`.
	DecodeEvent(ctx context.Context, event *model.PolymorphicEvent) error
}

type mounter struct {
	schemaStorage  SchemaStorage
	tz             *time.Location
	enableOldValue bool
	changefeedID   model.ChangeFeedID
	filter         pfilter.Filter

	tableCache map[model.TableID]*tableMetaValue

	metricTotalRows              prometheus.Gauge
	metricIgnoredDMLEventCounter prometheus.Counter
}

type tableMetaValue struct {
	physicalID model.TableID
	timestamp  uint64
	tableInfo  *model.TableInfo

	// These fields are used to construct a model.RowChangedEvent in place.
	buffer model.RowChangedEvent
	rawRow model.RowChangedDatums

	// To cache default values.
	colDefaults map[int64]columnDefaultOrZeroValue
}

type columnDefaultOrZeroValue struct {
	d    types.Datum
	v    interface{}
	size int
	warn string
	err  error
}

// NewMounter creates a mounter
func NewMounter(schemaStorage SchemaStorage,
	changefeedID model.ChangeFeedID,
	tz *time.Location,
	filter pfilter.Filter,
	enableOldValue bool,
) Mounter {
	return &mounter{
		schemaStorage:  schemaStorage,
		changefeedID:   changefeedID,
		enableOldValue: enableOldValue,
		filter:         filter,
		tz:             tz,

		tableCache: make(map[model.TableID]*tableMetaValue),

		metricTotalRows: totalRowsCountGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricIgnoredDMLEventCounter: ignoredDMLEventCounter.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}
}

// DecodeEvent decode kv events using ddl puller's schemaStorage
// this method could block indefinitely if the DDL puller is lagging.
func (m *mounter) DecodeEvent(ctx context.Context, event *model.PolymorphicEvent) error {
	m.metricTotalRows.Inc()
	if event.IsResolved() {
		return nil
	}
	row, err := m.unmarshalAndMountRowChanged(ctx, event.RawKV)
	if err != nil {
		return errors.Trace(err)
	}

	event.Row = row
	event.RawKV.Value = nil
	event.RawKV.OldValue = nil
	return nil
}

func (m *mounter) unmarshalAndMountRowChanged(ctx context.Context, raw *model.RawKVEntry) (*model.RowChangedEvent, error) {
	if !bytes.HasPrefix(raw.Key, tablePrefix) {
		return nil, nil
	}
	key, physicalTableID, err := decodeTableID(raw.Key)
	if err != nil {
		return nil, err
	}
	if len(raw.OldValue) == 0 && len(raw.Value) == 0 {
		log.Warn("empty value and old value",
			zap.String("namespace", m.changefeedID.Namespace),
			zap.String("changefeed", m.changefeedID.ID),
			zap.Any("row", raw))
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
		tableInfo, ts, exist := snap.PhysicalTableWithTsByID(physicalTableID)
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

			tableMeta := m.fillTableMeta(physicalTableID, ts, tableInfo)
			if err = m.mountRowKVEntry(tableMeta, rowKV, raw.ApproximateDataSize()); err != nil {
				return nil, err
			}
			// We need to filter a row here because we need its tableInfo.
			ignore, err := m.filter.ShouldIgnoreDMLEvent(&tableMeta.buffer, tableMeta.rawRow, tableInfo)
			if err != nil {
				return nil, err
			}
			// TODO(dongmen): try to find better way to indicate this row has been filtered.
			// Return a nil RowChangedEvent if this row should be ignored.
			if ignore {
				m.metricIgnoredDMLEventCounter.Inc()
				return nil, nil
			}
			row := shallowCopy(&tableMeta.buffer)
			return row, nil
		}
		return nil, nil
	}()
	if err != nil && !cerror.IsChangefeedUnRetryableError(err) {
		log.Error("failed to mount and unmarshals entry, start to print debug info", zap.Error(err))
		snap.PrintStatus(log.Error)
	}
	return row, err
}

func (m *mounter) unmarshalRowKVEntry(tableInfo *model.TableInfo, rawKey []byte, rawValue []byte, rawOldValue []byte, base baseKVEntry) (*rowKVEntry, error) {
	recordID, err := tablecodec.DecodeRowKey(rawKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if recordID.IsInt() {
		base.IntRowID = recordID.IntValue()
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

	return &rowKVEntry{
		baseKVEntry: base,
		Row:         row,
		PreRow:      preRow,
		RowExist:    rowExist,
		PreRowExist: preRowExist,
	}, nil
}

func (m *mounter) fillTableMeta(physicalID model.TableID, timestamp uint64, tableInfo *model.TableInfo) *tableMetaValue {
	if value, ok := m.tableCache[physicalID]; ok {
		if value.timestamp == timestamp {
			return value
		}
	}

	value := newTableMetaValue(physicalID, timestamp, tableInfo)
	m.tableCache[physicalID] = value
	return value
}

func newTableMetaValue(physicalID model.TableID, timestamp uint64, tableInfo *model.TableInfo) *tableMetaValue {
	_, _, colInfos := tableInfo.GetRowColInfos()

	value := &tableMetaValue{
		physicalID: physicalID,
		timestamp:  timestamp,
		tableInfo:  tableInfo,
		// Initialize immutable fields of the buffer model.RowChangedEvent.
		buffer: model.RowChangedEvent{
			Table: &model.TableName{
				Schema:      tableInfo.TableName.Schema,
				Table:       tableInfo.TableName.Table,
				TableID:     physicalID,
				IsPartition: tableInfo.GetPartitionInfo() != nil,
			},
			Columns:      make([]*model.Column, len(tableInfo.RowColumnsOffset)),
			PreColumns:   make([]*model.Column, len(tableInfo.RowColumnsOffset)),
			IndexColumns: tableInfo.IndexColumnsOffset,
			TableInfo:    tableInfo,
			ColInfos:     colInfos,
		},
	}

	// Allocate model.Columns in a continuous space.
	columns := make([]model.Column, len(tableInfo.RowColumnsOffset))
	preColumns := make([]model.Column, len(tableInfo.RowColumnsOffset))
	for _, colInfo := range tableInfo.Columns {
		if !model.IsColCDCVisible(colInfo) {
			log.Debug("skip the column which is not visible",
				zap.String("table", tableInfo.Name.O), zap.String("column", colInfo.Name.O))
			continue
		}
		offset := tableInfo.RowColumnsOffset[colInfo.ID]
		columns[offset] = model.Column{
			Name:    colInfo.Name.O,
			Type:    colInfo.GetType(),
			Charset: colInfo.GetCharset(),
			Flag:    tableInfo.ColumnsFlag[colInfo.ID],
			Default: getDDLDefaultDefinition(colInfo),
		}
		preColumns[offset] = model.Column{
			Name:    colInfo.Name.O,
			Type:    colInfo.GetType(),
			Charset: colInfo.GetCharset(),
			Flag:    tableInfo.ColumnsFlag[colInfo.ID],
			Default: getDDLDefaultDefinition(colInfo),
		}
		value.buffer.Columns[offset] = &columns[offset]
		value.buffer.PreColumns[offset] = &preColumns[offset]
	}

	value.rawRow.RowDatums = make([]types.Datum, len(tableInfo.RowColumnsOffset))
	value.rawRow.PreRowDatums = make([]types.Datum, len(tableInfo.RowColumnsOffset))
	value.colDefaults = make(map[int64]columnDefaultOrZeroValue)
	return value
}

// IsLegacyFormatJob returns true if the job is from the legacy DDL list key.
func IsLegacyFormatJob(rawKV *model.RawKVEntry) bool {
	return bytes.HasPrefix(rawKV.Key, metaPrefix)
}

// ParseDDLJob parses the job from the raw KV entry. id is the column id of `job_meta`.
func ParseDDLJob(tblInfo *model.TableInfo, rawKV *model.RawKVEntry, id int64) (*timodel.Job, error) {
	var v []byte
	if bytes.HasPrefix(rawKV.Key, metaPrefix) {
		// old queue base job.
		v = rawKV.Value
	} else {
		// DDL job comes from `tidb_ddl_job` table after we support concurrent DDL. We should decode the job from the column.
		recordID, err := tablecodec.DecodeRowKey(rawKV.Key)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row, err := decodeRow(rawKV.Value, recordID, tblInfo, time.UTC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		datum := row[id]
		v = datum.GetBytes()
	}

	return parseJob(v, rawKV.StartTs, rawKV.CRTs)
}

// parseJob unmarshal the job from "v".
func parseJob(v []byte, startTs, CRTs uint64) (*timodel.Job, error) {
	job := &timodel.Job{}
	err := json.Unmarshal(v, job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debug("get new DDL job", zap.String("detail", job.String()))
	if !job.IsDone() && !job.IsSynced() {
		return nil, nil
	}
	// FinishedTS is only set when the job is synced,
	// but we can use the entry's ts here
	job.StartTS = startTs
	job.BinlogInfo.FinishedTS = CRTs
	return job, nil
}

// datum2Column reads columns from `datums`, and fill them into v.buffer.
func (v *tableMetaValue) datum2Column(isPreValue bool, datums map[int64]types.Datum, fillWithDefaultValue bool) error {
	var rawCols []types.Datum
	var colvals []model.ColumnValue
	if isPreValue {
		rawCols = v.rawRow.PreRowDatums
		v.buffer.PreColumnValues = make([]model.ColumnValue, len(v.tableInfo.RowColumnsOffset))
		colvals = v.buffer.PreColumnValues
	} else {
		rawCols = v.rawRow.RowDatums
		v.buffer.ColumnValues = make([]model.ColumnValue, len(v.tableInfo.RowColumnsOffset))
		colvals = v.buffer.ColumnValues
	}
	for i := 0; i < len(v.tableInfo.RowColumnsOffset); i++ {
		rawCols[i] = types.Datum{}
	}

	for _, colInfo := range v.tableInfo.Columns {
		if !model.IsColCDCVisible(colInfo) {
			continue
		}
		offset := v.tableInfo.RowColumnsOffset[colInfo.ID]

		colDatum, exist := datums[colInfo.ID]
		if !exist && !fillWithDefaultValue {
			log.Debug("column value is not found",
				zap.String("table", v.tableInfo.Name.O), zap.String("column", colInfo.Name.O))
			continue
		}

		var colValue interface{}
		var size int
		var warn string
		var err error
		if exist {
			colValue, size, warn, err = formatColVal(colDatum, colInfo)
		} else if fillWithDefaultValue {
			if def, ok := v.colDefaults[colInfo.ID]; ok {
				colDatum = def.d
				colValue = def.v
				size = def.size
				warn = def.warn
				err = def.err
			} else {
				colDatum, colValue, size, warn, err = getDefaultOrZeroValue(colInfo)
				v.colDefaults[colInfo.ID] = columnDefaultOrZeroValue{
					d: colDatum, v: colValue, size: size, warn: warn, err: err,
				}
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
		if warn != "" {
			log.Warn(warn,
				zap.String("table", v.tableInfo.TableName.String()),
				zap.String("column", colInfo.Name.String()))
		}

		rawCols[offset] = colDatum
		colvals[offset].Value = colValue
		v.buffer.ApproximateMemSize += size + sizeOfColumnValue
	}
	return nil
}

func (m *mounter) mountRowKVEntry(tableMeta *tableMetaValue, row *rowKVEntry, dataSize int64) (err error) {
	tableMeta.buffer.StartTs = row.StartTs
	tableMeta.buffer.CommitTs = row.CRTs
	tableMeta.buffer.RowID = row.IntRowID
	tableMeta.buffer.ApproximateDataSize = dataSize
	tableMeta.buffer.ApproximateMemSize = int(unsafe.Sizeof(model.RowChangedEvent{}))

	// Since we now always use old value internally,
	// we need to control the output(sink will use the PreColumns field to determine whether to output old value).
	// Normally old value is output when only enableOldValue is on,
	// but for the Delete event, when the old value feature is off,
	// the HandleKey column needs to be included as well. So we need to do the following filtering.
	if row.PreRowExist {
		// FIXME(leoppro): using pre table info to mounter pre column datum
		// the pre column and current column in one event may using different table info
		err = tableMeta.datum2Column(true, row.PreRow, m.enableOldValue)
		if err != nil {
			return errors.Trace(err)
		}

		// NOTICE: When the old Value feature is off,
		// the Delete event only needs to keep the handle key column.
		if row.Delete && !m.enableOldValue {
			for i, col := range tableMeta.buffer.PreColumns {
				if col != nil && !col.Flag.IsHandleKey() {
					tableMeta.buffer.PreColumns[i] = nil
				}
			}
		}
	}

	if row.RowExist {
		err = tableMeta.datum2Column(false, row.Row, true)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

var emptyBytes = make([]byte, 0)

const (
	sizeOfColumnValue = int(unsafe.Sizeof(model.ColumnValue{}))
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
func formatColVal(datum types.Datum, col *timodel.ColumnInfo) (
	value interface{}, size int, warn string, err error,
) {
	if datum.IsNull() {
		return nil, 0, "", nil
	}
	switch col.GetType() {
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
func getDefaultOrZeroValue(col *timodel.ColumnInfo) (types.Datum, any, int, string, error) {
	var d types.Datum
	// NOTICE: SHOULD use OriginDefaultValue here, more info pls ref to
	// https://github.com/pingcap/tiflow/issues/4048
	// FIXME: Too many corner cases may hit here, like type truncate, timezone
	// (1) If this column is uk(no pk), will cause data inconsistency in Scenarios(2)
	// (2) If not fix here, will cause data inconsistency in Scenarios(3) directly
	// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
	if col.GetOriginDefaultValue() != nil {
		d = types.NewDatum(col.GetOriginDefaultValue())
		return d, d.GetValue(), sizeOfDatum(d), "", nil
	}

	if !mysql.HasNotNullFlag(col.GetFlag()) {
		// NOTICE: NotNullCheck need do after OriginDefaultValue check, as when TiDB meet "amend + add column default xxx",
		// ref: https://github.com/pingcap/ticdc/issues/3929
		// must use null if TiDB not write the column value when default value is null
		// and the value is null, see https://github.com/pingcap/tidb/issues/9304
		d = types.NewDatum(nil)
	} else {
		switch col.GetType() {
		case mysql.TypeEnum:
			// For enum type, if no default value and not null is set,
			// the default value is the first element of the enum list
			d = types.NewDatum(col.FieldType.GetElem(0))
		case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
			return d, emptyBytes, sizeOfEmptyBytes, "", nil
		default:
			d = table.GetZeroValue(col)
			if d.IsNull() {
				log.Error("meet unsupported column type", zap.String("columnInfo", col.FieldType.String()))
			}
		}
	}
	v, size, warn, err := formatColVal(d, col)
	return d, v, size, warn, err
}

func getDDLDefaultDefinition(col *timodel.ColumnInfo) interface{} {
	defaultValue := col.GetDefaultValue()
	if defaultValue == nil {
		defaultValue = col.GetOriginDefaultValue()
	}
	defaultDatum := types.NewDatum(defaultValue)
	return defaultDatum.GetValue()
}

// DecodeTableID decodes the raw key to a table ID
func DecodeTableID(key []byte) (model.TableID, error) {
	_, physicalTableID, err := decodeTableID(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return physicalTableID, nil
}

func shallowCopy(r *model.RowChangedEvent) (rr *model.RowChangedEvent) {
	rr = new(model.RowChangedEvent)
	*rr = *r
	r.ColumnValues = nil
	r.PreColumnValues = nil
	if len(rr.ColumnValues) == 0 {
		rr.Columns = nil
	}
	if len(rr.PreColumnValues) == 0 {
		rr.PreColumns = nil
	}
	return
}
