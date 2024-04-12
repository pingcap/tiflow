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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pfilter "github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
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
	// DecodeEvent accepts `model.PolymorphicEvent` with `RawKVEntry` filled and
	// decodes `RawKVEntry` into `RowChangedEvent`.
	// If a `model.PolymorphicEvent` should be ignored, it will returns (false, nil).
	DecodeEvent(ctx context.Context, event *model.PolymorphicEvent) error
}

type mounter struct {
	schemaStorage                SchemaStorage
	tz                           *time.Location
	changefeedID                 model.ChangeFeedID
	filter                       pfilter.Filter
	metricTotalRows              prometheus.Gauge
	metricIgnoredDMLEventCounter prometheus.Counter

	integrity *integrity.Config

	// decoder and preDecoder are used to decode the raw value, also used to extract checksum,
	// they should not be nil after decode at least one event in the row format v2.
	decoder    *rowcodec.DatumMapDecoder
	preDecoder *rowcodec.DatumMapDecoder

	// encoder is used to calculate the checksum.
	encoder *rowcodec.Encoder
	// sctx hold some information can be used by the encoder to calculate the checksum.
	sctx *stmtctx.StatementContext
}

// NewMounter creates a mounter
func NewMounter(schemaStorage SchemaStorage,
	changefeedID model.ChangeFeedID,
	tz *time.Location,
	filter pfilter.Filter,
	integrity *integrity.Config,
) Mounter {
	return &mounter{
		schemaStorage: schemaStorage,
		changefeedID:  changefeedID,
		filter:        filter,
		metricTotalRows: totalRowsCountGauge.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricIgnoredDMLEventCounter: ignoredDMLEventCounter.
			WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		tz:        tz,
		integrity: integrity,

		encoder: &rowcodec.Encoder{},
		sctx: &stmtctx.StatementContext{
			TimeZone: tz,
		},
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

	m.decoder = nil
	m.preDecoder = nil

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
	// When async commit is enabled, the commitTs of DMLs may be equals with DDL finishedTs.
	// A DML whose commitTs is equal to a DDL finishedTs should use the schema info before the DDL.
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
			// for truncate table and truncate table partition DDL, the table ID is changed, but DML can be inserted to TiKV with old table ID.
			// normally, cdc will close the old table pipeline and create a new one, and these invalid DMLs keys will not be pulled by CDC,
			// but if redo is enabled or push based table pipeline is enabled, puller and mounter are not blocked by barrier ts.
			// So some invalid DML keys will be decoded before processor removing the table pipeline
			if snap.IsTruncateTableID(physicalTableID) {
				log.Debug("skip the DML of truncated table", zap.Uint64("ts", raw.CRTs), zap.Int64("tableID", physicalTableID))
				return nil, nil
			}
			log.Error("can not found table schema",
				zap.Uint64("ts", raw.CRTs),
				zap.String("key", hex.EncodeToString(raw.Key)),
				zap.Int64("tableID", physicalTableID))
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
			row, rawRow, err := m.mountRowKVEntry(tableInfo, rowKV, raw.ApproximateDataSize())
			if err != nil {
				return nil, err
			}
			// We need to filter a row here because we need its tableInfo.
			ignore, err := m.filter.ShouldIgnoreDMLEvent(row, rawRow, tableInfo)
			if err != nil {
				return nil, err
			}
			// TODO(dongmen): try to find better way to indicate this row has been filtered.
			// Return a nil RowChangedEvent if this row should be ignored.
			if ignore {
				m.metricIgnoredDMLEventCounter.Inc()
				return nil, nil
			}
			return row, nil
		}
		return nil, nil
	}()
	if err != nil && !cerror.ShouldFailChangefeed(err) {
		log.Error("failed to mount and unmarshals entry, start to print debug info", zap.Error(err))
		snap.PrintStatus(log.Error)
	}
	return row, err
}

func (m *mounter) unmarshalRowKVEntry(
	tableInfo *model.TableInfo,
	rawKey []byte,
	rawValue []byte,
	rawOldValue []byte,
	base baseKVEntry,
) (*rowKVEntry, error) {
	recordID, err := tablecodec.DecodeRowKey(rawKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	base.RecordID = recordID

	var (
		row, preRow           map[int64]types.Datum
		rowExist, preRowExist bool
	)

	row, rowExist, err = m.decodeRow(rawValue, recordID, tableInfo, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	preRow, preRowExist, err = m.decodeRow(rawOldValue, recordID, tableInfo, true)
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

func (m *mounter) decodeRow(
	rawValue []byte, recordID kv.Handle, tableInfo *model.TableInfo, isPreColumns bool,
) (map[int64]types.Datum, bool, error) {
	if len(rawValue) == 0 {
		return map[int64]types.Datum{}, false, nil
	}
	handleColIDs, handleColFt, reqCols := tableInfo.GetRowColInfos()
	var (
		datums map[int64]types.Datum
		err    error
	)

	if rowcodec.IsNewFormat(rawValue) {
		decoder := rowcodec.NewDatumMapDecoder(reqCols, m.tz)
		if isPreColumns {
			m.preDecoder = decoder
		} else {
			m.decoder = decoder
		}
		datums, err = decodeRowV2(decoder, rawValue)
	} else {
		datums, err = decodeRowV1(rawValue, tableInfo, m.tz)
	}

	if err != nil {
		return nil, false, errors.Trace(err)
	}

	datums, err = tablecodec.DecodeHandleToDatumMap(
		recordID, handleColIDs, handleColFt, m.tz, datums)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	return datums, true, nil
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
	if !job.IsDone() {
		return nil, nil
	}
	// FinishedTS is only set when the job is synced,
	// but we can use the entry's ts here
	job.StartTS = startTs
	job.BinlogInfo.FinishedTS = CRTs
	return job, nil
}

func datum2Column(
	tableInfo *model.TableInfo, datums map[int64]types.Datum, tz *time.Location,
) ([]*model.Column, []types.Datum, []*timodel.ColumnInfo, []rowcodec.ColInfo, error) {
	cols := make([]*model.Column, len(tableInfo.RowColumnsOffset))
	rawCols := make([]types.Datum, len(tableInfo.RowColumnsOffset))

	// columnInfos and rowColumnInfos hold different column metadata,
	// they should have the same length and order.
	columnInfos := make([]*timodel.ColumnInfo, len(tableInfo.RowColumnsOffset))
	rowColumnInfos := make([]rowcodec.ColInfo, len(tableInfo.RowColumnsOffset))

	_, _, extendColumnInfos := tableInfo.GetRowColInfos()

	for idx, colInfo := range tableInfo.Columns {
		if !model.IsColCDCVisible(colInfo) {
			log.Debug("skip the column which is not visible",
				zap.String("table", tableInfo.Name.O), zap.String("column", colInfo.Name.O))
			continue
		}

		colName := colInfo.Name.O
		colID := colInfo.ID
		colDatums, exist := datums[colID]

		var (
			colValue interface{}
			size     int
			warn     string
			err      error
		)
		if exist {
			colValue, size, warn, err = formatColVal(colDatums, colInfo)
		} else {
			colDatums, colValue, size, warn, err = getDefaultOrZeroValue(colInfo, tz)
		}
		if err != nil {
			return nil, nil, nil, nil, errors.Trace(err)
		}
		if warn != "" {
			log.Warn(warn, zap.String("table", tableInfo.TableName.String()),
				zap.String("column", colInfo.Name.String()))
		}

		defaultValue := GetDDLDefaultDefinition(colInfo)
		offset := tableInfo.RowColumnsOffset[colID]
		rawCols[offset] = colDatums
		cols[offset] = &model.Column{
			Name:      colName,
			Type:      colInfo.GetType(),
			Charset:   colInfo.GetCharset(),
			Collation: colInfo.GetCollate(),
			Value:     colValue,
			Default:   defaultValue,
			Flag:      tableInfo.ColumnsFlag[colID],
			// ApproximateBytes = column data size + column struct size
			ApproximateBytes: size + sizeOfEmptyColumn,
		}
		columnInfos[offset] = colInfo
		rowColumnInfos[offset] = extendColumnInfos[idx]
	}
	return cols, rawCols, columnInfos, rowColumnInfos, nil
}

func (m *mounter) calculateChecksum(
	columnInfos []*timodel.ColumnInfo, rawColumns []types.Datum,
) (uint32, error) {
	columns := make([]rowcodec.ColData, 0, len(rawColumns))
	for idx, col := range columnInfos {
		column := rowcodec.ColData{
			ColumnInfo: col,
			Datum:      &rawColumns[idx],
		}
		columns = append(columns, column)
	}
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].ID < columns[j].ID
	})

	calculator := rowcodec.RowData{
		Cols: columns,
		Data: make([]byte, 0),
	}

	checksum, err := calculator.Checksum(m.tz)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return checksum, nil
}

// return error if cannot get the expected checksum from the decoder
// return false if the checksum is not matched
func (m *mounter) verifyChecksum(
	columnInfos []*timodel.ColumnInfo, rawColumns []types.Datum, isPreRow bool,
) (uint32, int, bool, error) {
	if !m.integrity.Enabled() {
		return 0, 0, true, nil
	}

	var decoder *rowcodec.DatumMapDecoder
	if isPreRow {
		decoder = m.preDecoder
	} else {
		decoder = m.decoder
	}
	if decoder == nil {
		return 0, 0, false, errors.New("cannot found the decoder to get the checksum")
	}

	version := decoder.ChecksumVersion()
	// if the checksum cannot be found, which means the upstream TiDB checksum is not enabled,
	// so return matched as true to skip check the event.
	first, ok := decoder.GetChecksum()
	if !ok {
		return 0, version, true, nil
	}

	checksum, err := m.calculateChecksum(columnInfos, rawColumns)
	if err != nil {
		log.Error("failed to calculate the checksum", zap.Uint32("first", first), zap.Error(err))
		return 0, version, false, errors.Trace(err)
	}

	// the first checksum matched, it hits in the most case.
	if checksum == first {
		log.Debug("checksum matched",
			zap.Uint32("checksum", checksum), zap.Uint32("first", first))
		return checksum, version, true, nil
	}

	extra, ok := decoder.GetExtraChecksum()
	if !ok {
		log.Error("cannot found the extra checksum, the first checksum mismatched",
			zap.Uint32("checksum", checksum), zap.Uint32("first", first))
		return checksum, version, false, nil
	}

	if checksum == extra {
		log.Debug("extra checksum matched, this may happen the upstream TiDB is during the DDL"+
			"execution phase",
			zap.Uint32("checksum", checksum),
			zap.Uint32("extra", extra))
		return checksum, version, true, nil
	}

	log.Error("checksum mismatch",
		zap.Uint32("checksum", checksum),
		zap.Uint32("first", first),
		zap.Uint32("extra", extra))
	return checksum, version, false, nil
}

func (m *mounter) mountRowKVEntry(tableInfo *model.TableInfo, row *rowKVEntry, dataSize int64) (*model.RowChangedEvent, model.RowChangedDatums, error) {
	var (
		rawRow            model.RowChangedDatums
		columnInfos       []*timodel.ColumnInfo
		extendColumnInfos []rowcodec.ColInfo
		matched           bool
		err               error

		checksum *integrity.Checksum

		checksumVersion int
		corrupted       bool
	)

	// Decode previous columns.
	var (
		preCols     []*model.Column
		preRawCols  []types.Datum
		preChecksum uint32
	)
	// Since we now always use old value internally,
	// we need to control the output(sink will use the PreColumns field to determine whether to output old value).
	// Normally old value is output when only enableOldValue is on,
	// but for the Delete event, when the old value feature is off,
	// the HandleKey column needs to be included as well. So we need to do the following filtering.
	if row.PreRowExist {
		// FIXME(leoppro): using pre table info to mounter pre column datum
		// the pre column and current column in one event may using different table info
		preCols, preRawCols, columnInfos, extendColumnInfos, err = datum2Column(tableInfo, row.PreRow, m.tz)
		if err != nil {
			return nil, rawRow, errors.Trace(err)
		}

		preChecksum, checksumVersion, matched, err = m.verifyChecksum(columnInfos, preRawCols, true)
		if err != nil {
			return nil, rawRow, errors.Trace(err)
		}

		if !matched {
			log.Error("previous columns checksum mismatch",
				zap.Uint32("checksum", preChecksum),
				zap.Any("tableInfo", tableInfo),
				zap.Any("row", row))
			if m.integrity.ErrorHandle() {
				return nil, rawRow, cerror.ErrCorruptedDataMutation.
					GenWithStackByArgs(m.changefeedID.Namespace, m.changefeedID.ID, row)
			}
			corrupted = true
		}
	}

	var (
		cols    []*model.Column
		rawCols []types.Datum
		current uint32
	)
	if row.RowExist {
		cols, rawCols, columnInfos, extendColumnInfos, err = datum2Column(tableInfo, row.Row, m.tz)
		if err != nil {
			return nil, rawRow, errors.Trace(err)
		}

		current, checksumVersion, matched, err = m.verifyChecksum(columnInfos, rawCols, false)
		if err != nil {
			return nil, rawRow, errors.Trace(err)
		}
		if !matched {
			log.Error("columns checksum mismatch",
				zap.Uint32("checksum", preChecksum),
				zap.Any("tableInfo", tableInfo),
				zap.Any("rawCols", rawCols))
			if m.integrity.ErrorHandle() {
				return nil, rawRow, cerror.ErrCorruptedDataMutation.
					GenWithStackByArgs(m.changefeedID.Namespace, m.changefeedID.ID, row)
			}
			corrupted = true
		}
	}

	schemaName := tableInfo.TableName.Schema
	tableName := tableInfo.TableName.Table
	var intRowID int64
	if row.RecordID.IsInt() {
		intRowID = row.RecordID.IntValue()
	}

	rawRow.PreRowDatums = preRawCols
	rawRow.RowDatums = rawCols

	// if both are 0, it means the checksum is not enabled
	// so the checksum is nil to reduce memory allocation.
	if preChecksum != 0 || current != 0 {
		checksum = &integrity.Checksum{
			Current:   current,
			Previous:  preChecksum,
			Corrupted: corrupted,
			Version:   checksumVersion,
		}
	}

	return &model.RowChangedEvent{
		StartTs:  row.StartTs,
		CommitTs: row.CRTs,
		RowID:    intRowID,
		Table: &model.TableName{
			Schema:      schemaName,
			Table:       tableName,
			TableID:     row.PhysicalTableID,
			IsPartition: tableInfo.GetPartitionInfo() != nil,
		},
		ColInfos:   extendColumnInfos,
		TableInfo:  tableInfo,
		Columns:    cols,
		PreColumns: preCols,

		Checksum: checksum,

		IndexColumns:        tableInfo.IndexColumnsOffset,
		ApproximateDataSize: dataSize,
	}, rawRow, nil
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
	case mysql.TypeFloat:
		v := datum.GetFloat32()
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 1) || math.IsInf(float64(v), -1) {
			warn = fmt.Sprintf("the value is invalid in column: %f", v)
			v = 0
		}
		const sizeOfV = unsafe.Sizeof(v)
		return v, int(sizeOfV), warn, nil
	case mysql.TypeDouble:
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
func getDefaultOrZeroValue(col *timodel.ColumnInfo, tz *time.Location) (types.Datum, any, int, string, error) {
	var (
		d   types.Datum
		err error
	)
	// NOTICE: SHOULD use OriginDefaultValue here, more info pls ref to
	// https://github.com/pingcap/tiflow/issues/4048
	// FIXME: Too many corner cases may hit here, like type truncate, timezone
	// (1) If this column is uk(no pk), will cause data inconsistency in Scenarios(2)
	// (2) If not fix here, will cause data inconsistency in Scenarios(3) directly
	// Ref: https://github.com/pingcap/tidb/blob/d2c352980a43bb593db81fd1db996f47af596d91/table/column.go#L489
	if col.GetOriginDefaultValue() != nil {
		datum := types.NewDatum(col.GetOriginDefaultValue())
		sctx := new(stmtctx.StatementContext)
		sctx.TimeZone = tz
		d, err = datum.ConvertTo(sctx, &col.FieldType)
		if err != nil {
			return d, d.GetValue(), sizeOfDatum(d), "", errors.Trace(err)
		}
	} else if !mysql.HasNotNullFlag(col.GetFlag()) {
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
			name := col.FieldType.GetElem(0)
			enumValue, err := types.ParseEnumName(col.FieldType.GetElems(), name, col.GetCollate())
			if err != nil {
				return d, nil, 0, "", errors.Trace(err)
			}
			d = types.NewMysqlEnumDatum(enumValue)
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

// GetDDLDefaultDefinition returns the default definition of a column.
func GetDDLDefaultDefinition(col *timodel.ColumnInfo) interface{} {
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
