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
	"reflect"
	"sort"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pfilter "github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/spanz"
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

// DDLTableInfo contains the tableInfo about tidb_ddl_job and tidb_ddl_history
// and the column id of `job_meta` in these two tables.
type DDLTableInfo struct {
	// ddlJobsTable use to parse all ddl jobs except `create table`
	DDLJobTable *model.TableInfo
	// It holds the column id of `job_meta` in table `tidb_ddl_jobs`.
	JobMetaColumnIDinJobTable int64
	// ddlHistoryTable only use to parse `create table` ddl job
	DDLHistoryTable *model.TableInfo
	// It holds the column id of `job_meta` in table `tidb_ddl_history`.
	JobMetaColumnIDinHistoryTable int64
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

	lastSkipOldValueTime time.Time
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
	// checksumKey is only used to calculate raw checksum if necessary.
	checksumKey := raw.Key
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
			row, rawRow, err := m.mountRowKVEntry(tableInfo, rowKV, checksumKey, raw.ApproximateDataSize())
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

// ParseDDLJob parses the job from the raw KV entry.
func ParseDDLJob(rawKV *model.RawKVEntry, ddlTableInfo *DDLTableInfo) (*timodel.Job, error) {
	var v []byte
	var datum types.Datum

	// for test case only
	if bytes.HasPrefix(rawKV.Key, metaPrefix) {
		v = rawKV.Value
		job, err := parseJob(v, rawKV.StartTs, rawKV.CRTs, false)
		if err != nil || job == nil {
			job, err = parseJob(v, rawKV.StartTs, rawKV.CRTs, true)
		}
		return job, err
	}

	recordID, err := tablecodec.DecodeRowKey(rawKV.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tableID := tablecodec.DecodeTableID(rawKV.Key)

	// parse it with tidb_ddl_job
	if tableID == spanz.JobTableID {
		row, err := decodeRow(rawKV.Value, recordID, ddlTableInfo.DDLJobTable, time.UTC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		datum = row[ddlTableInfo.JobMetaColumnIDinJobTable]
		v = datum.GetBytes()

		return parseJob(v, rawKV.StartTs, rawKV.CRTs, false)
	} else if tableID == spanz.JobHistoryID {
		// parse it with tidb_ddl_history
		row, err := decodeRow(rawKV.Value, recordID, ddlTableInfo.DDLHistoryTable, time.UTC)
		if err != nil {
			return nil, errors.Trace(err)
		}
		datum = row[ddlTableInfo.JobMetaColumnIDinHistoryTable]
		v = datum.GetBytes()

		return parseJob(v, rawKV.StartTs, rawKV.CRTs, true)
	}

	return nil, fmt.Errorf("Unvalid tableID %v in rawKV.Key", tableID)
}

// parseJob unmarshal the job from "v".
// fromHistoryTable is used to distinguish the job is from tidb_dd_job or tidb_ddl_history
// We need to be compatible with the two modes, enable_fast_create_table=on and enable_fast_create_table=off
// When enable_fast_create_table=on, `create table` will only be inserted into tidb_ddl_history after being executed successfully.
// When enable_fast_create_table=off, `create table` just like other ddls will be firstly inserted to tidb_ddl_job,
// and being inserted into tidb_ddl_history after being executed successfully.
// In both two modes, other ddls are all firstly inserted into tidb_ddl_job, and then inserted into tidb_ddl_history after being executed successfully.
//
// To be compatible with these two modes, we will get `create table` ddl from tidb_ddl_history, and all ddls from tidb_ddl_job.
// When enable_fast_create_table=off, for each `create table` ddl we will get twice(once from tidb_ddl_history, once from tidb_ddl_job)
// Because in `handleJob` we will skip the repeated ddls, thus it's ok for us to get `create table` twice.
// Besides, the `create table` from tidb_ddl_job always have a earlier commitTs than from tidb_ddl_history.
// Therefore, we always use the commitTs of ddl from `tidb_ddl_job` as StartTs, which ensures we can get all the dmls.
func parseJob(v []byte, startTs, CRTs uint64, fromHistoryTable bool) (*timodel.Job, error) {
	var job timodel.Job
	err := json.Unmarshal(v, &job)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if fromHistoryTable {
		// we only want to get `create table` and `create tables` ddl from tidb_ddl_history, so we just throw out others ddls.
		// We only want the job with `JobStateSynced`, which is means the ddl job is done successfully.
		// Besides, to satisfy the subsequent processing,
		// We need to set the job to be Done to make it will replay in schemaStorage
		if (job.Type != timodel.ActionCreateTable && job.Type != timodel.ActionCreateTables) || job.State != timodel.JobStateSynced {
			return nil, nil
		}
		job.State = timodel.JobStateDone
	} else {
		// we need to get all ddl job which is done from tidb_ddl_job
		if !job.IsDone() {
			return nil, nil
		}
	}

	// FinishedTS is only set when the job is synced,
	// but we can use the entry's ts here
	job.StartTS = startTs
	// Since ddl in stateDone doesn't contain the FinishedTS,
	// we need to set it as the txn's commit ts.
	job.BinlogInfo.FinishedTS = CRTs
	return &job, nil
}

func datum2Column(
	tableInfo *model.TableInfo, datums map[int64]types.Datum, tz *time.Location,
) ([]*model.ColumnData, []types.Datum, []*timodel.ColumnInfo, error) {
	cols := make([]*model.ColumnData, len(tableInfo.RowColumnsOffset))
	rawCols := make([]types.Datum, len(tableInfo.RowColumnsOffset))

	// columnInfos should have the same length and order with cols
	columnInfos := make([]*timodel.ColumnInfo, len(tableInfo.RowColumnsOffset))

	for _, colInfo := range tableInfo.Columns {
		if !model.IsColCDCVisible(colInfo) {
			log.Debug("skip the column which is not visible",
				zap.String("table", tableInfo.Name.O), zap.String("column", colInfo.Name.O))
			continue
		}

		colID := colInfo.ID
		colDatum, exist := datums[colID]

		var (
			colValue interface{}
			size     int
			warn     string
			err      error
		)
		if exist {
			colValue, size, warn, err = formatColVal(colDatum, colInfo)
		} else {
			colDatum, colValue, size, warn, err = getDefaultOrZeroValue(colInfo, tz)
		}
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		if warn != "" {
			log.Warn(warn, zap.String("table", tableInfo.TableName.String()),
				zap.String("column", colInfo.Name.String()))
		}

		offset := tableInfo.RowColumnsOffset[colID]
		rawCols[offset] = colDatum
		cols[offset] = &model.ColumnData{
			ColumnID: colID,
			Value:    colValue,
			// ApproximateBytes = column data size + column struct size
			ApproximateBytes: size + sizeOfEmptyColumn,
		}
		columnInfos[offset] = colInfo
	}
	return cols, rawCols, columnInfos, nil
}

func calculateColumnChecksum(
	columnInfos []*timodel.ColumnInfo, rawColumns []types.Datum, tz *time.Location,
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

	checksum, err := calculator.Checksum(tz)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return checksum, nil
}

func (m *mounter) verifyColumnChecksum(
	columnInfos []*timodel.ColumnInfo, rawColumns []types.Datum,
	decoder *rowcodec.DatumMapDecoder, skipFail bool,
) (uint32, bool, error) {
	// if the checksum cannot be found, which means the upstream TiDB checksum is not enabled,
	// so return matched as true to skip check the event.
	first, ok := decoder.GetChecksum()
	if !ok {
		return 0, true, nil
	}

	checksum, err := calculateColumnChecksum(columnInfos, rawColumns, m.tz)
	if err != nil {
		log.Error("failed to calculate the checksum", zap.Uint32("first", first), zap.Error(err))
		return 0, false, err
	}

	// the first checksum matched, it hits in the most case.
	if checksum == first {
		log.Debug("checksum matched", zap.Uint32("checksum", checksum), zap.Uint32("first", first))
		return checksum, true, nil
	}

	extra, ok := decoder.GetExtraChecksum()
	if ok && checksum == extra {
		log.Debug("extra checksum matched, this may happen the upstream TiDB is during the DDL execution phase",
			zap.Uint32("checksum", checksum), zap.Uint32("extra", extra))
		return checksum, true, nil
	}

	if !skipFail {
		log.Error("cannot found the extra checksum, the first checksum mismatched",
			zap.Uint32("checksum", checksum), zap.Uint32("first", first), zap.Uint32("extra", extra))
		return checksum, false, nil
	}

	if time.Since(m.lastSkipOldValueTime) > time.Minute {
		log.Warn("checksum mismatch on the old value, "+
			"this may caused by Add Column / Drop Column executed, skip verification",
			zap.Uint32("checksum", checksum), zap.Uint32("first", first), zap.Uint32("extra", extra))
		m.lastSkipOldValueTime = time.Now()
	}
	return checksum, true, nil
}

func newDatum(value interface{}, ft types.FieldType) (types.Datum, error) {
	if value == nil {
		return types.NewDatum(nil), nil
	}
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		switch v := value.(type) {
		case uint64:
			return types.NewUintDatum(v), nil
		case int64:
			return types.NewIntDatum(v), nil
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		t, err := types.ParseTime(types.DefaultStmtNoWarningContext, value.(string), ft.GetType(), ft.GetDecimal())
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewTimeDatum(t), nil
	case mysql.TypeDuration:
		d, _, err := types.ParseDuration(types.StrictContext, value.(string), ft.GetDecimal())
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewDurationDatum(d), nil
	case mysql.TypeJSON:
		bj, err := types.ParseBinaryJSONFromString(value.(string))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewJSONDatum(bj), nil
	case mysql.TypeNewDecimal:
		mysqlDecimal := new(types.MyDecimal)
		err := mysqlDecimal.FromString([]byte(value.(string)))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		datum := types.NewDecimalDatum(mysqlDecimal)
		datum.SetLength(ft.GetFlen())
		datum.SetFrac(ft.GetDecimal())
		return datum, nil
	case mysql.TypeEnum:
		enum, err := types.ParseEnumValue(ft.GetElems(), value.(uint64))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewMysqlEnumDatum(enum), nil
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.GetElems(), value.(uint64))
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		return types.NewMysqlSetDatum(set, ft.GetCollate()), nil
	case mysql.TypeBit:
		byteSize := (ft.GetFlen() + 7) >> 3
		binaryLiteral := types.NewBinaryLiteralFromUint(value.(uint64), byteSize)
		return types.NewMysqlBitDatum(binaryLiteral), nil
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		switch v := value.(type) {
		case []byte:
			return types.NewBytesDatum(v), nil
		case string:
			return types.NewBytesDatum([]byte(v)), nil
		}
		log.Panic("unknown data type when build datum",
			zap.Any("type", ft.GetType()), zap.Any("value", value), zap.Reflect("type", reflect.TypeOf(value)))
	case mysql.TypeFloat:
		return types.NewFloat32Datum(value.(float32)), nil
	case mysql.TypeDouble:
		return types.NewFloat64Datum(value.(float64)), nil
	case mysql.TypeTiDBVectorFloat32:
		return types.NewVectorFloat32Datum(value.(types.VectorFloat32)), nil
	default:
		log.Panic("unexpected mysql type found", zap.Any("type", ft.GetType()))
	}
	return types.Datum{}, nil
}

func verifyRawBytesChecksum(
	tableInfo *model.TableInfo, columns []*model.ColumnData, decoder *rowcodec.DatumMapDecoder,
	key kv.Key, tz *time.Location,
) (uint32, bool, error) {
	expected, ok := decoder.GetChecksum()
	if !ok {
		return 0, true, nil
	}
	var (
		columnIDs []int64
		datums    []*types.Datum
	)
	for _, col := range columns {
		// TiDB does not encode null value into the bytes, so just ignore it.
		if col.Value == nil {
			continue
		}
		columnID := col.ColumnID
		columnInfo := tableInfo.ForceGetColumnInfo(columnID)
		datum, err := newDatum(col.Value, columnInfo.FieldType)
		if err != nil {
			return 0, false, errors.Trace(err)
		}
		datums = append(datums, &datum)
		columnIDs = append(columnIDs, columnID)
	}
	obtained, err := decoder.CalculateRawChecksum(tz, columnIDs, datums, key, nil)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if obtained == expected {
		return expected, true, nil
	}

	log.Error("raw bytes checksum mismatch",
		zap.Uint32("expected", expected), zap.Uint32("obtained", obtained))

	return expected, false, nil
}

// return error when calculate the checksum.
// return false if the checksum is not matched
func (m *mounter) verifyChecksum(
	tableInfo *model.TableInfo, columnInfos []*timodel.ColumnInfo,
	columns []*model.ColumnData, rawColumns []types.Datum,
	key kv.Key, isPreRow bool,
) (uint32, bool, error) {
	if !m.integrity.Enabled() {
		return 0, true, nil
	}

	var decoder *rowcodec.DatumMapDecoder
	if isPreRow {
		decoder = m.preDecoder
	} else {
		decoder = m.decoder
	}

	version := decoder.ChecksumVersion()
	switch version {
	case 0:
		// skip old value checksum verification for the checksum v1, since it cannot handle
		// Update / Delete event correctly, after Add Column / Drop column DDL,
		// since the table schema does not contain complete column information.
		return m.verifyColumnChecksum(columnInfos, rawColumns, decoder, isPreRow)
	case 1:
		expected, matched, err := verifyRawBytesChecksum(tableInfo, columns, decoder, key, m.tz)
		if err != nil {
			return 0, false, errors.Trace(err)
		}
		if !matched {
			return expected, matched, err
		}
		columnChecksum, err := calculateColumnChecksum(columnInfos, rawColumns, m.tz)
		if err != nil {
			log.Error("failed to calculate column-level checksum, after raw checksum verification passed", zap.Error(err))
			return 0, false, errors.Trace(err)
		}
		return columnChecksum, true, nil
	default:
	}
	return 0, false, errors.Errorf("unknown checksum version %d", version)
}

func (m *mounter) mountRowKVEntry(
	tableInfo *model.TableInfo, row *rowKVEntry, key kv.Key, dataSize int64,
) (*model.RowChangedEvent, model.RowChangedDatums, error) {
	var (
		rawRow      model.RowChangedDatums
		columnInfos []*timodel.ColumnInfo
		matched     bool
		err         error

		checksum *integrity.Checksum

		checksumVersion int
		corrupted       bool
	)

	if m.decoder != nil {
		checksumVersion = m.decoder.ChecksumVersion()
	} else if m.preDecoder != nil {
		checksumVersion = m.preDecoder.ChecksumVersion()
	}

	// Decode previous columns.
	var (
		preCols     []*model.ColumnData
		preRawCols  []types.Datum
		preChecksum uint32
	)
	if row.PreRowExist {
		// FIXME(leoppro): using pre table info to mounter pre column datum
		// the pre column and current column in one event may using different table info
		preCols, preRawCols, columnInfos, err = datum2Column(tableInfo, row.PreRow, m.tz)
		if err != nil {
			return nil, rawRow, errors.Trace(err)
		}

		preChecksum, matched, err = m.verifyChecksum(tableInfo, columnInfos, preCols, preRawCols, key, true)
		if err != nil {
			log.Error("calculate the previous columns checksum failed",
				zap.Any("tableInfo", tableInfo),
				zap.Any("rawCols", preRawCols))
			return nil, rawRow, errors.Trace(err)
		}

		if !matched {
			log.Error("previous columns checksum mismatch",
				zap.Uint32("checksum", preChecksum),
				zap.Any("tableInfo", tableInfo),
				zap.Any("rawCols", preRawCols))
			if m.integrity.ErrorHandle() {
				return nil, rawRow, cerror.ErrCorruptedDataMutation.
					GenWithStackByArgs(m.changefeedID.Namespace, m.changefeedID.ID)
			}
			corrupted = true
		}
	}

	var (
		cols            []*model.ColumnData
		rawCols         []types.Datum
		currentChecksum uint32
	)
	if row.RowExist {
		cols, rawCols, columnInfos, err = datum2Column(tableInfo, row.Row, m.tz)
		if err != nil {
			return nil, rawRow, errors.Trace(err)
		}

		currentChecksum, matched, err = m.verifyChecksum(tableInfo, columnInfos, cols, rawCols, key, false)
		if err != nil {
			log.Error("calculate the current columns checksum failed",
				zap.Any("tableInfo", tableInfo),
				zap.Any("rawCols", rawCols))
			return nil, rawRow, errors.Trace(err)
		}
		if !matched {
			log.Error("current columns checksum mismatch",
				zap.Uint32("checksum", currentChecksum),
				zap.Any("tableInfo", tableInfo),
				zap.Any("rawCols", rawCols))
			if m.integrity.ErrorHandle() {
				return nil, rawRow, cerror.ErrCorruptedDataMutation.
					GenWithStackByArgs(m.changefeedID.Namespace, m.changefeedID.ID)
			}
			corrupted = true
		}
	}

	var intRowID int64
	if row.RecordID.IsInt() {
		intRowID = row.RecordID.IntValue()
	}

	rawRow.PreRowDatums = preRawCols
	rawRow.RowDatums = rawCols

	// if both are 0, it means the checksum is not enabled
	// so the checksum is nil to reduce memory allocation.
	if preChecksum != 0 || currentChecksum != 0 {
		checksum = &integrity.Checksum{
			Current:   currentChecksum,
			Previous:  preChecksum,
			Corrupted: corrupted,
			Version:   checksumVersion,
		}
	}

	return &model.RowChangedEvent{
		StartTs:         row.StartTs,
		CommitTs:        row.CRTs,
		RowID:           intRowID,
		HandleKey:       row.RecordID,
		PhysicalTableID: row.PhysicalTableID,
		TableInfo:       tableInfo,
		Columns:         cols,
		PreColumns:      preCols,

		Checksum: checksum,

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
		v, err := datum.GetBinaryLiteral().ToInt(types.DefaultStmtNoWarningContext)
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
	case mysql.TypeTiDBVectorFloat32:
		b := datum.GetVectorFloat32()
		return b, b.Len(), "", nil
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
func getDefaultOrZeroValue(
	col *timodel.ColumnInfo, tz *time.Location,
) (types.Datum, any, int, string, error) {
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
		d, err = datum.ConvertTo(types.DefaultStmtNoWarningContext, &col.FieldType)
		if err != nil {
			return d, d.GetValue(), sizeOfDatum(d), "", errors.Trace(err)
		}
		switch col.GetType() {
		case mysql.TypeTimestamp:
			t := d.GetMysqlTime()
			err = t.ConvertTimeZone(time.UTC, tz)
			if err != nil {
				return d, d.GetValue(), sizeOfDatum(d), "", errors.Trace(err)
			}
			d.SetMysqlTime(t)
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

// DecodeTableID decodes the raw key to a table ID
func DecodeTableID(key []byte) (model.TableID, error) {
	_, physicalTableID, err := decodeTableID(key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return physicalTableID, nil
}
