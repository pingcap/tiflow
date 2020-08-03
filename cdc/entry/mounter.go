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
	"encoding/binary"
	"encoding/json"
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/quotes"
	"github.com/pingcap/ticdc/pkg/util"
)

const (
	defaultOutputChanSize = 128000
)

type baseKVEntry struct {
	StartTs uint64
	// Commit or resolved TS
	CRTs uint64

	PhysicalTableID int64
	RecordID        int64
	Delete          bool
}

type rowKVEntry struct {
	baseKVEntry
	Row    map[int64]types.Datum
	PreRow map[int64]types.Datum
}

type indexKVEntry struct {
	baseKVEntry
	IndexID    int64
	IndexValue []types.Datum
}

func (idx *indexKVEntry) unflatten(tableInfo *model.TableInfo, tz *time.Location) error {
	if tableInfo.ID != idx.PhysicalTableID {
		isPartition := false
		if pi := tableInfo.GetPartitionInfo(); pi != nil {
			for _, p := range pi.Definitions {
				if p.ID == idx.PhysicalTableID {
					isPartition = true
					break
				}
			}
		}
		if !isPartition {
			return errors.New("wrong table info in unflatten")
		}
	}
	index, exist := tableInfo.GetIndexInfo(idx.IndexID)
	if !exist {
		return errors.NotFoundf("index info, indexID: %d", idx.IndexID)
	}
	if !isDistinct(index, idx.IndexValue) {
		idx.RecordID = idx.IndexValue[len(idx.IndexValue)-1].GetInt64()
		idx.IndexValue = idx.IndexValue[:len(idx.IndexValue)-1]
	}
	for i, v := range idx.IndexValue {
		colOffset := index.Columns[i].Offset
		fieldType := &tableInfo.Columns[colOffset].FieldType
		datum, err := unflatten(v, fieldType, tz)
		if err != nil {
			return errors.Trace(err)
		}
		idx.IndexValue[i] = datum
	}
	return nil
}

func isDistinct(index *timodel.IndexInfo, indexValue []types.Datum) bool {
	if index.Primary {
		return true
	}
	if index.Unique {
		for _, value := range indexValue {
			if value.IsNull() {
				return false
			}
		}
		return true
	}
	return false
}

// Mounter is used to parse SQL events from KV events
type Mounter interface {
	Run(ctx context.Context) error
	Input() chan<- *model.PolymorphicEvent
}

type mounterImpl struct {
	schemaStorage    *SchemaStorage
	rawRowChangedChs []chan *model.PolymorphicEvent
	tz               *time.Location
	workerNum        int
	enableOldValue   bool
}

// NewMounter creates a mounter
func NewMounter(schemaStorage *SchemaStorage, workerNum int, enableOldValue bool) Mounter {
	if workerNum <= 0 {
		workerNum = defaultMounterWorkerNum
	}
	chs := make([]chan *model.PolymorphicEvent, workerNum)
	for i := 0; i < workerNum; i++ {
		chs[i] = make(chan *model.PolymorphicEvent, defaultOutputChanSize)
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
	errg.Go(func() error {
		m.collectMetrics(ctx)
		return nil
	})
	for i := 0; i < m.workerNum; i++ {
		index := i
		errg.Go(func() error {
			return m.codecWorker(ctx, index)
		})
	}
	return errg.Wait()
}

func (m *mounterImpl) codecWorker(ctx context.Context, index int) error {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricMountDuration := mountDuration.WithLabelValues(captureAddr, changefeedID)

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
		pEvent.RawKV.Key = nil
		pEvent.RawKV.Value = nil
		pEvent.PrepareFinished()
		metricMountDuration.Observe(time.Since(startTime).Seconds())
	}
}

func (m *mounterImpl) Input() chan<- *model.PolymorphicEvent {
	return m.rawRowChangedChs[rand.Intn(m.workerNum)]
}

func (m *mounterImpl) collectMetrics(ctx context.Context) {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricMounterInputChanSize := mounterInputChanSizeGauge.WithLabelValues(captureAddr, changefeedID)

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 15):
			chSize := 0
			for _, ch := range m.rawRowChangedChs {
				chSize += len(ch)
			}
			metricMounterInputChanSize.Set(float64(chSize))
		}
	}
}

func (m *mounterImpl) unmarshalAndMountRowChanged(ctx context.Context, raw *model.RawKVEntry) (*model.RowChangedEvent, error) {
	if !bytes.HasPrefix(raw.Key, tablePrefix) {
		return nil, nil
	}
	key, physicalTableID, err := decodeTableID(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	baseInfo := baseKVEntry{
		StartTs:         raw.StartTs,
		CRTs:            raw.CRTs,
		PhysicalTableID: physicalTableID,
		Delete:          raw.OpType == model.OpTypeDelete,
	}
	snap, err := m.schemaStorage.GetSnapshot(ctx, raw.CRTs)
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
			return nil, errors.NotFoundf("table in schema storage, id: %d", physicalTableID)
		}
		switch {
		case bytes.HasPrefix(key, recordPrefix):
			rowKV, err := m.unmarshalRowKVEntry(tableInfo, key, raw.Value, raw.OldValue, baseInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if rowKV == nil {
				return nil, nil
			}
			return m.mountRowKVEntry(tableInfo, rowKV)
		case bytes.HasPrefix(key, indexPrefix):
			indexKV, err := m.unmarshalIndexKVEntry(key, raw.Value, raw.OldValue, baseInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if indexKV == nil {
				return nil, nil
			}
			return m.mountIndexKVEntry(tableInfo, indexKV)
		}
		return nil, nil
	}()
	if err != nil {
		log.Error("failed to mount and unmarshals entry, start to print debug info", zap.Error(err))
		snap.PrintStatus(log.Error)
	}
	return row, err
}

func (m *mounterImpl) unmarshalRowKVEntry(tableInfo *model.TableInfo, restKey []byte, rawValue []byte, rawOldValue []byte, base baseKVEntry) (*rowKVEntry, error) {
	key, recordID, err := decodeRecordID(restKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(key) != 0 {
		return nil, errors.New("invalid record key")
	}
	row, err := decodeRow(rawValue, recordID, tableInfo, m.tz)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var preRow map[int64]types.Datum
	if rawOldValue != nil {
		preRow, err = decodeRow(rawOldValue, recordID, tableInfo, m.tz)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	base.RecordID = recordID
	return &rowKVEntry{
		baseKVEntry: base,
		Row:         row,
		PreRow:      preRow,
	}, nil
}

func (m *mounterImpl) unmarshalIndexKVEntry(restKey []byte, rawValue []byte, rawOldValue []byte, base baseKVEntry) (*indexKVEntry, error) {
	// Skip set index KV.
	// By default we cannot get the old value of a deleted row, then we must get the value of unique key
	// or primary key for seeking the deleted row through its index key.
	// After the old value was enabled, we can skip the index key.
	if !base.Delete || m.enableOldValue {
		return nil, nil
	}

	indexID, indexValue, err := decodeIndexKey(restKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var recordID int64

	if len(rawValue) == 8 {
		// primary key or unique index
		buf := bytes.NewBuffer(rawValue)
		err = binary.Read(buf, binary.BigEndian, &recordID)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	base.RecordID = recordID
	return &indexKVEntry{
		baseKVEntry: base,
		IndexID:     indexID,
		IndexValue:  indexValue,
	}, nil
}

const ddlJobListKey = "DDLJobList"
const ddlAddIndexJobListKey = "DDLJobAddIdxList"

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
	if !job.IsDone() && !job.IsSynced() {
		return nil, nil
	}
	// FinishedTS is only set when the job is synced,
	// but we can use the entry's ts here
	job.StartTS = raw.StartTs
	job.BinlogInfo.FinishedTS = raw.CRTs
	return job, nil
}

func datum2Column(tableInfo *model.TableInfo, datums map[int64]types.Datum, fillWithDefaultValue bool) (map[string]*model.Column, error) {
	estimateLen := len(datums)
	if fillWithDefaultValue {
		estimateLen = len(tableInfo.Columns)
	}
	cols := make(map[string]*model.Column, estimateLen)
	for index, colValue := range datums {
		colInfo, exist := tableInfo.GetColumnInfo(index)
		if !exist {
			return nil, errors.NotFoundf("column info, colID: %d", index)
		}
		if !tableInfo.IsColCDCVisible(colInfo) {
			continue
		}
		colName := colInfo.Name.O
		value, err := formatColVal(colValue, colInfo.Tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		col := &model.Column{
			Type:  colInfo.Tp,
			Value: value,
			Flag:  transColumnFlag(colInfo),
		}
		if tableInfo.IsColumnUnique(colInfo.ID) {
			whereHandle := true
			col.WhereHandle = &whereHandle
		}
		cols[colName] = col
	}
	if fillWithDefaultValue {
		for _, col := range tableInfo.Columns {
			_, ok := cols[col.Name.O]
			if !ok && tableInfo.IsColCDCVisible(col) {
				column := &model.Column{
					Type:  col.Tp,
					Value: getDefaultOrZeroValue(col),
					Flag:  transColumnFlag(col),
				}
				if tableInfo.IsColumnUnique(col.ID) {
					whereHandle := true
					column.WhereHandle = &whereHandle
				}
				cols[col.Name.O] = column
			}
		}
	}

	err := setHandleKeyFlag(tableInfo, cols)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cols, nil
}

func (m *mounterImpl) mountRowKVEntry(tableInfo *model.TableInfo, row *rowKVEntry) (*model.RowChangedEvent, error) {
	// if m.enableOldValue == true, go into this function
	// if m.enableNewValue == false and row.Delete == false, go into this function
	// if m.enableNewValue == false and row.Delete == true and tableInfo.PKIsHandle = true, go into this function
	// only if m.enableNewValue == false and row.Delete == true and tableInfo.PKIsHandle == false, skip this function
	if !m.enableOldValue && row.Delete && !tableInfo.PKIsHandle {
		return nil, nil
	}

	var err error
	// Decode previous columns.
	var preCols map[string]*model.Column
	if len(row.PreRow) != 0 {
		// FIXME(leoppro): using pre table info to mounter pre column datum
		// the pre column and current column in one event may using different table info
		preCols, err = datum2Column(tableInfo, row.PreRow, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	var cols map[string]*model.Column
	oldValueDisabledAndRowIsDelete := !m.enableOldValue && row.Delete
	cols, err = datum2Column(tableInfo, row.Row, !oldValueDisabledAndRowIsDelete)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if oldValueDisabledAndRowIsDelete {
		preCols = cols
		cols = nil
	}

	var partitionID int64
	if tableInfo.GetPartitionInfo() != nil {
		partitionID = row.PhysicalTableID
	}

	schemaName := tableInfo.TableName.Schema
	tableName := tableInfo.TableName.Table
	return &model.RowChangedEvent{
		StartTs:          row.StartTs,
		CommitTs:         row.CRTs,
		RowID:            row.RecordID,
		TableInfoVersion: tableInfo.TableInfoVersion,
		Table: &model.TableName{
			Schema:    schemaName,
			Table:     tableName,
			Partition: partitionID,
		},
		IndieMarkCol: tableInfo.IndieMarkCol,
		Delete:       row.Delete,
		Columns:      cols,
		PreColumns:   preCols,
		// FIXME(leoppor): Correctness of conflict detection with old values
		Keys: genMultipleKeys(tableInfo.TableInfo, preCols, cols, quotes.QuoteSchema(schemaName, tableName)),
	}, nil
}

func setHandleKeyFlag(tableInfo *model.TableInfo, colValues map[string]*model.Column) error {
	switch tableInfo.HandleIndexID {
	case model.HandleIndexTableIneligible:
		log.Fatal("this table is not eligible", zap.Int64("tableID", tableInfo.ID))
	case model.HandleIndexPKIsHandle:
		// pk is handle
		if !tableInfo.PKIsHandle {
			log.Fatal("the pk of this table is not handle", zap.Int64("tableID", tableInfo.ID))
		}
		for _, colInfo := range tableInfo.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				colValues[colInfo.Name.O].Flag.SetIsHandleKey()
				break
			}
		}
	default:
		handleIndexInfo, exist := tableInfo.GetIndexInfo(tableInfo.HandleIndexID)
		if !exist {
			return errors.NotFoundf("handle index info(%d) in table(%d)", tableInfo.HandleIndexID, tableInfo.ID)
		}
		for _, colInfo := range handleIndexInfo.Columns {
			colName := tableInfo.Columns[colInfo.Offset].Name.O
			colValues[colName].Flag.SetIsHandleKey()
		}
	}
	return nil
}

func (m *mounterImpl) mountIndexKVEntry(tableInfo *model.TableInfo, idx *indexKVEntry) (*model.RowChangedEvent, error) {
	// skip set index KV
	if !idx.Delete || m.enableOldValue {
		return nil, nil
	}
	// skip any index that is not the handle
	if idx.IndexID != tableInfo.HandleIndexID {
		return nil, nil
	}

	indexInfo, exist := tableInfo.GetIndexInfo(idx.IndexID)
	if !exist {
		log.Warn("index info not found", zap.Int64("indexID", idx.IndexID))
		return nil, nil
	}

	if !tableInfo.IsIndexUnique(indexInfo) {
		return nil, nil
	}

	err := idx.unflatten(tableInfo, m.tz)
	if err != nil {
		return nil, errors.Trace(err)
	}

	preCols := make(map[string]*model.Column, len(idx.IndexValue))
	for i, idxCol := range indexInfo.Columns {
		value, err := formatColVal(idx.IndexValue[i], tableInfo.Columns[idxCol.Offset].Tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		whereHandle := true
		col := &model.Column{
			Type:        tableInfo.Columns[idxCol.Offset].Tp,
			WhereHandle: &whereHandle,
			Value:       value,
			Flag:        transColumnFlag(tableInfo.Columns[idxCol.Offset]),
		}
		col.Flag.SetIsHandleKey()
		preCols[idxCol.Name.O] = col
	}
	return &model.RowChangedEvent{
		StartTs:  idx.StartTs,
		CommitTs: idx.CRTs,
		RowID:    idx.RecordID,
		Table: &model.TableName{
			Schema: tableInfo.TableName.Schema,
			Table:  tableInfo.TableName.Table,
		},
		IndieMarkCol: tableInfo.IndieMarkCol,
		Delete:       true,
		PreColumns:   preCols,
		Keys:         genMultipleKeys(tableInfo.TableInfo, preCols, nil, quotes.QuoteSchema(tableInfo.TableName.Schema, tableInfo.TableName.Table)),
	}, nil
}

func formatColVal(datum types.Datum, tp byte) (interface{}, error) {

	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		return datum.GetMysqlTime().String(), nil
	case mysql.TypeDuration:
		return datum.GetMysqlDuration().String(), nil
	case mysql.TypeJSON:
		return datum.GetMysqlJSON().String(), nil
	case mysql.TypeNewDecimal:
		v := datum.GetMysqlDecimal()
		if v == nil {
			return nil, nil
		}
		return v.String(), nil
	case mysql.TypeEnum:
		return datum.GetMysqlEnum().Value, nil
	case mysql.TypeSet:
		return datum.GetMysqlSet().Value, nil
	case mysql.TypeBit:
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		return datum.GetBinaryLiteral().ToInt(nil)
	default:
		return datum.GetValue(), nil
	}
}

func getDefaultOrZeroValue(col *timodel.ColumnInfo) interface{} {
	// see https://github.com/pingcap/tidb/issues/9304
	// must use null if TiDB not write the column value when default value is null
	// and the value is null
	if !mysql.HasNotNullFlag(col.Flag) {
		d := types.NewDatum(nil)
		return d.GetValue()
	}

	if col.GetDefaultValue() != nil {
		d := types.NewDatum(col.GetDefaultValue())
		return d.GetValue()
	}

	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		d := types.NewDatum(col.FieldType.Elems[0])
		return d.GetValue()
	}

	d := table.GetZeroValue(col)
	return d.GetValue()
}

func fetchHandleValue(tableInfo *model.TableInfo, recordID int64) (pkCoID int64, pkValue *types.Datum, err error) {
	handleColOffset := -1
	for i, col := range tableInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			handleColOffset = i
			break
		}
	}
	if handleColOffset == -1 {
		return -1, nil, errors.New("can't find handle column, please check if the pk is handle")
	}
	handleCol := tableInfo.Columns[handleColOffset]
	pkCoID = handleCol.ID
	pkValue = &types.Datum{}
	if mysql.HasUnsignedFlag(handleCol.Flag) {
		pkValue.SetUint64(uint64(recordID))
	} else {
		pkValue.SetInt64(recordID)
	}
	return
}

func genMultipleKeys(ti *timodel.TableInfo, preCols, cols map[string]*model.Column, table string) []string {
	estimateLen := len(ti.Indices) + 1
	if len(preCols) != 0 && len(cols) != 0 {
		estimateLen *= 2
	}
	multipleKeys := make([]string, 0, estimateLen)
	buildKeys := func(colValues map[string]*model.Column) {
		if len(colValues) == 0 {
			return
		}
		if ti.PKIsHandle {
			if pk := ti.GetPkColInfo(); pk != nil && !pk.IsGenerated() {
				cols := []*timodel.ColumnInfo{pk}

				key := genKeyList(table, cols, colValues)
				if len(key) > 0 { // ignore `null` value.
					multipleKeys = append(multipleKeys, key)
				} else {
					log.L().Debug("ignore empty primary key", zap.String("table", table))
				}
			}
		}

		for _, indexCols := range ti.Indices {
			if !indexCols.Unique {
				continue
			}
			cols := getIndexColumns(ti.Columns, indexCols)
			key := genKeyList(table, cols, colValues)
			if len(key) > 0 { // ignore `null` value.
				noGeneratedColumn := true
				for _, col := range cols {
					if col.IsGenerated() {
						noGeneratedColumn = false
						break
					}
				}
				// If the index contain generated column, we can't use this key to detect conflict with other DML,
				// Because such as insert can't specified the generated value.
				if noGeneratedColumn {
					multipleKeys = append(multipleKeys, key)
				}
			} else {
				log.L().Debug("ignore empty index key", zap.String("table", table))
			}
		}
	}
	buildKeys(preCols)
	buildKeys(cols)

	if len(multipleKeys) == 0 {
		// use table name as key if no key generated (no PK/UK),
		// no concurrence for rows in the same table.
		log.L().Debug("use table name as the key", zap.String("table", table))
		multipleKeys = append(multipleKeys, table)
	}

	return multipleKeys
}

func transColumnFlag(col *timodel.ColumnInfo) model.ColumnFlagType {
	var flag model.ColumnFlagType
	if col.Charset == "binary" {
		flag.SetIsBinary()
	}
	if col.IsGenerated() {
		flag.SetIsGeneratedColumn()
	}
	return flag
}

func genKeyList(table string, columns []*timodel.ColumnInfo, values map[string]*model.Column) string {
	var buf strings.Builder
	for _, col := range columns {
		val, ok := values[col.Name.O]
		if !ok || val.Value == nil {
			log.L().Debug("ignore null value", zap.String("column", col.Name.O), zap.String("table", table))
			continue // ignore `null` value.
		}
		buf.WriteString(model.ColumnValueString(val.Value))
	}
	if buf.Len() == 0 {
		log.L().Debug("all value are nil, no key generated", zap.String("table", table))
		return "" // all values are `null`.
	}

	buf.WriteString(table)
	return buf.String()
}

func getIndexColumns(columns []*timodel.ColumnInfo, indexColumns *timodel.IndexInfo) []*timodel.ColumnInfo {
	cols := make([]*timodel.ColumnInfo, 0, len(indexColumns.Columns))
	for _, column := range indexColumns.Columns {
		cols = append(cols, columns[column.Offset])
	}
	return cols
}
