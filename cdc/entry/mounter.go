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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/mvpninjas/go-bitflag"
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
	Row map[int64]types.Datum
}

type indexKVEntry struct {
	baseKVEntry
	IndexID    int64
	IndexValue []types.Datum
}

func (idx *indexKVEntry) unflatten(tableInfo *TableInfo, tz *time.Location) error {
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
}

// NewMounter creates a mounter
func NewMounter(schemaStorage *SchemaStorage, workerNum int) Mounter {
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
			rowKV, err := m.unmarshalRowKVEntry(tableInfo, key, raw.Value, baseInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if rowKV == nil {
				return nil, nil
			}
			return m.mountRowKVEntry(tableInfo, rowKV)
		case bytes.HasPrefix(key, indexPrefix):
			indexKV, err := m.unmarshalIndexKVEntry(key, raw.Value, baseInfo)
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

func (m *mounterImpl) unmarshalRowKVEntry(tableInfo *TableInfo, restKey []byte, rawValue []byte, base baseKVEntry) (*rowKVEntry, error) {
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
	base.RecordID = recordID
	return &rowKVEntry{
		baseKVEntry: base,
		Row:         row,
	}, nil
}

func (m *mounterImpl) unmarshalIndexKVEntry(restKey []byte, rawValue []byte, base baseKVEntry) (*indexKVEntry, error) {
	// skip set index KV
	if !base.Delete {
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

func (m *mounterImpl) mountRowKVEntry(tableInfo *TableInfo, row *rowKVEntry) (*model.RowChangedEvent, error) {
	if row.Delete && !tableInfo.PKIsHandle {
		return nil, nil
	}

	datumsNum := 1
	if !row.Delete {
		datumsNum = len(tableInfo.Columns)
	}

	values := make(map[string]*model.Column, datumsNum)
	for index, colValue := range row.Row {
		colInfo, exist := tableInfo.GetColumnInfo(index)
		if !exist {
			return nil, errors.NotFoundf("column info, colID: %d", index)
		}
		if !row.Delete && !tableInfo.IsColWritable(colInfo) {
			continue
		}
		colName := colInfo.Name.O
		value, err := formatColVal(colValue, colInfo.Tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var flag bitflag.Flag
		if colInfo.Charset == "binary" {
			flag.Set(model.BinaryFlag)
		}
		col := &model.Column{
			Type:  colInfo.Tp,
			Value: value,
			Flag:  flag,
		}
		if tableInfo.IsColumnUnique(colInfo.ID) {
			whereHandle := true
			col.WhereHandle = &whereHandle
		}
		values[colName] = col
	}
	var partitionID int64
	if tableInfo.GetPartitionInfo() != nil {
		partitionID = row.PhysicalTableID
	}

	event := &model.RowChangedEvent{
		StartTs:       row.StartTs,
		CommitTs:      row.CRTs,
		RowID:         row.RecordID,
		SchemaID:      tableInfo.SchemaID,
		TableUpdateTs: tableInfo.UpdateTS,
		Table: &model.TableName{
			Schema:    tableInfo.TableName.Schema,
			Table:     tableInfo.TableName.Table,
			Partition: partitionID,
		},
		IndieMarkCol: tableInfo.IndieMarkCol,
	}

	if !row.Delete {
		for _, col := range tableInfo.Columns {
			_, ok := values[col.Name.O]
			if !ok && tableInfo.IsColWritable(col) {
				var flag bitflag.Flag
				if col.Charset == "binary" {
					flag.Set(model.BinaryFlag)
				}
				column := &model.Column{
					Type:  col.Tp,
					Value: getDefaultOrZeroValue(col),
					Flag:  flag,
				}
				if tableInfo.IsColumnUnique(col.ID) {
					whereHandle := true
					column.WhereHandle = &whereHandle
				}
				values[col.Name.O] = column
			}
		}
	}
	event.Delete = row.Delete
	event.Columns = values
	event.Keys = genMultipleKeys(tableInfo.TableInfo, values, quotes.QuoteSchema(event.Table.Schema, event.Table.Table))
	return event, nil
}

func (m *mounterImpl) mountIndexKVEntry(tableInfo *TableInfo, idx *indexKVEntry) (*model.RowChangedEvent, error) {
	// skip set index KV
	if !idx.Delete {
		return nil, nil
	}

	indexInfo, exist := tableInfo.GetIndexInfo(idx.IndexID)
	if !exist {
		return nil, errors.NotFoundf("index info %d", idx.IndexID)
	}

	if !tableInfo.IsIndexUnique(indexInfo) {
		return nil, nil
	}

	err := idx.unflatten(tableInfo, m.tz)
	if err != nil {
		return nil, errors.Trace(err)
	}

	values := make(map[string]*model.Column, len(idx.IndexValue))
	for i, idxCol := range indexInfo.Columns {
		value, err := formatColVal(idx.IndexValue[i], tableInfo.Columns[idxCol.Offset].Tp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		whereHandle := true
		values[idxCol.Name.O] = &model.Column{
			Type:        tableInfo.Columns[idxCol.Offset].Tp,
			WhereHandle: &whereHandle,
			Value:       value,
		}
	}
	return &model.RowChangedEvent{
		StartTs:  idx.StartTs,
		CommitTs: idx.CRTs,
		RowID:    idx.RecordID,
		SchemaID: tableInfo.SchemaID,
		Table: &model.TableName{
			Schema: tableInfo.TableName.Schema,
			Table:  tableInfo.TableName.Table,
		},
		IndieMarkCol: tableInfo.IndieMarkCol,
		Delete:       true,
		Columns:      values,
		Keys:         genMultipleKeys(tableInfo.TableInfo, values, quotes.QuoteSchema(tableInfo.TableName.Schema, tableInfo.TableName.Table)),
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
	case mysql.TypeNewDecimal, mysql.TypeDecimal:
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

func fetchHandleValue(tableInfo *TableInfo, recordID int64) (pkCoID int64, pkValue *types.Datum, err error) {
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

func genMultipleKeys(ti *timodel.TableInfo, values map[string]*model.Column, table string) []string {
	multipleKeys := make([]string, 0, len(ti.Indices)+1)
	if ti.PKIsHandle {
		if pk := ti.GetPkColInfo(); pk != nil && !pk.IsGenerated() {
			cols := []*timodel.ColumnInfo{pk}
			key := genKeyList(table, cols, values)
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
		key := genKeyList(table, cols, values)
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

	if len(multipleKeys) == 0 {
		// use table name as key if no key generated (no PK/UK),
		// no concurrence for rows in the same table.
		log.L().Debug("use table name as the key", zap.String("table", table))
		multipleKeys = append(multipleKeys, table)
	}

	return multipleKeys
}

func columnValue(value interface{}) string {
	var data string
	switch v := value.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(int64(v), 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(uint64(v), 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func genKeyList(table string, columns []*timodel.ColumnInfo, values map[string]*model.Column) string {
	var buf strings.Builder
	for _, col := range columns {
		val, ok := values[col.Name.O]
		if !ok || val.Value == nil {
			log.L().Debug("ignore null value", zap.String("column", col.Name.O), zap.String("table", table))
			continue // ignore `null` value.
		}
		buf.WriteString(columnValue(val.Value))
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
