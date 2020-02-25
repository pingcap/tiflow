package entry

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

// Mounter is used to parse SQL events from KV events
type Mounter struct {
	schemaStorage   *Storage
	schemaBuilder   *StorageBuilder
	rawRowChangedCh <-chan *model.RawKVEntry
	output          chan *model.RowChangedEvent
}

// NewMounter creates a mounter
func NewMounter(rawRowChangedCh <-chan *model.RawKVEntry, schemaBuilder *StorageBuilder) *Mounter {
	return &Mounter{
		schemaBuilder:   schemaBuilder,
		rawRowChangedCh: rawRowChangedCh,
	}
}

func (m *Mounter) Run(ctx context.Context) error {
	for {
		var rawRow *model.RawKVEntry
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case rawRow = <-m.rawRowChangedCh:
		}
		if rawRow.OpType == model.OpTypeResolved {
			m.output <- &model.RowChangedEvent{Resolved: true, Ts: rawRow.Ts}
			continue
		}

		if m.schemaStorage == nil {
			m.schemaStorage = m.schemaBuilder.Build(rawRow.Ts)
		}
		err := m.schemaStorage.HandlePreviousDDLJobIfNeed(rawRow.Ts)
		if err != nil {
			return errors.Trace(err)
		}

		event, err := m.unmarshalAndMountRowChanged(rawRow)
		if err != nil {
			return errors.Trace(err)
		}
		if event == nil {
			continue
		}
		m.output <- event
	}
}

func (m *Mounter) Output() <-chan *model.RowChangedEvent {
	return m.output
}

func (m *Mounter) unmarshalAndMountRowChanged(raw *model.RawKVEntry) (*model.RowChangedEvent, error) {
	if !bytes.HasPrefix(raw.Key, tablePrefix) {
		return nil, nil
	}
	key, tableID, err := decodeTableID(raw.Key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	baseInfo := baseKVEntry{
		Ts:      raw.Ts,
		TableID: tableID,
		Delete:  raw.OpType == model.OpTypeDelete,
	}
	switch {
	case bytes.HasPrefix(key, recordPrefix):
		rowKV, err := m.unmarshalRowKVEntry(key, raw.Value, baseInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowKV == nil {
			return nil, nil
		}
		return m.mountRowKVEntry(rowKV)
	case bytes.HasPrefix(key, indexPrefix):
		indexKV, err := m.unmarshalIndexKVEntry(key, raw.Value, baseInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if indexKV == nil {
			return nil, nil
		}
		return m.mountIndexKVEntry(indexKV)
	}
	return nil, nil
}

func (m *Mounter) unmarshalRowKVEntry(restKey []byte, rawValue []byte, base baseKVEntry) (*rowKVEntry, error) {
	tableID := base.TableID
	tableInfo, exist := m.schemaStorage.TableByID(tableID)
	if !exist {
		if m.schemaStorage.IsTruncateTableID(tableID) {
			log.Debug("skip the DML of truncated table", zap.Uint64("ts", base.Ts), zap.Int64("tableID", tableID))
			return nil, nil
		}
		return nil, errors.NotFoundf("table in schema storage, id: %d", tableID)
	}

	key, recordID, err := decodeRecordID(restKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(key) != 0 {
		return nil, errors.New("invalid record key")
	}
	row, err := decodeRow(rawValue, recordID, tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	base.RecordID = recordID
	return &rowKVEntry{
		baseKVEntry: base,
		Row:         row,
	}, nil
}

func (m *Mounter) unmarshalIndexKVEntry(restKey []byte, rawValue []byte, base baseKVEntry) (*indexKVEntry, error) {
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

func (m *Mounter) mountRowKVEntry(row *rowKVEntry) (*model.RowChangedEvent, error) {
	tableInfo, tableName, exist := m.fetchTableInfo(row.TableID)
	if !exist {
		return nil, errors.NotFoundf("table in schema storage, id: %d", row.TableID)
	}

	if row.Delete && !tableInfo.PKIsHandle {
		return nil, nil
	}

	datumsNum := 1
	if !row.Delete {
		datumsNum = len(tableInfo.Columns)
	}
	values := make(map[string]model.Column, datumsNum)
	for index, colValue := range row.Row {
		colInfo, exist := tableInfo.GetColumnInfo(index)
		if !exist {
			return nil, errors.NotFoundf("column info, colID: %d", index)
		}
		colName := colInfo.Name.O
		//TODO formatColVal
		values[colName] = model.Column{
			Type:   colInfo.Tp,
			Unique: tableInfo.IsColumnUnique(colInfo.ID),
			Value:  colValue.GetValue(),
		}
	}

	event := &model.RowChangedEvent{
		Ts:       row.Ts,
		Resolved: false,
		Schema:   tableName.Schema,
		Table:    tableName.Table,
	}

	if row.Delete {
		event.Delete = values
	} else {
		for _, col := range tableInfo.Columns {
			_, ok := values[col.Name.O]
			if !ok {
				values[col.Name.O] = model.Column{
					Type:   col.Tp,
					Unique: tableInfo.IsColumnUnique(col.ID),
					Value:  getDefaultOrZeroValue(col),
				}
			}
		}
		event.Update = values
	}
	return event, nil
}

func (m *Mounter) mountIndexKVEntry(idx *indexKVEntry) (*model.RowChangedEvent, error) {
	// skip set index KV
	if !idx.Delete {
		return nil, nil
	}
	tableInfo, tableName, exist := m.fetchTableInfo(idx.TableID)
	if !exist {
		if m.schemaStorage.IsTruncateTableID(idx.TableID) {
			log.Debug("skip the DML of truncated table", zap.Uint64("ts", idx.Ts), zap.Int64("tableID", idx.TableID))
			return nil, nil
		}
		return nil, errors.NotFoundf("table in schema storage, id: %d", idx.TableID)
	}
	indexInfo, exist := tableInfo.GetIndexInfo(idx.IndexID)
	if !exist {
		return nil, errors.NotFoundf("index info %d", idx.IndexID)
	}

	if !tableInfo.IsIndexUnique(indexInfo) {
		return nil, nil
	}

	err := idx.unflatten(tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	values := make(map[string]model.Column, len(idx.IndexValue))
	for i, idxCol := range indexInfo.Columns {
		values[idxCol.Name.O] = model.Column{
			Type:   tableInfo.Columns[idxCol.Offset].Tp,
			Unique: true,
			Value:  idx.IndexValue[i].GetValue(),
		}
	}
	return &model.RowChangedEvent{
		Ts:       idx.Ts,
		Resolved: false,
		Schema:   tableName.Schema,
		Table:    tableName.Table,
		Delete:   values,
	}, nil
}

func getDefaultOrZeroValue(col *timodel.ColumnInfo) interface{} {
	// see https://github.com/pingcap/tidb/issues/9304
	// must use null if TiDB not write the column value when default value is null
	// and the value is null
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.NewDatum(nil).GetValue()
	}

	if col.GetDefaultValue() != nil {
		return types.NewDatum(col.GetDefaultValue()).GetValue()
	}

	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		return types.NewDatum(col.FieldType.Elems[0]).GetValue()
	}

	return table.GetZeroValue(col).GetValue()
}

func (m *Mounter) fetchTableInfo(tableID int64) (tableInfo *TableInfo, tableName TableName, exist bool) {
	tableInfo, exist = m.schemaStorage.TableByID(tableID)
	if !exist {
		return
	}
	tableName, exist = m.schemaStorage.GetTableNameByID(tableID)
	return
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
