package entry

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

// Mounter is used to parse SQL events from KV events
type Mounter struct {
	schemaStorage *schema.Storage
}

// NewTxnMounter creates a mounter
func NewTxnMounter(schema *schema.Storage) *Mounter {
	return &Mounter{schemaStorage: schema}
}

// Mount parses a raw transaction and returns a transaction
func (m *Mounter) Mount(rawTxn model.RawTxn) (model.Txn, error) {
	t := model.Txn{
		Ts: rawTxn.Ts,
	}
	var replaceDMLs, deleteDMLs []*model.DML
	for _, raw := range rawTxn.Entries {
		kvEntry, err := unmarshal(raw)
		if err != nil {
			return model.Txn{}, errors.Trace(err)
		}

		switch e := kvEntry.(type) {
		case *rowKVEntry:
			dml, err := m.mountRowKVEntry(e)
			if err != nil {
				return model.Txn{}, errors.Trace(err)
			}
			if dml != nil {
				if dml.Tp == model.InsertDMLType {
					replaceDMLs = append(replaceDMLs, dml)
				} else {
					deleteDMLs = append(deleteDMLs, dml)
				}
			}
		case *indexKVEntry:
			dml, err := m.mountIndexKVEntry(e)
			if err != nil {
				return model.Txn{}, errors.Trace(err)
			}
			if dml != nil {
				deleteDMLs = append(deleteDMLs, dml)
			}
		case *ddlJobKVEntry:
			t.DDL, err = m.mountDDL(e)
			if err != nil {
				return model.Txn{}, errors.Trace(err)
			}
			return t, nil
		case *unknownKVEntry:
			log.Debug("Found unknown kv entry", zap.Binary("unknownKey", e.Key))
		}
	}
	t.DMLs = append(deleteDMLs, replaceDMLs...)
	return t, nil
}

func (m *Mounter) mountRowKVEntry(row *rowKVEntry) (*model.DML, error) {
	tableInfo, tableName, err := m.fetchTableInfo(row.TableID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = row.unflatten(tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if row.Delete {
		if tableInfo.PKIsHandle {
			pkColName, pkValue, err := fetchHandleValue(tableInfo, row)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return &model.DML{
				Database: tableName.Schema,
				Table:    tableName.Table,
				Tp:       model.DeleteDMLType,
				Values:   map[string]types.Datum{pkColName: *pkValue},
			}, nil
		}
		return nil, nil
	}

	values := make(map[string]types.Datum, len(row.Row)+1)
	for index, colValue := range row.Row {
		colInfo, exist := tableInfo.GetColumnInfo(index)
		if !exist {
			return nil, errors.NotFoundf("column info, colID: %d", index)
		}
		colName := colInfo.Name.O
		values[colName] = colValue
	}
	if tableInfo.PKIsHandle {
		pkColName, pkValue, err := fetchHandleValue(tableInfo, row)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values[pkColName] = *pkValue
	}

	for _, col := range tableInfo.Columns {
		_, ok := values[col.Name.O]
		if !ok {
			values[col.Name.O] = getDefaultOrZeroValue(col)
		}
	}

	return &model.DML{
		Database: tableName.Schema,
		Table:    tableName.Table,
		Tp:       model.InsertDMLType,
		Values:   values,
	}, nil
}

func (m *Mounter) mountIndexKVEntry(idx *indexKVEntry) (*model.DML, error) {
	// skip set index KV
	if !idx.Delete {
		return nil, nil
	}
	tableInfo, tableName, err := m.fetchTableInfo(idx.TableID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	indexInfo, exist := tableInfo.GetIndexInfo(idx.IndexID)
	if !exist {
		return nil, errors.NotFoundf("index info %d", idx.IndexID)
	}

	if !tableInfo.IsIndexUnique(indexInfo) {
		return nil, nil
	}

	err = idx.unflatten(tableInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}

	values := make(map[string]types.Datum, len(idx.IndexValue))
	for i, idxCol := range indexInfo.Columns {
		values[idxCol.Name.O] = idx.IndexValue[i]
	}
	return &model.DML{
		Database: tableName.Schema,
		Table:    tableName.Table,
		Tp:       model.DeleteDMLType,
		Values:   values,
	}, nil
}

func getDefaultOrZeroValue(col *timodel.ColumnInfo) types.Datum {
	// see https://github.com/pingcap/tidb/issues/9304
	// must use null if TiDB not write the column value when default value is null
	// and the value is null
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.NewDatum(nil)
	}

	if col.GetDefaultValue() != nil {
		return types.NewDatum(col.GetDefaultValue())
	}

	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		return types.NewDatum(col.FieldType.Elems[0])
	}

	return table.GetZeroValue(col)
}

func (m *Mounter) fetchTableInfo(tableID int64) (tableInfo *schema.TableInfo, tableName schema.TableName, err error) {
	tableInfo, exist := m.schemaStorage.TableByID(tableID)
	if !exist {
		err = errors.Errorf("can not find table, id: %d", tableID)
		return
	}

	tableName, exist = m.schemaStorage.GetTableNameByID(tableID)
	if !exist {
		err = errors.Errorf("can not find table, id: %d", tableID)
	}
	return
}

func fetchHandleValue(tableInfo *schema.TableInfo, row *rowKVEntry) (pkColName string, pkValue *types.Datum, err error) {
	handleColOffset := -1
	for i, col := range tableInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			handleColOffset = i
			break
		}
	}
	if handleColOffset == -1 {
		return "", nil, errors.New("can't find handle column, please check if the pk is handle")
	}
	handleCol := tableInfo.Columns[handleColOffset]
	pkColName = handleCol.Name.O
	pkValue = &types.Datum{}
	if mysql.HasUnsignedFlag(handleCol.Flag) {
		pkValue.SetUint64(uint64(row.RecordID))
	} else {
		pkValue.SetInt64(row.RecordID)
	}
	return
}

func (m *Mounter) mountDDL(jobEntry *ddlJobKVEntry) (*model.DDL, error) {
	databaseName := jobEntry.Job.SchemaName
	var tableName string
	table := jobEntry.Job.BinlogInfo.TableInfo
	if table == nil {
		tableName = ""
	} else {
		tableName = table.Name.O
	}
	return &model.DDL{
		Database: databaseName,
		Table:    tableName,
		Job:      jobEntry.Job,
	}, nil
}
