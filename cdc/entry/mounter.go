package entry

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

type Mounter struct {
	schemaStorage *schema.Storage
	loc           *time.Location
}

func NewTxnMounter(schema *schema.Storage, loc *time.Location) *Mounter {
	return &Mounter{schemaStorage: schema, loc: loc}
}

func (m *Mounter) Mount(rawTxn model.RawTxn) (*model.Txn, error) {
	t := &model.Txn{
		Ts: rawTxn.Ts,
	}
	var replaceDMLs, deleteDMLs []*model.DML
	for _, raw := range rawTxn.Entries {
		kvEntry, err := Unmarshal(raw)
		if err != nil {
			return nil, errors.Trace(err)
		}

		switch e := kvEntry.(type) {
		case *RowKVEntry:
			dml, err := m.mountRowKVEntry(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if dml != nil {
				if dml.Tp == model.InsertDMLType {
					replaceDMLs = append(replaceDMLs, dml)
				} else {
					deleteDMLs = append(deleteDMLs, dml)
				}
			}
		case *IndexKVEntry:
			dml, err := m.mountIndexKVEntry(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if dml != nil {
				deleteDMLs = append(deleteDMLs, dml)
			}
		case *DDLJobKVEntry:
			t.DDL, err = m.mountDDL(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return t, nil
		case *UnknownKVEntry:
			log.Warn("Found unknown kv entry", zap.Reflect("UnknownKVEntry", e))
		}
	}
	t.DMLs = append(deleteDMLs, replaceDMLs...)
	return t, nil
}

func (m *Mounter) mountRowKVEntry(row *RowKVEntry) (*model.DML, error) {
	tableInfo, tableName, handleColName, err := m.fetchTableInfo(row.TableID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = row.Unflatten(tableInfo, m.loc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if row.Delete {
		if tableInfo.PKIsHandle {
			values := map[string]types.Datum{handleColName: types.NewIntDatum(row.RecordID)}
			return &model.DML{
				Database: tableName.Schema,
				Table:    tableName.Table,
				Tp:       model.DeleteDMLType,
				Values:   values,
			}, nil
		}
		return nil, nil
	}

	values := make(map[string]types.Datum, len(row.Row)+1)
	for index, colValue := range row.Row {
		colName := tableInfo.Columns[index-1].Name.O
		values[colName] = colValue
	}
	if tableInfo.PKIsHandle {
		values[handleColName] = types.NewIntDatum(row.RecordID)
	}
	return &model.DML{
		Database: tableName.Schema,
		Table:    tableName.Table,
		Tp:       model.InsertDMLType,
		Values:   values,
	}, nil
}

func (m *Mounter) mountIndexKVEntry(idx *IndexKVEntry) (*model.DML, error) {
	tableInfo, tableName, _, err := m.fetchTableInfo(idx.TableID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = idx.Unflatten(tableInfo, m.loc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	indexInfo := tableInfo.Indices[idx.IndexID-1]
	if !indexInfo.Primary && !indexInfo.Unique {
		return nil, nil
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

func (m *Mounter) fetchTableInfo(tableID int64) (tableInfo *timodel.TableInfo, tableName *schema.TableName, handleColName string, err error) {
	tableInfo, exist := m.schemaStorage.TableByID(tableID)
	if !exist {
		return nil, nil, "", errors.Errorf("can not find table, id: %d", tableID)
	}

	database, table, exist := m.schemaStorage.SchemaAndTableName(tableID)
	if !exist {
		return nil, nil, "", errors.Errorf("can not find table, id: %d", tableID)
	}
	tableName = &schema.TableName{Schema: database, Table: table}

	pkColOffset := -1
	for i, col := range tableInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			pkColOffset = i
			handleColName = tableInfo.Columns[i].Name.O
			break
		}
	}
	if tableInfo.PKIsHandle && pkColOffset == -1 {
		return nil, nil, "", errors.Errorf("this table (%d) is handled by pk, but pk column not found", tableID)
	}

	return
}

func (m *Mounter) mountDDL(jobEntry *DDLJobKVEntry) (*model.DDL, error) {
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
