// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"sort"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
)

func newTableSchemaMap(tableInfo *model.TableInfo) any {
	pkInIndexes := false
	indexesSchema := make([]any, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		index := map[string]any{
			"name":     idx.Name.O,
			"unique":   idx.Unique,
			"primary":  idx.Primary,
			"nullable": false,
		}
		columns := make([]string, 0, len(idx.Columns))
		for _, col := range idx.Columns {
			columns = append(columns, col.Name.O)
			colInfo := tableInfo.Columns[col.Offset]
			// An index is not null when all columns of are not null
			if !mysql.HasNotNullFlag(colInfo.GetFlag()) {
				index["nullable"] = true
			}
		}
		index["columns"] = columns
		if idx.Primary {
			pkInIndexes = true
		}
		indexesSchema = append(indexesSchema, index)
	}

	// sometimes the primary key is not in the index, we need to find it manually.
	if !pkInIndexes {
		pkColumns := tableInfo.GetPrimaryKeyColumnNames()
		if len(pkColumns) != 0 {
			index := map[string]any{
				"name":     "primary",
				"nullable": false,
				"primary":  true,
				"unique":   true,
				"columns":  pkColumns,
			}
			indexesSchema = append(indexesSchema, index)
		}
	}

	sort.SliceStable(tableInfo.Columns, func(i, j int) bool {
		return tableInfo.Columns[i].ID < tableInfo.Columns[j].ID
	})

	columnsSchema := make([]any, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		mysqlType := map[string]any{
			"mysqlType": types.TypeToStr(col.GetType(), col.GetCharset()),
			"charset":   col.GetCharset(),
			"collate":   col.GetCollate(),
			"length":    col.GetFlen(),
		}

		switch col.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeFloat, mysql.TypeDouble, mysql.TypeBit, mysql.TypeYear:
			mysqlType["unsigned"] = map[string]any{
				"boolean": mysql.HasUnsignedFlag(col.GetFlag()),
			}
			mysqlType["zerofill"] = map[string]any{
				"boolean": mysql.HasZerofillFlag(col.GetFlag()),
			}
		case mysql.TypeEnum, mysql.TypeSet:
			mysqlType["elements"] = map[string]any{
				"array": col.GetElems(),
			}
		case mysql.TypeNewDecimal:
			mysqlType["decimal"] = map[string]any{
				"int": col.GetDecimal(),
			}
			mysqlType["unsigned"] = map[string]any{
				"boolean": mysql.HasUnsignedFlag(col.GetFlag()),
			}
			mysqlType["zerofill"] = map[string]any{
				"boolean": mysql.HasZerofillFlag(col.GetFlag()),
			}
		default:
		}

		column := map[string]any{
			"name":     col.Name.O,
			"dataType": mysqlType,
			"nullable": !mysql.HasNotNullFlag(col.GetFlag()),
			"default":  nil,
		}
		defaultValue := col.GetDefaultValue()
		if defaultValue != nil {
			// according to TiDB source code, the default value is converted to string if not nil.
			column["default"] = map[string]any{
				"string": defaultValue,
			}
		}

		columnsSchema = append(columnsSchema, column)
	}

	result := map[string]any{
		"database": tableInfo.TableName.Schema,
		"table":    tableInfo.TableName.Table,
		"tableID":  tableInfo.ID,
		"version":  int64(tableInfo.UpdateTS),
		"columns":  columnsSchema,
		"indexes":  indexesSchema,
	}

	return result
}

func newResolvedMessageMap(ts uint64) map[string]any {
	watermark := map[string]any{
		"version":  defaultVersion,
		"type":     string(MessageTypeWatermark),
		"commitTs": int64(ts),
		"buildTs":  time.Now().UnixMilli(),
	}
	watermark = map[string]any{
		"com.pingcap.simple.avro.Watermark": watermark,
	}

	payload := map[string]any{
		"type":    string(MessageTypeWatermark),
		"payload": watermark,
	}

	return map[string]any{
		"com.pingcap.simple.avro.Message": payload,
	}
}

func newBootstrapMessageMap(tableInfo *model.TableInfo) map[string]any {
	m := map[string]any{
		"version":     defaultVersion,
		"type":        string(MessageTypeBootstrap),
		"tableSchema": newTableSchemaMap(tableInfo),
		"buildTs":     time.Now().UnixMilli(),
	}

	m = map[string]any{
		"com.pingcap.simple.avro.Bootstrap": m,
	}

	payload := map[string]any{
		"type":    string(MessageTypeBootstrap),
		"payload": m,
	}

	return map[string]any{
		"com.pingcap.simple.avro.Message": payload,
	}
}

func newDDLMessageMap(ddl *model.DDLEvent) map[string]any {
	result := map[string]any{
		"version":  defaultVersion,
		"type":     string(getDDLType(ddl.Type)),
		"sql":      ddl.Query,
		"commitTs": int64(ddl.CommitTs),
		"buildTs":  time.Now().UnixMilli(),
	}

	if ddl.TableInfo != nil && ddl.TableInfo.TableInfo != nil {
		tableSchema := newTableSchemaMap(ddl.TableInfo)
		result["tableSchema"] = map[string]any{
			"com.pingcap.simple.avro.TableSchema": tableSchema,
		}
	}
	if ddl.PreTableInfo != nil && ddl.PreTableInfo.TableInfo != nil {
		tableSchema := newTableSchemaMap(ddl.PreTableInfo)
		result["preTableSchema"] = map[string]any{
			"com.pingcap.simple.avro.TableSchema": tableSchema,
		}
	}

	result = map[string]any{
		"com.pingcap.simple.avro.DDL": result,
	}
	payload := map[string]any{
		"type":    string(MessageTypeDDL),
		"payload": result,
	}
	return map[string]any{
		"com.pingcap.simple.avro.Message": payload,
	}
}

var (
	// genericMapPool return holder for each column and checksum
	genericMapPool = sync.Pool{
		New: func() any {
			return make(map[string]any)
		},
	}
	// rowMapPool return map for each row
	rowMapPool = sync.Pool{
		New: func() any {
			return make(map[string]any)
		},
	}

	dmlMessagePayloadPool = sync.Pool{
		New: func() any {
			return make(map[string]any)
		},
	}

	// dmlMessagePool return a map for the dml message
	dmlMessagePool = sync.Pool{
		New: func() any {
			return make(map[string]any)
		},
	}

	messageHolderPool = sync.Pool{
		New: func() any {
			return make(map[string]any)
		},
	}
)

func (a *avroMarshaller) newDMLMessageMap(
	event *model.RowChangedEvent,
	onlyHandleKey bool,
	claimCheckFileName string,
) map[string]any {
	dmlMessagePayload := dmlMessagePayloadPool.Get().(map[string]any)
	dmlMessagePayload["version"] = defaultVersion
	dmlMessagePayload["database"] = event.TableInfo.GetSchemaName()
	dmlMessagePayload["table"] = event.TableInfo.GetTableName()
	dmlMessagePayload["tableID"] = event.GetTableID()
	dmlMessagePayload["commitTs"] = int64(event.CommitTs)
	dmlMessagePayload["buildTs"] = time.Now().UnixMilli()
	dmlMessagePayload["schemaVersion"] = int64(event.TableInfo.UpdateTS)

	if !a.config.LargeMessageHandle.Disabled() && onlyHandleKey {
		dmlMessagePayload["handleKeyOnly"] = map[string]any{
			"boolean": true,
		}
	}

	if a.config.LargeMessageHandle.EnableClaimCheck() && claimCheckFileName != "" {
		dmlMessagePayload["claimCheckLocation"] = map[string]any{
			"string": claimCheckFileName,
		}
	}

	if a.config.EnableRowChecksum && event.Checksum != nil {
		cc := map[string]any{
			"version":   event.Checksum.Version,
			"corrupted": event.Checksum.Corrupted,
			"current":   int64(event.Checksum.Current),
			"previous":  int64(event.Checksum.Previous),
		}

		holder := genericMapPool.Get().(map[string]any)
		holder["com.pingcap.simple.avro.Checksum"] = cc
		dmlMessagePayload["checksum"] = holder
	}

	if event.IsInsert() {
		data := a.collectColumns(event.Columns, event.TableInfo, onlyHandleKey)
		dmlMessagePayload["data"] = data
		dmlMessagePayload["type"] = string(DMLTypeInsert)
	} else if event.IsDelete() {
		old := a.collectColumns(event.PreColumns, event.TableInfo, onlyHandleKey)
		dmlMessagePayload["old"] = old
		dmlMessagePayload["type"] = string(DMLTypeDelete)
	} else if event.IsUpdate() {
		data := a.collectColumns(event.Columns, event.TableInfo, onlyHandleKey)
		dmlMessagePayload["data"] = data
		old := a.collectColumns(event.PreColumns, event.TableInfo, onlyHandleKey)
		dmlMessagePayload["old"] = old
		dmlMessagePayload["type"] = string(DMLTypeUpdate)
	}

	dmlMessagePayload = map[string]any{
		"com.pingcap.simple.avro.DML": dmlMessagePayload,
	}

	dmlMessage := dmlMessagePool.Get().(map[string]any)
	dmlMessage["type"] = string(MessageTypeDML)
	dmlMessage["payload"] = dmlMessagePayload

	messageHolder := messageHolderPool.Get().(map[string]any)
	messageHolder["com.pingcap.simple.avro.Message"] = dmlMessage

	return messageHolder
}

func recycleMap(m map[string]any) {
	dmlMessage := m["com.pingcap.simple.avro.Message"].(map[string]any)
	dml := dmlMessage["payload"].(map[string]any)["com.pingcap.simple.avro.DML"].(map[string]any)

	checksum := dml["checksum"]
	if checksum != nil {
		checksum := checksum.(map[string]any)
		clear(checksum)
		genericMapPool.Put(checksum)
	}

	dataMap := dml["data"]
	if dataMap != nil {
		dataMap := dataMap.(map[string]any)["map"].(map[string]any)
		for _, col := range dataMap {
			colMap := col.(map[string]any)
			clear(colMap)
			genericMapPool.Put(col)
		}
		clear(dataMap)
		rowMapPool.Put(dataMap)
	}

	oldDataMap := dml["old"]
	if oldDataMap != nil {
		oldDataMap := oldDataMap.(map[string]any)["map"].(map[string]any)
		for _, col := range oldDataMap {
			colMap := col.(map[string]any)
			clear(colMap)
			genericMapPool.Put(col)
		}
		clear(oldDataMap)
		rowMapPool.Put(oldDataMap)
	}

	clear(dml)
	dmlMessagePayloadPool.Put(dml)

	clear(dmlMessage)
	dmlMessagePool.Put(dmlMessage)

	clear(m)
	messageHolderPool.Put(m)
}

func (a *avroMarshaller) collectColumns(
	columns []*model.ColumnData, tableInfo *model.TableInfo, onlyHandleKey bool,
) map[string]any {
	result := rowMapPool.Get().(map[string]any)
	for _, col := range columns {
		if col != nil {
			colFlag := tableInfo.ForceGetColumnFlagType(col.ColumnID)
			if onlyHandleKey && !colFlag.IsHandleKey() {
				continue
			}
			colInfo := tableInfo.ForceGetColumnInfo(col.ColumnID)
			value, avroType := a.encodeValue4Avro(col.Value, &colInfo.FieldType)
			holder := genericMapPool.Get().(map[string]any)
			holder[avroType] = value
			result[colInfo.Name.O] = holder
		}
	}
	return map[string]any{
		"map": result,
	}
}

func newTableSchemaFromAvroNative(native map[string]any) *TableSchema {
	rawColumns := native["columns"].([]any)
	columns := make([]*columnSchema, 0, len(rawColumns))
	for _, raw := range rawColumns {
		raw := raw.(map[string]any)
		rawDataType := raw["dataType"].(map[string]any)

		var (
			decimal  int
			elements []string
			unsigned bool
			zerofill bool
		)

		if rawDataType["elements"] != nil {
			rawElements := rawDataType["elements"].(map[string]any)["array"].([]any)
			for _, rawElement := range rawElements {
				elements = append(elements, rawElement.(string))
			}
		}
		if rawDataType["decimal"] != nil {
			decimal = int(rawDataType["decimal"].(map[string]any)["int"].(int32))
		}
		if rawDataType["unsigned"] != nil {
			unsigned = rawDataType["unsigned"].(map[string]any)["boolean"].(bool)
		}
		if rawDataType["zerofill"] != nil {
			zerofill = rawDataType["zerofill"].(map[string]any)["boolean"].(bool)
		}

		dt := dataType{
			MySQLType: rawDataType["mysqlType"].(string),
			Charset:   rawDataType["charset"].(string),
			Collate:   rawDataType["collate"].(string),
			Length:    int(rawDataType["length"].(int64)),
			Decimal:   decimal,
			Elements:  elements,
			Unsigned:  unsigned,
			Zerofill:  zerofill,
		}

		var defaultValue any
		rawDefault := raw["default"]
		switch v := rawDefault.(type) {
		case nil:
		case map[string]any:
			defaultValue = v["string"].(string)
		}

		column := &columnSchema{
			Name:     raw["name"].(string),
			Nullable: raw["nullable"].(bool),
			Default:  defaultValue,
			DataType: dt,
		}
		columns = append(columns, column)
	}

	rawIndexes := native["indexes"].([]any)
	indexes := make([]*IndexSchema, 0, len(rawIndexes))
	for _, raw := range rawIndexes {
		raw := raw.(map[string]any)
		rawColumns := raw["columns"].([]any)
		keyColumns := make([]string, 0, len(rawColumns))
		for _, rawColumn := range rawColumns {
			keyColumns = append(keyColumns, rawColumn.(string))
		}
		index := &IndexSchema{
			Name:     raw["name"].(string),
			Unique:   raw["unique"].(bool),
			Primary:  raw["primary"].(bool),
			Nullable: raw["nullable"].(bool),
			Columns:  keyColumns,
		}
		indexes = append(indexes, index)
	}
	return &TableSchema{
		Schema:  native["database"].(string),
		Table:   native["table"].(string),
		TableID: native["tableID"].(int64),
		Version: uint64(native["version"].(int64)),
		Columns: columns,
		Indexes: indexes,
	}
}

func newMessageFromAvroNative(native any, m *message) {
	rawValues := native.(map[string]any)["com.pingcap.simple.avro.Message"].(map[string]any)
	rawPayload := rawValues["payload"].(map[string]any)

	rawMessage := rawPayload["com.pingcap.simple.avro.Watermark"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]any)
		m.Version = int(rawValues["version"].(int32))
		m.Type = MessageTypeWatermark
		m.CommitTs = uint64(rawValues["commitTs"].(int64))
		m.BuildTs = rawValues["buildTs"].(int64)
		return
	}

	rawMessage = rawPayload["com.pingcap.simple.avro.Bootstrap"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]any)
		m.Version = int(rawValues["version"].(int32))
		m.Type = MessageTypeBootstrap
		m.BuildTs = rawValues["buildTs"].(int64)
		m.TableSchema = newTableSchemaFromAvroNative(rawValues["tableSchema"].(map[string]any))
		return
	}

	rawMessage = rawPayload["com.pingcap.simple.avro.DDL"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]any)
		m.Version = int(rawValues["version"].(int32))
		m.Type = MessageType(rawValues["type"].(string))
		m.SQL = rawValues["sql"].(string)
		m.CommitTs = uint64(rawValues["commitTs"].(int64))
		m.BuildTs = rawValues["buildTs"].(int64)

		rawTableSchemaValues := rawValues["tableSchema"]
		if rawTableSchemaValues != nil {
			rawTableSchema := rawTableSchemaValues.(map[string]any)
			rawTableSchema = rawTableSchema["com.pingcap.simple.avro.TableSchema"].(map[string]any)
			m.TableSchema = newTableSchemaFromAvroNative(rawTableSchema)
		}

		rawPreTableSchemaValue := rawValues["preTableSchema"]
		if rawPreTableSchemaValue != nil {
			rawPreTableSchema := rawPreTableSchemaValue.(map[string]any)
			rawPreTableSchema = rawPreTableSchema["com.pingcap.simple.avro.TableSchema"].(map[string]any)
			m.PreTableSchema = newTableSchemaFromAvroNative(rawPreTableSchema)
		}
		return
	}

	rawValues = rawPayload["com.pingcap.simple.avro.DML"].(map[string]any)
	m.Type = MessageType(rawValues["type"].(string))
	m.Version = int(rawValues["version"].(int32))
	m.CommitTs = uint64(rawValues["commitTs"].(int64))
	m.BuildTs = rawValues["buildTs"].(int64)
	m.Schema = rawValues["database"].(string)
	m.Table = rawValues["table"].(string)
	m.TableID = rawValues["tableID"].(int64)
	m.SchemaVersion = uint64(rawValues["schemaVersion"].(int64))

	if rawValues["handleKeyOnly"] != nil {
		m.HandleKeyOnly = rawValues["handleKeyOnly"].(map[string]any)["boolean"].(bool)
	}
	if rawValues["claimCheckLocation"] != nil {
		m.ClaimCheckLocation = rawValues["claimCheckLocation"].(map[string]any)["string"].(string)
	}

	m.Checksum = newChecksum(rawValues)
	m.Data = newDataMap(rawValues["data"])
	m.Old = newDataMap(rawValues["old"])
}

func newChecksum(raw map[string]any) *checksum {
	rawValue := raw["checksum"]
	if rawValue == nil {
		return nil
	}
	rawChecksum := rawValue.(map[string]any)
	rawChecksum = rawChecksum["com.pingcap.simple.avro.Checksum"].(map[string]any)
	return &checksum{
		Version:   int(rawChecksum["version"].(int32)),
		Corrupted: rawChecksum["corrupted"].(bool),
		Current:   uint32(rawChecksum["current"].(int64)),
		Previous:  uint32(rawChecksum["previous"].(int64)),
	}
}

func newDataMap(rawValues any) map[string]any {
	if rawValues == nil {
		return nil
	}
	data := make(map[string]any)
	rawDataMap := rawValues.(map[string]any)["map"].(map[string]any)
	for key, value := range rawDataMap {
		if value == nil {
			data[key] = nil
			continue
		}
		valueMap := value.(map[string]any)
		for _, v := range valueMap {
			data[key] = v
		}
	}
	return data
}
