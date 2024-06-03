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
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
)

func newTableSchemaMap(tableInfo *model.TableInfo) interface{} {
	pkInIndexes := false
	indexesSchema := make([]interface{}, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.Indices {
		index := map[string]interface{}{
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
			index := map[string]interface{}{
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

	columnsSchema := make([]interface{}, 0, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		mysqlType := map[string]interface{}{
			"mysqlType": types.TypeToStr(col.GetType(), col.GetCharset()),
			"charset":   col.GetCharset(),
			"collate":   col.GetCollate(),
			"length":    col.GetFlen(),
		}

		switch col.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeFloat, mysql.TypeDouble, mysql.TypeBit, mysql.TypeYear:
			mysqlType["unsigned"] = map[string]interface{}{
				"boolean": mysql.HasUnsignedFlag(col.GetFlag()),
			}
			mysqlType["zerofill"] = map[string]interface{}{
				"boolean": mysql.HasZerofillFlag(col.GetFlag()),
			}
		case mysql.TypeEnum, mysql.TypeSet:
			mysqlType["elements"] = map[string]interface{}{
				"array": col.GetElems(),
			}
		case mysql.TypeNewDecimal:
			mysqlType["decimal"] = map[string]interface{}{
				"int": col.GetDecimal(),
			}
			mysqlType["unsigned"] = map[string]interface{}{
				"boolean": mysql.HasUnsignedFlag(col.GetFlag()),
			}
			mysqlType["zerofill"] = map[string]interface{}{
				"boolean": mysql.HasZerofillFlag(col.GetFlag()),
			}
		default:
		}

		column := map[string]interface{}{
			"name":     col.Name.O,
			"dataType": mysqlType,
			"nullable": !mysql.HasNotNullFlag(col.GetFlag()),
			"default":  nil,
		}
		defaultValue := model.GetColumnDefaultValue(col)
		if defaultValue != nil {
			// according to TiDB source code, the default value is converted to string if not nil.
			column["default"] = map[string]interface{}{
				"string": defaultValue,
			}
		}

		columnsSchema = append(columnsSchema, column)
	}

	result := map[string]interface{}{
		"database": tableInfo.TableName.Schema,
		"table":    tableInfo.TableName.Table,
		"tableID":  tableInfo.ID,
		"version":  int64(tableInfo.UpdateTS),
		"columns":  columnsSchema,
		"indexes":  indexesSchema,
	}

	return result
}

func newResolvedMessageMap(ts uint64) map[string]interface{} {
	watermark := map[string]interface{}{
		"version":  defaultVersion,
		"type":     string(MessageTypeWatermark),
		"commitTs": int64(ts),
		"buildTs":  time.Now().UnixMilli(),
	}
	watermark = map[string]interface{}{
		"com.pingcap.simple.avro.Watermark": watermark,
	}

	payload := map[string]interface{}{
		"type":    string(MessageTypeWatermark),
		"payload": watermark,
	}

	return map[string]interface{}{
		"com.pingcap.simple.avro.Message": payload,
	}
}

func newBootstrapMessageMap(tableInfo *model.TableInfo) map[string]interface{} {
	m := map[string]interface{}{
		"version":     defaultVersion,
		"type":        string(MessageTypeBootstrap),
		"tableSchema": newTableSchemaMap(tableInfo),
		"buildTs":     time.Now().UnixMilli(),
	}

	m = map[string]interface{}{
		"com.pingcap.simple.avro.Bootstrap": m,
	}

	payload := map[string]interface{}{
		"type":    string(MessageTypeBootstrap),
		"payload": m,
	}

	return map[string]interface{}{
		"com.pingcap.simple.avro.Message": payload,
	}
}

func newDDLMessageMap(ddl *model.DDLEvent) map[string]interface{} {
	result := map[string]interface{}{
		"version":  defaultVersion,
		"type":     string(getDDLType(ddl.Type)),
		"sql":      ddl.Query,
		"commitTs": int64(ddl.CommitTs),
		"buildTs":  time.Now().UnixMilli(),
	}

	if ddl.TableInfo != nil && ddl.TableInfo.TableInfo != nil {
		tableSchema := newTableSchemaMap(ddl.TableInfo)
		result["tableSchema"] = map[string]interface{}{
			"com.pingcap.simple.avro.TableSchema": tableSchema,
		}
	}
	if ddl.PreTableInfo != nil && ddl.PreTableInfo.TableInfo != nil {
		tableSchema := newTableSchemaMap(ddl.PreTableInfo)
		result["preTableSchema"] = map[string]interface{}{
			"com.pingcap.simple.avro.TableSchema": tableSchema,
		}
	}

	result = map[string]interface{}{
		"com.pingcap.simple.avro.DDL": result,
	}
	payload := map[string]interface{}{
		"type":    string(MessageTypeDDL),
		"payload": result,
	}
	return map[string]interface{}{
		"com.pingcap.simple.avro.Message": payload,
	}
}

var (
	// genericMapPool return holder for each column and checksum
	genericMapPool = sync.Pool{
		New: func() any {
			return make(map[string]interface{})
		},
	}

	// rowMapPool return map for each row
	rowMapPool = sync.Pool{
		New: func() any {
			return make(map[string]interface{})
		},
	}

	// dmlPayloadHolderPool return holder for the payload
	dmlPayloadHolderPool = sync.Pool{
		New: func() any {
			return make(map[string]interface{})
		},
	}

	messageHolderPool = sync.Pool{
		New: func() any {
			return make(map[string]interface{})
		},
	}
)

func (a *avroMarshaller) newDMLMessageMap(
	event *model.RowChangedEvent,
	onlyHandleKey bool,
	claimCheckFileName string,
) map[string]interface{} {
	m := map[string]interface{}{
		"version":       defaultVersion,
		"database":      event.TableInfo.GetSchemaName(),
		"table":         event.TableInfo.GetTableName(),
		"tableID":       event.TableInfo.ID,
		"commitTs":      int64(event.CommitTs),
		"buildTs":       time.Now().UnixMilli(),
		"schemaVersion": int64(event.TableInfo.UpdateTS),
	}

	if !a.config.LargeMessageHandle.Disabled() && onlyHandleKey {
		m["handleKeyOnly"] = map[string]interface{}{
			"boolean": true,
		}
	}

	if a.config.LargeMessageHandle.EnableClaimCheck() && claimCheckFileName != "" {
		m["claimCheckLocation"] = map[string]interface{}{
			"string": claimCheckFileName,
		}
	}

	if a.config.EnableRowChecksum && event.Checksum != nil {
		cc := map[string]interface{}{
			"version":   event.Checksum.Version,
			"corrupted": event.Checksum.Corrupted,
			"current":   int64(event.Checksum.Current),
			"previous":  int64(event.Checksum.Previous),
		}

		holder := genericMapPool.Get().(map[string]interface{})
		holder["com.pingcap.simple.avro.Checksum"] = cc
		m["checksum"] = holder
	}

	if event.IsInsert() {
		data := a.collectColumns(event.Columns, event.ColInfos, onlyHandleKey)
		m["data"] = data
		m["type"] = string(DMLTypeInsert)
	} else if event.IsDelete() {
		old := a.collectColumns(event.PreColumns, event.ColInfos, onlyHandleKey)
		m["old"] = old
		m["type"] = string(DMLTypeDelete)
	} else if event.IsUpdate() {
		data := a.collectColumns(event.Columns, event.ColInfos, onlyHandleKey)
		m["data"] = data
		old := a.collectColumns(event.PreColumns, event.ColInfos, onlyHandleKey)
		m["old"] = old
		m["type"] = string(DMLTypeUpdate)
	}

	m = map[string]interface{}{
		"com.pingcap.simple.avro.DML": m,
	}

	holder := dmlPayloadHolderPool.Get().(map[string]interface{})
	holder["type"] = string(MessageTypeDML)
	holder["payload"] = m

	messageHolder := messageHolderPool.Get().(map[string]interface{})
	messageHolder["com.pingcap.simple.avro.Message"] = holder

	return messageHolder
}

func recycleMap(m map[string]interface{}) {
	holder := m["com.pingcap.simple.avro.Message"].(map[string]interface{})
	payload := holder["payload"].(map[string]interface{})
	eventMap := payload["com.pingcap.simple.avro.DML"].(map[string]interface{})

	checksum := eventMap["checksum"]
	if checksum != nil {
		checksum := checksum.(map[string]interface{})
		clear(checksum)
		genericMapPool.Put(checksum)
	}

	dataMap := eventMap["data"]
	if dataMap != nil {
		dataMap := dataMap.(map[string]interface{})["map"].(map[string]interface{})
		for _, col := range dataMap {
			colMap := col.(map[string]interface{})
			clear(colMap)
			genericMapPool.Put(col)
		}
		clear(dataMap)
		rowMapPool.Put(dataMap)
	}

	oldDataMap := eventMap["old"]
	if oldDataMap != nil {
		oldDataMap := oldDataMap.(map[string]interface{})["map"].(map[string]interface{})
		for _, col := range oldDataMap {
			colMap := col.(map[string]interface{})
			clear(colMap)
			genericMapPool.Put(col)
		}
		clear(oldDataMap)
		rowMapPool.Put(oldDataMap)
	}
	holder["payload"] = nil
	dmlPayloadHolderPool.Put(holder)
	m["com.pingcap.simple.avro.Message"] = nil
	messageHolderPool.Put(m)
}

func (a *avroMarshaller) collectColumns(
	columns []*model.Column, columnInfos []rowcodec.ColInfo, onlyHandleKey bool,
) map[string]interface{} {
	result := rowMapPool.Get().(map[string]interface{})
	for idx, col := range columns {
		if col != nil {
			if onlyHandleKey && !col.Flag.IsHandleKey() {
				continue
			}
			value, avroType := a.encodeValue4Avro(col.Value, columnInfos[idx].Ft)
			holder := genericMapPool.Get().(map[string]interface{})
			holder[avroType] = value
			result[col.Name] = holder
		}
	}

	return map[string]interface{}{
		"map": result,
	}
}

func newTableSchemaFromAvroNative(native map[string]interface{}) *TableSchema {
	rawColumns := native["columns"].([]interface{})
	columns := make([]*columnSchema, 0, len(rawColumns))
	for _, raw := range rawColumns {
		raw := raw.(map[string]interface{})
		rawDataType := raw["dataType"].(map[string]interface{})

		var (
			decimal  int
			elements []string
			unsigned bool
			zerofill bool
		)

		if rawDataType["elements"] != nil {
			rawElements := rawDataType["elements"].(map[string]interface{})["array"].([]interface{})
			for _, rawElement := range rawElements {
				elements = append(elements, rawElement.(string))
			}
		}
		if rawDataType["decimal"] != nil {
			decimal = int(rawDataType["decimal"].(map[string]interface{})["int"].(int32))
		}
		if rawDataType["unsigned"] != nil {
			unsigned = rawDataType["unsigned"].(map[string]interface{})["boolean"].(bool)
		}
		if rawDataType["zerofill"] != nil {
			zerofill = rawDataType["zerofill"].(map[string]interface{})["boolean"].(bool)
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

		var defaultValue interface{}
		rawDefault := raw["default"]
		switch v := rawDefault.(type) {
		case nil:
		case map[string]interface{}:
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

	rawIndexes := native["indexes"].([]interface{})
	indexes := make([]*IndexSchema, 0, len(rawIndexes))
	for _, raw := range rawIndexes {
		raw := raw.(map[string]interface{})
		rawColumns := raw["columns"].([]interface{})
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

func newMessageFromAvroNative(native interface{}, m *message) {
	rawValues := native.(map[string]interface{})["com.pingcap.simple.avro.Message"].(map[string]interface{})
	rawPayload := rawValues["payload"].(map[string]interface{})

	rawMessage := rawPayload["com.pingcap.simple.avro.Watermark"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]interface{})
		m.Version = int(rawValues["version"].(int32))
		m.Type = MessageTypeWatermark
		m.CommitTs = uint64(rawValues["commitTs"].(int64))
		m.BuildTs = rawValues["buildTs"].(int64)
		return
	}

	rawMessage = rawPayload["com.pingcap.simple.avro.Bootstrap"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]interface{})
		m.Version = int(rawValues["version"].(int32))
		m.Type = MessageTypeBootstrap
		m.BuildTs = rawValues["buildTs"].(int64)
		m.TableSchema = newTableSchemaFromAvroNative(rawValues["tableSchema"].(map[string]interface{}))
		return
	}

	rawMessage = rawPayload["com.pingcap.simple.avro.DDL"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]interface{})
		m.Version = int(rawValues["version"].(int32))
		m.Type = MessageType(rawValues["type"].(string))
		m.SQL = rawValues["sql"].(string)
		m.CommitTs = uint64(rawValues["commitTs"].(int64))
		m.BuildTs = rawValues["buildTs"].(int64)

		rawTableSchemaValues := rawValues["tableSchema"]
		if rawTableSchemaValues != nil {
			rawTableSchema := rawTableSchemaValues.(map[string]interface{})
			rawTableSchema = rawTableSchema["com.pingcap.simple.avro.TableSchema"].(map[string]interface{})
			m.TableSchema = newTableSchemaFromAvroNative(rawTableSchema)
		}

		rawPreTableSchemaValue := rawValues["preTableSchema"]
		if rawPreTableSchemaValue != nil {
			rawPreTableSchema := rawPreTableSchemaValue.(map[string]interface{})
			rawPreTableSchema = rawPreTableSchema["com.pingcap.simple.avro.TableSchema"].(map[string]interface{})
			m.PreTableSchema = newTableSchemaFromAvroNative(rawPreTableSchema)
		}
		return
	}

	rawValues = rawPayload["com.pingcap.simple.avro.DML"].(map[string]interface{})
	m.Type = MessageType(rawValues["type"].(string))
	m.Version = int(rawValues["version"].(int32))
	m.CommitTs = uint64(rawValues["commitTs"].(int64))
	m.BuildTs = rawValues["buildTs"].(int64)
	m.Schema = rawValues["database"].(string)
	m.Table = rawValues["table"].(string)
	m.TableID = rawValues["tableID"].(int64)
	m.SchemaVersion = uint64(rawValues["schemaVersion"].(int64))

	if rawValues["handleKeyOnly"] != nil {
		m.HandleKeyOnly = rawValues["handleKeyOnly"].(map[string]interface{})["boolean"].(bool)
	}
	if rawValues["claimCheckLocation"] != nil {
		m.ClaimCheckLocation = rawValues["claimCheckLocation"].(map[string]interface{})["string"].(string)
	}

	m.Checksum = newChecksum(rawValues)
	m.Data = newDataMap(rawValues["data"])
	m.Old = newDataMap(rawValues["old"])
}

func newChecksum(raw map[string]interface{}) *checksum {
	rawValue := raw["checksum"]
	if rawValue == nil {
		return nil
	}
	rawChecksum := rawValue.(map[string]interface{})
	rawChecksum = rawChecksum["com.pingcap.simple.avro.Checksum"].(map[string]interface{})
	return &checksum{
		Version:   int(rawChecksum["version"].(int32)),
		Corrupted: rawChecksum["corrupted"].(bool),
		Current:   uint32(rawChecksum["current"].(int64)),
		Previous:  uint32(rawChecksum["previous"].(int64)),
	}
}

func newDataMap(rawValues interface{}) map[string]interface{} {
	if rawValues == nil {
		return nil
	}
	data := make(map[string]interface{})
	rawDataMap := rawValues.(map[string]interface{})["map"].(map[string]interface{})
	for key, value := range rawDataMap {
		if value == nil {
			data[key] = nil
			continue
		}
		valueMap := value.(map[string]interface{})
		for _, v := range valueMap {
			data[key] = v
		}
	}
	return data
}
