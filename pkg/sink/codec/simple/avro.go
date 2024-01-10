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

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

func newTableSchemaMap(tableInfo *model.TableInfo) interface{} {
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
			"decimal":   col.GetDecimal(),
			"elements":  col.GetElems(),
			"unsigned":  mysql.HasUnsignedFlag(col.GetFlag()),
			"zerofill":  mysql.HasZerofillFlag(col.GetFlag()),
		}
		column := map[string]interface{}{
			"name":     col.Name.O,
			"dataType": mysqlType,
			"nullable": !mysql.HasNotNullFlag(col.GetFlag()),
			"default":  nil,
		}
		defaultValue := entry.GetColumnDefaultValue(col)
		if defaultValue != nil {
			// according to TiDB source code, the default value is converted to string if not nil.
			column["default"] = map[string]interface{}{
				"string": defaultValue,
			}
		}

		columnsSchema = append(columnsSchema, column)
	}

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
			// An index is not null when all columns of aer not null
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

	result := map[string]interface{}{
		"schema":  tableInfo.TableName.Schema,
		"table":   tableInfo.TableName.Table,
		"version": int64(tableInfo.UpdateTS),
		"columns": columnsSchema,
		"indexes": indexesSchema,
	}

	return result
}

func newResolvedMessageMap(ts uint64) map[string]interface{} {
	return map[string]interface{}{
		"com.pingcap.simple.avro.Watermark": map[string]interface{}{
			"version":  defaultVersion,
			"type":     string(WatermarkType),
			"commitTs": int64(ts),
			"buildTs":  time.Now().UnixMilli(),
		},
	}
}

func newBootstrapMessageMap(tableInfo *model.TableInfo) map[string]interface{} {
	result := map[string]interface{}{
		"version":     defaultVersion,
		"type":        string(BootstrapType),
		"tableSchema": newTableSchemaMap(tableInfo),
		"buildTs":     time.Now().UnixMilli(),
	}
	return map[string]interface{}{
		"com.pingcap.simple.avro.Bootstrap": result,
	}
}

func newDDLMessageMap(ddl *model.DDLEvent) interface{} {
	result := map[string]interface{}{
		"version":  defaultVersion,
		"type":     string(DDLType),
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
	return map[string]interface{}{
		"com.pingcap.simple.avro.DDL": result,
	}
}

var genericMapPool = sync.Pool{
	New: func() any {
		return make(map[string]interface{})
	},
}

func newDMLMessageMap(
	event *model.RowChangedEvent, config *common.Config,
	onlyHandleKey bool,
	claimCheckFileName string,
) (map[string]interface{}, error) {
	m := map[string]interface{}{
		"version":            defaultVersion,
		"schema":             event.Table.Schema,
		"table":              event.Table.Table,
		"commitTs":           int64(event.CommitTs),
		"buildTs":            time.Now().UnixMilli(),
		"schemaVersion":      int64(event.TableInfo.UpdateTS),
		"handleKeyOnly":      onlyHandleKey,
		"claimCheckLocation": claimCheckFileName,
	}

	if config.EnableRowChecksum && event.Checksum != nil {
		cc := map[string]interface{}{
			"version":   event.Checksum.Version,
			"corrupted": event.Checksum.Corrupted,
			"current":   int64(event.Checksum.Current),
			"previous":  int64(event.Checksum.Previous),
		}

		genericMap, ok := genericMapPool.Get().(map[string]interface{})
		if !ok {
			genericMap = make(map[string]interface{})
		}
		genericMap["com.pingcap.simple.avro.Checksum"] = cc
		m["checksum"] = genericMap
	}

	if event.IsInsert() {
		data, err := collectColumns(event.Columns, event.ColInfos, onlyHandleKey)
		if err != nil {
			return nil, err
		}
		m["data"] = data
		m["type"] = string(InsertType)
	} else if event.IsDelete() {
		old, err := collectColumns(event.PreColumns, event.ColInfos, onlyHandleKey)
		if err != nil {
			return nil, err
		}
		m["old"] = old
		m["type"] = string(DeleteType)
	} else if event.IsUpdate() {
		data, err := collectColumns(event.Columns, event.ColInfos, onlyHandleKey)
		if err != nil {
			return nil, err
		}
		m["data"] = data
		old, err := collectColumns(event.PreColumns, event.ColInfos, onlyHandleKey)
		if err != nil {
			return nil, err
		}
		m["old"] = old
		m["type"] = string(UpdateType)
	} else {
		log.Panic("invalid event type, this should not hit", zap.Any("event", event))
	}

	return map[string]interface{}{
		"com.pingcap.simple.avro.DML": m,
	}, nil
}

func recycleMap(m map[string]interface{}) {
	eventMap := m["com.pingcap.simple.avro.DML"].(map[string]interface{})

	checksumMap := eventMap["com.pingcap.simple.avro.Checksum"]
	if checksumMap != nil {
		checksumMap := checksumMap.(map[string]interface{})
		clear(checksumMap)
		genericMapPool.Put(checksumMap)
	}

	dataMap := eventMap["data"]
	if dataMap != nil {
		dataMap := dataMap.(map[string]interface{})["map"].(map[string]interface{})
		for _, col := range dataMap {
			colMap := col.(map[string]interface{})
			clear(colMap)
			genericMapPool.Put(col)
		}
	}

	oldDataMap := eventMap["old"]
	if oldDataMap != nil {
		oldDataMap := oldDataMap.(map[string]interface{})["map"].(map[string]interface{})
		for _, col := range oldDataMap {
			colMap := col.(map[string]interface{})
			clear(colMap)
			genericMapPool.Put(col)
		}
	}
}

func collectColumns(
	columns []*model.Column, columnInfos []rowcodec.ColInfo, onlyHandleKey bool,
) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(columns))
	for idx, col := range columns {
		if col == nil {
			continue
		}
		if onlyHandleKey && !col.Flag.IsHandleKey() {
			continue
		}
		value, avroType, err := encodeValue4Avro(col.Value, columnInfos[idx].Ft)
		if err != nil {
			return nil, err
		}

		genericMap, ok := genericMapPool.Get().(map[string]interface{})
		if !ok {
			genericMap = make(map[string]interface{})
		}
		genericMap[avroType] = value
		result[col.Name] = genericMap
	}

	return map[string]interface{}{
		"map": result,
	}, nil
}

func newTableSchemaFromAvroNative(native map[string]interface{}) *TableSchema {
	rawColumns := native["columns"].([]interface{})
	columns := make([]*columnSchema, 0, len(rawColumns))
	for _, raw := range rawColumns {
		raw := raw.(map[string]interface{})
		rawDataType := raw["dataType"].(map[string]interface{})
		rawElements := rawDataType["elements"].([]interface{})
		elements := make([]string, 0, len(rawElements))
		for _, rawElement := range rawElements {
			elements = append(elements, rawElement.(string))
		}
		dt := dataType{
			MySQLType: rawDataType["mysqlType"].(string),
			Charset:   rawDataType["charset"].(string),
			Collate:   rawDataType["collate"].(string),
			Length:    int(rawDataType["length"].(int64)),
			Decimal:   int(rawDataType["decimal"].(int32)),
			Elements:  elements,
			Unsigned:  rawDataType["unsigned"].(bool),
			Zerofill:  rawDataType["zerofill"].(bool),
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
		Schema:  native["schema"].(string),
		Table:   native["table"].(string),
		Version: uint64(native["version"].(int64)),
		Columns: columns,
		Indexes: indexes,
	}
}

func newMessageFromAvroNative(native interface{}, m *message) error {
	rawValues, ok := native.(map[string]interface{})
	if !ok {
		return cerror.ErrDecodeFailed.GenWithStack("cannot convert the avro message to map")
	}

	rawMessage := rawValues["com.pingcap.simple.avro.Watermark"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]interface{})
		m.Version = int(rawValues["version"].(int32))
		m.Type = WatermarkType
		m.CommitTs = uint64(rawValues["commitTs"].(int64))
		m.BuildTs = rawValues["buildTs"].(int64)
		return nil
	}

	rawMessage = rawValues["com.pingcap.simple.avro.Bootstrap"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]interface{})
		m.Version = int(rawValues["version"].(int32))
		m.Type = BootstrapType
		m.BuildTs = rawValues["buildTs"].(int64)
		m.TableSchema = newTableSchemaFromAvroNative(rawValues["tableSchema"].(map[string]interface{}))
		return nil
	}

	rawMessage = rawValues["com.pingcap.simple.avro.DDL"]
	if rawMessage != nil {
		rawValues = rawMessage.(map[string]interface{})
		m.Version = int(rawValues["version"].(int32))
		m.Type = DDLType
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
		return nil
	}

	rawValues = rawValues["com.pingcap.simple.avro.DML"].(map[string]interface{})
	m.Type = EventType(rawValues["type"].(string))
	m.Version = int(rawValues["version"].(int32))
	m.CommitTs = uint64(rawValues["commitTs"].(int64))
	m.BuildTs = rawValues["buildTs"].(int64)
	m.Schema = rawValues["schema"].(string)
	m.Table = rawValues["table"].(string)
	m.SchemaVersion = uint64(rawValues["schemaVersion"].(int64))
	m.HandleKeyOnly = rawValues["handleKeyOnly"].(bool)
	m.ClaimCheckLocation = rawValues["claimCheckLocation"].(string)
	m.Checksum = newChecksum(rawValues)
	m.Data = newDataMap(rawValues["data"])
	m.Old = newDataMap(rawValues["old"])
	return nil
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
