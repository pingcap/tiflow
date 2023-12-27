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
	"time"

	"github.com/linkedin/goavro/v2"
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
			// todo: add detail description about length,
			// for the text type, default length is 4294967295,
			// it's out of the range, convert it to int32, make it -1.
			"length":   int32(col.GetFlen()),
			"decimal":  col.GetDecimal(),
			"elements": col.GetElems(),
			"unsigned": mysql.HasUnsignedFlag(col.GetFlag()),
			"zerofill": mysql.HasZerofillFlag(col.GetFlag()),
		}
		column := map[string]interface{}{
			"name":     col.Name.O,
			"dataType": mysqlType,
			"nullable": !mysql.HasNotNullFlag(col.GetFlag()),
			"default":  entry.GetColumnDefaultValue(col),
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

	return goavro.Union("com.pingcap.simple.avro.TableSchema", result)
}

func newResolvedMessageMap(ts uint64) interface{} {
	return goavro.Union("com.pingcap.simple.avro.Message", map[string]interface{}{
		"version":  defaultVersion,
		"type":     string(WatermarkType),
		"commitTs": int64(ts),
		"buildTs":  time.Now().UnixMilli(),
	})
}

func newDDLMessageMap(ddl *model.DDLEvent) interface{} {
	eventType := DDLType
	if ddl.IsBootstrap {
		eventType = BootstrapType
	}
	result := map[string]interface{}{
		"version":  defaultVersion,
		"type":     string(eventType),
		"commitTs": int64(ddl.CommitTs),
		"buildTs":  time.Now().UnixMilli(),
	}

	if ddl.TableInfo != nil && ddl.TableInfo.TableInfo != nil {
		result["tableSchema"] = newTableSchemaMap(ddl.TableInfo)
	}
	if !ddl.IsBootstrap {
		if ddl.PreTableInfo != nil && ddl.PreTableInfo.TableInfo != nil {
			result["preTableSchema"] = newTableSchemaMap(ddl.PreTableInfo)
		}
	}

	if eventType == DDLType {
		result["sql"] = goavro.Union("string", ddl.Query)
	}

	return goavro.Union("com.pingcap.simple.avro.Message", result)
}

func newDMLMessageMap(event *model.RowChangedEvent, config *common.Config, onlyHandleKey bool) interface{} {
	m := map[string]interface{}{
		"version":       defaultVersion,
		"schema":        goavro.Union("string", event.Table.Schema),
		"table":         goavro.Union("string", event.Table.Table),
		"commitTs":      int64(event.CommitTs),
		"buildTs":       time.Now().UnixMilli(),
		"schemaVersion": goavro.Union("long", int64(event.TableInfo.UpdateTS)),
	}

	if onlyHandleKey {
		m["handleKeyOnly"] = goavro.Union("boolean", true)
	}

	var claimCheckLocation string
	if claimCheckLocation != "" {
		m["claimCheckLocation"] = goavro.Union("string", claimCheckLocation)
	}

	if config.EnableRowChecksum && event.Checksum != nil {
		cc := map[string]interface{}{
			"version":   event.Checksum.Version,
			"corrupted": event.Checksum.Corrupted,
			"current":   event.Checksum.Current,
			"previous":  event.Checksum.Previous,
		}
		m["checksum"] = goavro.Union("com.pingcap.simple.avro.Checksum", cc)
	}

	if event.IsInsert() {
		m["data"] = collectColumns(event.Columns, event.ColInfos, onlyHandleKey)
		m["type"] = string(InsertType)
	} else if event.IsDelete() {
		m["old"] = collectColumns(event.PreColumns, event.ColInfos, onlyHandleKey)
		m["type"] = string(DeleteType)
	} else if event.IsUpdate() {
		m["data"] = collectColumns(event.Columns, event.ColInfos, onlyHandleKey)
		m["old"] = collectColumns(event.PreColumns, event.ColInfos, onlyHandleKey)
		m["type"] = string(UpdateType)
	} else {
		log.Panic("invalid event type, this should not hit", zap.Any("event", event))
	}

	return goavro.Union("com.pingcap.simple.avro.Message", m)
}

func collectColumns(columns []*model.Column, columnInfos []rowcodec.ColInfo, onlyHandleKey bool) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))
	for _, col := range columns {
		if col == nil {
			continue
		}
		if onlyHandleKey && !col.Flag.IsHandleKey() {
			continue
		}
		// todo: is it necessary to encode values into string ?
		//value, err := encodeValue(col.Value, columnInfos[idx].Ft)
		//if err != nil {
		//	log.Panic("encode value failed", zap.Error(err))
		//}
		result[col.Name] = col.Value
	}
	return result
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
			Length:    int(rawDataType["length"].(int32)),
			Decimal:   int(rawDataType["decimal"].(int32)),
			Elements:  elements,
			Unsigned:  rawDataType["unsigned"].(bool),
			Zerofill:  rawDataType["zerofill"].(bool),
		}

		column := &columnSchema{
			Name:     raw["name"].(string),
			Nullable: raw["nullable"].(bool),
			Default:  raw["default"],
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

func newMessageFromAvroNative(native interface{}) (*message, error) {
	rawValues, ok := native.(map[string]interface{})
	if !ok {
		return nil, cerror.ErrDecodeFailed.GenWithStack("cannot convert the avro message to map")
	}
	rawValues = rawValues["com.pingcap.simple.avro.Message"].(map[string]interface{})
	m := new(message)
	switch EventType(rawValues["type"].(string)) {
	case WatermarkType:
		m.Type = WatermarkType
		m.Version = int(rawValues["version"].(int32))
		m.CommitTs = uint64(rawValues["commitTs"].(int64))
		m.BuildTs = rawValues["buildTs"].(int64)
	case DDLType:
		m.Type = DDLType
		m.Version = int(rawValues["version"].(int32))
		m.SQL = rawValues["sql"].(map[string]interface{})["string"].(string)
		m.CommitTs = uint64(rawValues["commitTs"].(int64))
		m.BuildTs = rawValues["buildTs"].(int64)
		if rawValues["tableSchema"] != nil {
			rawTableSchema := rawValues["tableSchema"].(map[string]interface{})
			rawTableSchema = rawTableSchema["com.pingcap.simple.avro.TableSchema"].(map[string]interface{})
			m.TableSchema = newTableSchemaFromAvroNative(rawTableSchema)
		}
		if rawValues["preTableSchema"] != nil {
			rawPreTableSchema := rawValues["preTableSchema"].(map[string]interface{})
			rawPreTableSchema = rawPreTableSchema["com.pingcap.simple.avro.TableSchema"].(map[string]interface{})
			m.PreTableSchema = newTableSchemaFromAvroNative(rawPreTableSchema)
		}
	case BootstrapType:
		m.Type = BootstrapType
	case InsertType:
		m.Type = InsertType
	case UpdateType:
		m.Type = UpdateType
	case DeleteType:
		m.Type = DeleteType
	}
	return m, nil
}
