// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"encoding/base64"
	"strconv"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
)

type source struct {
	Version             string `json:"version,omitempty"`
	Connector           string `json:"connector,omitempty"`
	Name                string `json:"name,omitempty"`
	CommitTimeUnixMilli int64  `json:"ts_ms,omitempty"`
	Snapshot            string `json:"snapshot,omitempty"`
	DB                  string `json:"db,omitempty"`
	Table               string `json:"table,omitempty"`
	ServerID            string `json:"server_id,omitempty"`
	GtID                any    `json:"gtid,omitempty"`
	File                string `json:"file,omitempty"`
	Pos                 int    `json:"pos,omitempty"`
	Row                 int    `json:"row,omitempty"`
	Thread              int    `json:"thread,omitempty"`
	Query               any    `json:"query,omitempty"`
	CommitTs            uint64 `json:"commit_ts,omitempty"`
	ClusterID           string `json:"cluster_id,omitempty"`
	Type                any    `json:"type,omitempty"`
	Collate             string `json:"collate,omitempty"`
	Charset             string `json:"charset,omitempty"`
}

type column struct {
	Name            string `json:"name,omitempty"`
	JdbcType        int    `json:"jdbcType,omitempty"`
	NativeType      any    `json:"nativeType,omitempty"`
	TypeName        string `json:"typeName,omitempty"`
	TypeExpression  string `json:"typeExpression,omitempty"`
	CharsetName     string `json:"charsetName,omitempty"`
	Length          any    `json:"length,omitempty"`
	Scale           any    `json:"scale,omitempty"`
	Position        int    `json:"position,omitempty"`
	Optional        bool   `json:"optional,omitempty"`
	AutoIncremented bool   `json:"autoIncremented,omitempty"`
	Generated       bool   `json:"generated,omitempty"`
}
type table struct {
	DefaultCharsetName    string           `json:"defaultCharsetName,omitempty"`
	PrimaryKeyColumnNames []string         `json:"primaryKeyColumnNames,omitempty"`
	Columns               []column         `json:"columns,omitempty"`
	Attributes            []map[string]any `json:"attributes,omitempty"`
}
type tableChange struct {
	Type  string `json:"type,omitempty"`
	ID    string `json:"id,omitempty"`
	Table table  `json:"table,omitempty"`
}

type payload struct {
	Source      source         `json:"source,omitempty"`
	BuildTs     int64          `json:"ts_ms,omitempty"`
	Transaction string         `json:"transaction,omitempty"`
	Before      map[string]any `json:"before,omitempty"`
	After       map[string]any `json:"after,omitempty"`
	Op          string         `json:"op,omitempty"`

	DatabaseName string        `json:"databaseName,omitempty"`
	SchemaName   any           `json:"schemaName,omitempty"`
	DDL          string        `json:"ddl,omitempty"`
	TableChanges []tableChange `json:"tableChanges,omitempty"`
}

type field struct {
	Type     string  `json:"type,omitempty"`
	Optional bool    `json:"optional,omitempty"`
	Field    string  `json:"field,omitempty"`
	Fields   []field `json:"fields,omitempty"`
}

type schema struct {
	Type     string  `json:"type,omitempty"`
	Optional string  `json:"optional,omitempty"`
	Name     string  `json:"name,omitempty"`
	Version  string  `json:"version,omitempty"`
	Fields   []field `json:"fields,omitempty"`
}

type message struct {
	Payload payload `json:"payload,omitempty"`
	Schema  schema  `json:"schema,omitempty"`
}

func decodeColumn(value interface{}, id int64, fieldType *types.FieldType) *model.ColumnData {
	result := &model.ColumnData{
		ColumnID: id,
		Value:    value,
	}
	if value == nil {
		return result
	}

	var err error
	if mysql.HasBinaryFlag(fieldType.GetFlag()) {
		switch v := value.(type) {
		case string:
			value, err = base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil
			}
		default:
		}
		result.Value = value
		return result
	}

	switch fieldType.GetType() {
	case mysql.TypeBit, mysql.TypeSet:
		switch v := value.(type) {
		// avro encoding, set is encoded as `int64`, bit encoded as `string`
		// json encoding, set is encoded as `string`, bit encoded as `string`
		case string:
			value, err = strconv.ParseUint(v, 10, 64)
		case int64:
			value = uint64(v)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeYear:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseInt(v, 10, 64)
		default:
			value = v
		}
	case mysql.TypeLonglong:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				value, err = strconv.ParseUint(v, 10, 64)
			}
		case map[string]interface{}:
			value = uint64(v["value"].(int64))
		default:
			value = v
		}
	case mysql.TypeFloat:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseFloat(v, 32)
		default:
			value = v
		}
	case mysql.TypeDouble:
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseFloat(v, 64)
		default:
			value = v
		}
	case mysql.TypeEnum:
		// avro encoding, enum is encoded as `int64`, use it directly.
		// json encoding, enum is encoded as `string`
		switch v := value.(type) {
		case string:
			value, err = strconv.ParseUint(v, 10, 64)
		}
	default:
	}

	if err != nil {
		return nil
	}

	result.Value = value
	return result
}

func decodeColumns(rawData map[string]interface{}) []*model.Column {
	if rawData == nil {
		return nil
	}
	var result []*model.Column
	return result
}

// msgToRowChangeEvent converts from message to RowChangedEvent.
func msgToRowChangeEvent(msg *message) *model.RowChangedEvent {
	e := new(model.RowChangedEvent)
	e.CommitTs = msg.Payload.Source.CommitTs

	preCols := decodeColumns(msg.Payload.Before)
	cols := decodeColumns(msg.Payload.After)

	indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(cols)
	e.TableInfo = model.BuildTableInfo(msg.Payload.Source.DB, msg.Payload.Source.Table, cols, indexColumns)

	e.Columns = model.Columns2ColumnDatas(cols, e.TableInfo)
	e.PreColumns = model.Columns2ColumnDatas(preCols, e.TableInfo)

	return e
}

func msgToDDLEvent(msg *message) *model.DDLEvent {
	e := new(model.DDLEvent)
	e.TableInfo = new(model.TableInfo)
	e.TableInfo.TableName = model.TableName{
		Schema: msg.Payload.Source.DB,
		Table:  msg.Payload.Source.Table,
	}
	e.CommitTs = msg.Payload.Source.CommitTs
	e.Type = msg.Payload.Source.Type.(timodel.ActionType)
	e.Query = msg.Payload.DDL
	e.Collate = msg.Payload.Source.Collate
	e.Charset = msg.Payload.Source.Charset
	return e
}
