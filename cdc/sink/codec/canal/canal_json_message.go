// Copyright 2022 PingCAP, Inc.
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

package canal

import (
	"sort"
	"strings"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	canal "github.com/pingcap/tiflow/proto/canal"
)

const tidbWaterMarkType = "TIDB_WATERMARK"

// The TiCDC Canal-JSON implementation extend the official format with a TiDB extension field.
// canalJSONMessageInterface is used to support this without affect the original format.
type canalJSONMessageInterface interface {
	getSchema() *string
	getTable() *string
	getCommitTs() uint64
	getQuery() string
	getOld() map[string]interface{}
	getData() map[string]interface{}
	getMySQLType() map[string]string
	getJavaSQLType() map[string]int32
	messageType() model.MessageType
	eventType() canal.EventType
	pkNameSet() map[string]struct{}
}

// JSONMessage adapted from https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/protocol/src/main/java/com/alibaba/otter/canal/protocol/FlatMessage.java#L1
type JSONMessage struct {
	// ignored by consumers
	ID        int64    `json:"id"`
	Schema    string   `json:"database"`
	Table     string   `json:"table"`
	PKNames   []string `json:"pkNames"`
	IsDDL     bool     `json:"isDdl"`
	EventType string   `json:"type"`
	// officially the timestamp of the event-time of the message, in milliseconds since Epoch.
	ExecutionTime int64 `json:"es"`
	// officially the timestamp of building the message, in milliseconds since Epoch.
	BuildTime int64 `json:"ts"`
	// SQL that generated the change event, DDL or Query
	Query string `json:"sql"`
	// only works for INSERT / UPDATE / DELETE events, records each column's java representation type.
	SQLType map[string]int32 `json:"sqlType"`
	// only works for INSERT / UPDATE / DELETE events, records each column's mysql representation type.
	MySQLType map[string]string `json:"mysqlType"`
	// A Datum should be a string or nil
	Data []map[string]interface{} `json:"data"`
	Old  []map[string]interface{} `json:"old"`
}

func (c *JSONMessage) getSchema() *string {
	return &c.Schema
}

func (c *JSONMessage) getTable() *string {
	return &c.Table
}

// for JSONMessage, we lost the commitTs.
func (c *JSONMessage) getCommitTs() uint64 {
	return 0
}

func (c *JSONMessage) getQuery() string {
	return c.Query
}

func (c *JSONMessage) getOld() map[string]interface{} {
	if c.Old == nil {
		return nil
	}
	return c.Old[0]
}

func (c *JSONMessage) getData() map[string]interface{} {
	if c.Data == nil {
		return nil
	}
	return c.Data[0]
}

func (c *JSONMessage) getMySQLType() map[string]string {
	return c.MySQLType
}

func (c *JSONMessage) getJavaSQLType() map[string]int32 {
	return c.SQLType
}

func (c *JSONMessage) messageType() model.MessageType {
	if c.IsDDL {
		return model.MessageTypeDDL
	}

	if c.EventType == tidbWaterMarkType {
		return model.MessageTypeResolved
	}

	return model.MessageTypeRow
}

func (c *JSONMessage) eventType() canal.EventType {
	return canal.EventType(canal.EventType_value[c.EventType])
}

func (c *JSONMessage) pkNameSet() map[string]struct{} {
	result := make(map[string]struct{}, len(c.PKNames))
	for _, item := range c.PKNames {
		result[item] = struct{}{}
	}
	return result
}

type tidbExtension struct {
	CommitTs    uint64 `json:"commitTs,omitempty"`
	WatermarkTs uint64 `json:"watermarkTs,omitempty"`
}

type canalJSONMessageWithTiDBExtension struct {
	*JSONMessage
	// Extensions is a TiCDC custom field that different from official Canal-JSON format.
	// It would be useful to store something for special usage.
	// At the moment, only store the `tso` of each event,
	// which is useful if the message consumer needs to restore the original transactions.
	Extensions *tidbExtension `json:"_tidb"`
}

func (c *canalJSONMessageWithTiDBExtension) getCommitTs() uint64 {
	return c.Extensions.CommitTs
}

func canalJSONMessage2RowChange(msg canalJSONMessageInterface) (*model.RowChangedEvent, error) {
	result := new(model.RowChangedEvent)
	result.CommitTs = msg.getCommitTs()
	result.Table = &model.TableName{
		Schema: *msg.getSchema(),
		Table:  *msg.getTable(),
	}

	mysqlType := msg.getMySQLType()
	javaSQLType := msg.getJavaSQLType()

	var err error
	if msg.eventType() == canal.EventType_DELETE {
		// for `DELETE` event, `data` contain the old data, set it as the `PreColumns`
		result.PreColumns, err = canalJSONColumnMap2RowChangeColumns(
			msg.getData(), mysqlType, javaSQLType)
		// canal-json encoder does not encode `Flag` information into the result,
		// we have to set the `Flag` to make it can be handled by MySQL Sink.
		// see https://github.com/pingcap/tiflow/blob/7bfce98/cdc/sink/mysql.go#L869-L888
		result.WithHandlePrimaryFlag(msg.pkNameSet())
		return result, err
	}

	// for `INSERT` and `UPDATE`, `data` contain fresh data, set it as the `Columns`
	result.Columns, err = canalJSONColumnMap2RowChangeColumns(msg.getData(),
		mysqlType, javaSQLType)
	if err != nil {
		return nil, err
	}

	// for `UPDATE`, `old` contain old data, set it as the `PreColumns`
	if msg.eventType() == canal.EventType_UPDATE {
		result.PreColumns, err = canalJSONColumnMap2RowChangeColumns(msg.getOld(),
			mysqlType, javaSQLType)
		if err != nil {
			return nil, err
		}
	}
	result.WithHandlePrimaryFlag(msg.pkNameSet())

	return result, nil
}

func canalJSONColumnMap2RowChangeColumns(cols map[string]interface{}, mysqlType map[string]string, javaSQLType map[string]int32) ([]*model.Column, error) {
	result := make([]*model.Column, 0, len(cols))
	for name, value := range cols {
		javaType, ok := javaSQLType[name]
		if !ok {
			// this should not happen, else we have to check encoding for javaSQLType.
			return nil, cerrors.ErrCanalDecodeFailed.GenWithStack(
				"java sql type does not found, column: %+v, mysqlType: %+v", name, javaSQLType)
		}
		mysqlTypeStr, ok := mysqlType[name]
		if !ok {
			// this should not happen, else we have to check encoding for mysqlType.
			return nil, cerrors.ErrCanalDecodeFailed.GenWithStack(
				"mysql type does not found, column: %+v, mysqlType: %+v", name, mysqlType)
		}
		mysqlTypeStr = trimUnsignedFromMySQLType(mysqlTypeStr)
		mysqlType := types.StrToType(mysqlTypeStr)
		col := internal.NewColumn(value, mysqlType).ToCanalJSONFormatColumn(name, internal.JavaSQLType(javaType))
		result = append(result, col)
	}
	if len(result) == 0 {
		return nil, nil
	}
	sort.Slice(result, func(i, j int) bool {
		return strings.Compare(result[i].Name, result[j].Name) > 0
	})
	return result, nil
}

func canalJSONMessage2DDLEvent(msg canalJSONMessageInterface) *model.DDLEvent {
	result := new(model.DDLEvent)
	// we lost the startTs from kafka message
	result.CommitTs = msg.getCommitTs()

	result.TableInfo = new(model.TableInfo)
	result.TableInfo.TableName = model.TableName{
		Schema: *msg.getSchema(),
		Table:  *msg.getTable(),
	}

	// we lost DDL type from canal json format, only got the DDL SQL.
	result.Query = msg.getQuery()

	// hack the DDL Type to be compatible with MySQL sink's logic
	// see https://github.com/pingcap/tiflow/blob/0578db337d/cdc/sink/mysql.go#L362-L370
	result.Type = getDDLActionType(result.Query)
	return result
}

// return DDL ActionType by the prefix
// see https://github.com/pingcap/tidb/blob/6dbf2de2f/parser/model/ddl.go#L101-L102
func getDDLActionType(query string) timodel.ActionType {
	query = strings.ToLower(query)
	if strings.HasPrefix(query, "create schema") || strings.HasPrefix(query, "create database") {
		return timodel.ActionCreateSchema
	}
	if strings.HasPrefix(query, "drop schema") || strings.HasPrefix(query, "drop database") {
		return timodel.ActionDropSchema
	}

	return timodel.ActionNone
}
