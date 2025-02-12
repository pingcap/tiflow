// Copyright 2025 PingCAP, Inc.
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
	"container/list"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

type Decoder struct {
	config *common.Config

	upstreamTiDB     *sql.DB
	tableIDAllocator *common.FakeTableIDAllocator
	memo             common.TableInfoProvider
	// cachedMessages is used to store the messages which does not have received corresponding table info yet.
	cachedMessages *list.List
	// CachedRowChangedEvents are events just decoded from the cachedMessages
	CachedRowChangedEvents []*model.RowChangedEvent

	keyPayload   map[string]interface{}
	keySchema    map[string]interface{}
	valuePayload map[string]interface{}
	valueSchema  map[string]interface{}
}

// NewDecoder return an avro decoder
func NewDecoder(
	config *common.Config,
	db *sql.DB,
) codec.RowEventDecoder {
	return &Decoder{
		config:           config,
		upstreamTiDB:     db,
		tableIDAllocator: common.NewFakeTableIDAllocator(),
		memo:             common.NewMemoryTableInfoProvider(),
		cachedMessages:   list.New(),
	}
}

func (d *Decoder) AddKeyValue(key, value []byte) error {
	if d.valuePayload != nil || d.valueSchema != nil {
		return errors.New("key or value is not nil")
	}
	keyPayload, keySchema, err := decodeRawBytes(key)
	if err != nil {
		return errors.ErrDebeziumEncodeFailed.FastGenByArgs(err)
	}
	valuePayload, valueSchema, err := decodeRawBytes(value)
	if err != nil {
		return errors.ErrDebeziumEncodeFailed.FastGenByArgs(err)
	}
	d.keyPayload = keyPayload
	d.keySchema = keySchema
	d.valuePayload = valuePayload
	d.valueSchema = valueSchema
	return nil
}

func (d *Decoder) HasNext() (model.MessageType, bool, error) {
	if d.valuePayload == nil && d.valueSchema == nil {
		return model.MessageTypeUnknown, false, nil
	}

	if len(d.valuePayload) < 1 {
		return model.MessageTypeUnknown, false, errors.ErrDebeziumInvalidMessage.FastGenByArgs(d.valuePayload)
	}
	op, ok := d.valuePayload["op"]
	if !ok {
		return model.MessageTypeDDL, true, nil
	}
	switch op {
	case "c", "u", "d":
		return model.MessageTypeRow, true, nil
	case "m":
		return model.MessageTypeResolved, true, nil
	}
	return model.MessageTypeUnknown, false, errors.ErrDebeziumInvalidMessage.FastGenByArgs(d.valuePayload)
}

// NextResolvedEvent returns the next resolved event if exists
func (d *Decoder) NextResolvedEvent() (uint64, error) {
	if len(d.valuePayload) == 0 {
		return 0, errors.New("value should not be empty")
	}
	commitTs := d.getCommitTs()
	d.reset()
	return commitTs, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *Decoder) NextDDLEvent() (*model.DDLEvent, error) {
	if len(d.valuePayload) == 0 {
		return nil, errors.New("value should not be empty")
	}
	defer d.reset()
	schemaName := d.getSchemaName()
	tableName := d.getTableName()
	result := new(model.DDLEvent)
	result.Query = d.valuePayload["ddl"].(string)
	result.CommitTs = d.getCommitTs()
	if tableName != "" {
		tableChanges := d.valuePayload["tableChanges"].([]map[string]interface{})
		tableInfo, ok := tableChanges[0]["table"].(map[string]interface{})
		if !ok {
			// there is no table info when droping table
			return result, nil
		}
		pkNames := make(map[string]struct{}, 0)
		columns := assembleColumnInfo(tableInfo)
		result.TableInfo = model.BuildTableInfoWithPKNames4Test(schemaName, tableName, columns, pkNames)
		d.memo.Write(result.TableInfo)
	}
	for ele := d.cachedMessages.Front(); ele != nil; {
		d.resolveMsg(ele)
		event, err := d.NextRowChangedEvent()
		if err != nil {
			return nil, err
		}
		d.CachedRowChangedEvents = append(d.CachedRowChangedEvents, event)

		next := ele.Next()
		d.cachedMessages.Remove(ele)
		ele = next
	}
	return result, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *Decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if len(d.valuePayload) == 0 {
		return nil, errors.New("value should not be empty")
	}
	defer d.reset()
	schemaName := d.getSchemaName()
	tableName := d.getTableName()
	commitTs := d.getCommitTs()
	tableInfo := d.memo.Read(schemaName, tableName, commitTs)
	if tableInfo == nil {
		log.Debug("table info not found for the event, "+
			"the consumer should cache this event temporarily, and update the tableInfo after it's received",
			zap.String("schema", schemaName),
			zap.String("table", tableName),
			zap.Uint64("version", commitTs))
		d.cachedMessages.PushBack(d.cacheMsg())
		return nil, nil
	}
	event := &model.RowChangedEvent{
		CommitTs:  commitTs,
		TableInfo: tableInfo,
	}
	// set handleKey flag
	for name, _ := range d.keyPayload {
		tableId := tableInfo.ForceGetColumnIDByName(name)
		colInfo := tableInfo.GetColumnByID(tableId)
		colInfo.SetFlag(uint(model.HandleKeyFlag))
	}
	if before, ok := d.valuePayload["before"].(map[string]interface{}); ok {
		event.PreColumns = assembleColumnData(before, tableInfo)
	}
	if after, ok := d.valuePayload["after"].(map[string]interface{}); ok {
		event.Columns = assembleColumnData(after, tableInfo)
	}
	event.PhysicalTableID = d.tableIDAllocator.AllocateTableID(event.TableInfo.GetSchemaName(), event.TableInfo.GetTableName())

	return event, nil
}

func (d *Decoder) getCommitTs() uint64 {
	source := d.valuePayload["source"].(map[string]interface{})
	commitTs := source["commit_ts"].(uint64)
	return commitTs
}

func (d *Decoder) getSchemaName() string {
	source := d.valuePayload["source"].(map[string]interface{})
	schemaName := source["db"].(string)
	return schemaName
}

func (d *Decoder) getTableName() string {
	source := d.valuePayload["source"].(map[string]interface{})
	tableName := source["table"].(string)
	return tableName
}

func (d *Decoder) cacheMsg() any {
	return []map[string]interface{}{d.keyPayload, d.keySchema, d.valuePayload, d.valueSchema}
}

func (d *Decoder) resolveMsg(ele interface{}) {
	msg := ele.([]map[string]interface{})
	d.keyPayload = msg[0]
	d.keySchema = msg[1]
	d.valuePayload = msg[2]
	d.valueSchema = msg[3]
}

func (d *Decoder) reset() {
	d.keyPayload = nil
	d.keySchema = nil
	d.valuePayload = nil
	d.valueSchema = nil
}

// GetCachedEvents returns the cached events
func (d *Decoder) GetCachedEvents() []*model.RowChangedEvent {
	result := d.CachedRowChangedEvents
	d.CachedRowChangedEvents = nil
	return result
}

func assembleColumnInfo(data map[string]interface{}) []*model.Column {
	columns := data["columns"].([]map[string]interface{})
	result := make([]*model.Column, 0, len(columns))
	for _, column := range columns {
		colName := column["name"].(string)
		typeName := column["typeName"].(string)
		optional := column["optional"].(bool)
		generated := column["generated"].(bool)
		result = append(result, &model.Column{
			Name: colName,
			Type: getMySQLType(typeName),
			Flag: getFlag(typeName, optional, generated),
		})
	}
	return result
}

func assembleColumnData(data map[string]interface{}, tableInfo *model.TableInfo) []*model.ColumnData {
	result := make([]*model.ColumnData, 0, len(data))
	for key, value := range data {
		result = append(result, &model.ColumnData{
			ColumnID: tableInfo.ForceGetColumnIDByName(key),
			Value:    value,
		})
	}
	return result
}

func getMySQLType(typeName string) byte {
	typeName = strings.Replace(typeName, " UNSIGNED ZEROFILL", "", 1)
	typeName = strings.Replace(typeName, " UNSIGNED", "", 1)
	typeName = strings.ToLower(typeName)
	return types.StrToType(typeName)
}

func getFlag(typeName string, optional, generated bool) model.ColumnFlagType {
	var flag model.ColumnFlagType
	if strings.Contains(typeName, "UNSIGNED") {
		flag.SetIsUnsigned()
	}
	if optional {
		flag.SetIsNullable()
	}
	if generated {
		flag.SetIsGeneratedColumn()
	}
	return flag
}

func decodeRawBytes(data []byte) (map[string]interface{}, map[string]interface{}, error) {
	var v map[string]interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, nil, errors.Trace(err)
	}
	payload, ok := v["payload"].(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("decode payload failed, data: %+v", v)
	}
	schema, ok := v["schema"].(map[string]interface{})
	if !ok {
		return nil, nil, fmt.Errorf("decode payload failed, data: %+v", v)
	}
	return payload, schema, nil
}
