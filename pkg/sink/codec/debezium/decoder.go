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
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptype "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"

	"go.uber.org/zap"
)

type Decoder struct {
	config *common.Config

	upstreamTiDB     *sql.DB
	tableIDAllocator *common.FakeTableIDAllocator
	memo             common.TableInfoProvider
	// cachedMessages is used to store the messages which does not have received corresponding table info yet.
	cachedMessages *list.List
	// cachedRowChangedEvents are events just decoded from the cachedMessages
	cachedRowChangedEvents []*model.RowChangedEvent

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
	// schemaName := d.getSchemaName()
	tableName := d.getTableName()
	result := &model.DDLEvent{
		TableInfo: model.WrapTableInfo(100, "", 100, &timodel.TableInfo{}),
	}
	result.Query = d.valuePayload["ddl"].(string)
	result.CommitTs = d.getCommitTs()
	// tableName is empty when droping table
	if tableName != "" {
		result.TableInfo = d.getTableInfo()
		d.memo.Write(result.TableInfo)
	}
	for ele := d.cachedMessages.Front(); ele != nil; {
		d.unmarshalMsg(ele.Value)
		event, err := d.NextRowChangedEvent()
		if err != nil {
			return nil, err
		}
		d.cachedRowChangedEvents = append(d.cachedRowChangedEvents, event)

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
	tableInfo := d.memo.Read(schemaName, tableName, 0)
	if tableInfo == nil {
		log.Debug("table info not found for the event, "+
			"the consumer should cache this event temporarily, and update the tableInfo after it's received",
			zap.String("schema", schemaName),
			zap.String("table", tableName))
		d.cachedMessages.PushBack(d.marshalMsg())
		return nil, nil
	}
	commitTs := d.getCommitTs()
	event := &model.RowChangedEvent{
		CommitTs:  commitTs,
		TableInfo: tableInfo,
	}
	// set handleKey flag
	for name := range d.keyPayload {
		tableId := tableInfo.ForceGetColumnIDByName(name)
		colInfo := tableInfo.GetColumnByID(tableId)
		colInfo.AddFlag(uint(model.HandleKeyFlag))
		tableInfo.ColumnsFlag[tableId].SetIsHandleKey()
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
	commitTs := source["commit_ts"].(float64)
	return uint64(commitTs)
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

func (d *Decoder) marshalMsg() any {
	return []map[string]interface{}{d.keyPayload, d.keySchema, d.valuePayload, d.valueSchema}
}

func (d *Decoder) unmarshalMsg(value any) {
	msg := value.([]map[string]interface{})
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
	result := d.cachedRowChangedEvents
	d.cachedRowChangedEvents = nil
	return result
}

func (d *Decoder) getTableInfo() *model.TableInfo {
	tidbTableInfo := &timodel.TableInfo{}
	tableChanges := d.valuePayload["tableChanges"].([]interface{})
	tableChange := tableChanges[0].(map[string]interface{})
	if tableInfo, ok := tableChange["table"].(map[string]interface{}); ok {
		columns := tableInfo["columns"].([]interface{})
		columnIDAllocator := model.NewIncrementalColumnIDAllocator()
		for _, column := range columns {
			column := column.(map[string]interface{})
			colName := column["name"].(string)
			jdbcType := column["jdbcType"].(float64)
			position := column["position"].(float64)
			typeName := column["typeName"].(string)
			optional := column["optional"].(bool)
			generated := column["generated"].(bool)

			fieldType := ptype.FieldType{}
			if strings.Contains(typeName, "UNSIGNED") {
				fieldType.AddFlag(mysql.UnsignedFlag)
			}
			if !optional {
				fieldType.AddFlag(mysql.NotNullFlag)
			}
			if generated {
				fieldType.AddFlag(mysql.GeneratedColumnFlag)
			}
			switch internal.JavaSQLType(jdbcType) {
			case internal.JavaSQLTypeBLOB, internal.JavaSQLTypeVARBINARY, internal.JavaSQLTypeBINARY:
				fieldType.AddFlag(mysql.BinaryFlag)
			}
			fieldType.SetType(getMySQLType(typeName))
			if length, ok := column["length"].(float64); ok {
				switch fieldType.GetType() {
				case mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDatetime:
					fieldType.SetDecimal(int(length))
				case mysql.TypeYear, mysql.TypeNewDecimal,
					mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong,
					mysql.TypeLonglong, mysql.TypeFloat, mysql.TypeDouble,
					mysql.TypeBit, mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTiDBVectorFloat32:
					fieldType.SetFlen(int(length))
				}
			}
			if scale, ok := column["scale"].(float64); ok {
				switch fieldType.GetType() {
				case mysql.TypeNewDecimal, mysql.TypeFloat, mysql.TypeDouble:
					fieldType.SetDecimal(int(scale))
				}
			}
			tidbTableInfo.Columns = append(tidbTableInfo.Columns, &timodel.ColumnInfo{
				State:     timodel.StatePublic,
				Name:      pmodel.NewCIStr(colName),
				FieldType: fieldType,
				Offset:    int(position) - 1,
				ID:        columnIDAllocator.GetColumnID(colName),
			})
		}
	}
	database := d.getSchemaName()
	tidbTableInfo.Name = pmodel.NewCIStr(d.getTableName())
	return model.WrapTableInfo(100, database, 100, tidbTableInfo)
}

func assembleColumnData(data map[string]interface{}, tableInfo *model.TableInfo) []*model.ColumnData {
	result := make([]*model.ColumnData, 0, len(data))
	for key, value := range data {
		columnID := tableInfo.ForceGetColumnIDByName(key)
		colInfo := tableInfo.GetColumnByID(columnID)
		result = append(result, decodeColumn(value, colInfo))
	}
	return result
}

func decodeColumn(value interface{}, colInfo *timodel.ColumnInfo) *model.ColumnData {
	result := &model.ColumnData{
		ColumnID: colInfo.ID,
		Value:    value,
	}
	if value == nil {
		return result
	}
	ft := colInfo.FieldType
	switch ft.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		s := value.(string)
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			v, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
				return nil
			}
			value = v
		}
	case mysql.TypeDate, mysql.TypeNewDate:
		v := value.(float64)
		t := time.Unix(int64(v*60*60*24), 0)
		value = t.UTC().String()
	case mysql.TypeDatetime:
		v := value.(float64)
		var t time.Time
		if ft.GetDecimal() <= 3 {
			t = time.UnixMilli(int64(v))
		} else {
			t = time.UnixMicro(int64(v))
		}
		value = t.UTC().String()
	case mysql.TypeDuration:
		v := value.(float64)
		d, err := types.NumberToDuration(int64(v/1e6), ft.GetDecimal())
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
		value = d.String()
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		v := value.(float64)
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			// pay attention to loss of precision
			if v > 0 && uint64(v) > math.MaxInt64 {
				value = math.MaxInt64
			} else {
				value = uint64(v)
			}
		} else {
			if v > math.MaxInt64 {
				value = math.MaxInt64
			} else if v < math.MinInt64 {
				value = math.MinInt64
			} else {
				value = v
			}
		}
	case mysql.TypeBit:
		n := ft.GetFlen()
		if n != 0 {
			s := value.(string)
			v, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
				return nil
			}
			var buf [8]byte
			numBytes := n / 8
			if n%8 != 0 {
				numBytes += 1
			}
			copy(buf[:numBytes], v)
			value = binary.LittleEndian.Uint64(buf[:])
		} else {
			s := value.(bool)
			if s {
				value = uint64(1)
			} else {
				value = uint64(0)
			}
		}
	default:
	}
	result.Value = value
	return result
}

func getMySQLType(typeName string) byte {
	typeName = strings.Replace(typeName, " UNSIGNED ZEROFILL", "", 1)
	typeName = strings.Replace(typeName, " UNSIGNED", "", 1)
	typeName = strings.ToLower(typeName)
	return ptype.StrToType(typeName)
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
