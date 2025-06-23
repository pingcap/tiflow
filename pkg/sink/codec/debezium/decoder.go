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
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// Decoder implement the RowEventDecoder interface
type Decoder struct {
	config *common.Config

	upstreamTiDB     *sql.DB
	tableIDAllocator *common.FakeTableIDAllocator

	keyPayload   map[string]interface{}
	keySchema    map[string]interface{}
	valuePayload map[string]interface{}
	valueSchema  map[string]interface{}
}

// NewDecoder return an debezium decoder
func NewDecoder(
	config *common.Config,
	db *sql.DB,
) codec.RowEventDecoder {
	return &Decoder{
		config:           config,
		upstreamTiDB:     db,
		tableIDAllocator: common.NewFakeTableIDAllocator(),
	}
}

// AddKeyValue add the received key and values to the Decoder
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

// HasNext returns whether there is any event need to be consumed
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
		return 0, errors.ErrDebeziumEmptyValueMessage
	}
	commitTs := d.getCommitTs()
	d.clear()
	return commitTs, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *Decoder) NextDDLEvent() (*model.DDLEvent, error) {
	if len(d.valuePayload) == 0 {
		return nil, errors.ErrDebeziumEmptyValueMessage
	}
	defer d.clear()
	event := new(model.DDLEvent)
	event.TableInfo = new(model.TableInfo)
	tableName := d.getTableName()
	if tableName != "" {
		event.TableInfo.TableName = model.TableName{
			Schema: d.getSchemaName(),
			Table:  tableName,
		}
	}
	event.Query = d.valuePayload["ddl"].(string)
	event.CommitTs = d.getCommitTs()
	return event, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *Decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if len(d.valuePayload) == 0 {
		return nil, errors.ErrDebeziumEmptyValueMessage
	}
	if d.config.DebeziumDisableSchema {
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("DebeziumDisableSchema is true")
	}
	if !d.config.EnableTiDBExtension {
		return nil, errors.ErrDebeziumInvalidMessage.GenWithStackByArgs("EnableTiDBExtension is false")
	}
	defer d.clear()
	tableInfo := d.getTableInfo()
	commitTs := d.getCommitTs()
	event := &model.RowChangedEvent{
		CommitTs:  commitTs,
		TableInfo: tableInfo,
	}
	if before, ok := d.valuePayload["before"].(map[string]interface{}); ok {
		event.PreColumns = assembleColumnData(before, tableInfo)
	}
	if after, ok := d.valuePayload["after"].(map[string]interface{}); ok {
		event.Columns = assembleColumnData(after, tableInfo)
	}
	event.PhysicalTableID = d.tableIDAllocator.AllocateTableID(tableInfo.GetSchemaName(), tableInfo.GetTableName())
	return event, nil
}

func (d *Decoder) getCommitTs() uint64 {
	source := d.valuePayload["source"].(map[string]interface{})
	commitTs, err := source["commit_ts"].(json.Number).Int64()
	if err != nil {
		log.Error("decode value failed", zap.Error(err), zap.Any("value", source))
	}
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

func (d *Decoder) clear() {
	d.keyPayload = nil
	d.keySchema = nil
	d.valuePayload = nil
	d.valueSchema = nil
}

func (d *Decoder) getTableInfo() *model.TableInfo {
	tidbTableInfo := new(timodel.TableInfo)
	tidbTableInfo.Name = pmodel.NewCIStr(d.getTableName())
	columnIDAllocator := model.NewIncrementalColumnIDAllocator()
	fields := d.valueSchema["fields"].([]interface{})
	after := fields[1].(map[string]interface{})
	columnsField := after["fields"].([]interface{})
	indexColumns := make([]*timodel.IndexColumn, 0, len(d.keyPayload))
	for idx, column := range columnsField {
		col := column.(map[string]interface{})
		colName := col["field"].(string)
		tidbType := col["tidb_type"].(string)
		optional := col["optional"].(bool)
		fieldType := parseTiDBType(tidbType, optional)
		if fieldType.GetType() == mysql.TypeDatetime {
			name := col["name"].(string)
			if name == "io.debezium.time.MicroTimestamp" {
				fieldType.SetDecimal(6)
			}
		}
		if _, ok := d.keyPayload[colName]; ok {
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name:   pmodel.NewCIStr(colName),
				Offset: idx,
			})
			fieldType.AddFlag(mysql.PriKeyFlag)
		}
		tidbTableInfo.Columns = append(tidbTableInfo.Columns, &timodel.ColumnInfo{
			State:     timodel.StatePublic,
			Name:      pmodel.NewCIStr(colName),
			FieldType: *fieldType,
			ID:        columnIDAllocator.GetColumnID(colName),
		})
	}
	tidbTableInfo.Indices = append(tidbTableInfo.Indices, &timodel.IndexInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("primary"),
		Columns: indexColumns,
		Unique:  true,
		Primary: true,
	})
	return model.WrapTableInfo(100, d.getSchemaName(), 100, tidbTableInfo)
}

func assembleColumnData(data map[string]interface{}, tableInfo *model.TableInfo) []*model.ColumnData {
	result := make([]*model.ColumnData, 0, len(data))
	for key, value := range data {
		columnID := tableInfo.ForceGetColumnIDByName(key)
		colInfo := tableInfo.GetColumnByID(columnID)
		result = append(result, decodeColumn(value, colInfo))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ColumnID > result[j].ColumnID
	})
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
	// Notice: value may be the default value of the column
	switch ft.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			s := value.(string)
			v, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
				return nil
			}
			value = v
		}
	case mysql.TypeDate, mysql.TypeNewDate:
		v, err := value.(json.Number).Int64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
		t := time.Unix(v*60*60*24, 0)
		value = t.UTC().String()
	case mysql.TypeDatetime:
		v, err := value.(json.Number).Int64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
		var t time.Time
		if ft.GetDecimal() <= 3 {
			t = time.UnixMilli(v)
		} else {
			t = time.UnixMicro(v)
		}
		value = t.UTC().String()
	case mysql.TypeDuration:
		v, err := value.(json.Number).Int64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
		d := types.NewDuration(0, 0, 0, int(v), types.MaxFsp)
		value = d.String()
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		v, err := value.(json.Number).Int64()
		if err != nil {
			log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
			return nil
		}
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			value = uint64(v)
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
		switch v := value.(type) {
		case string:
			b, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				log.Error("decode value failed", zap.Error(err), zap.Any("value", value))
				return nil
			}
			var buf [8]byte
			copy(buf[:len(b)], b)
			value = binary.LittleEndian.Uint64(buf[:])
		case bool:
			if v {
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

func parseTiDBType(tidbType string, optional bool) *ptypes.FieldType {
	ft := new(ptypes.FieldType)
	if optional {
		ft.AddFlag(mysql.NotNullFlag)
	}
	if strings.Contains(tidbType, " unsigned") {
		ft.AddFlag(mysql.UnsignedFlag)
		tidbType = strings.Replace(tidbType, " unsigned", "", 1)
	}
	if strings.Contains(tidbType, "blob") || strings.Contains(tidbType, "binary") {
		ft.AddFlag(mysql.BinaryFlag)
	}
	tp := ptypes.StrToType(tidbType)
	ft.SetType(tp)
	return ft
}

func decodeRawBytes(data []byte) (map[string]interface{}, map[string]interface{}, error) {
	var v map[string]interface{}
	d := json.NewDecoder(bytes.NewBuffer(data))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
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
