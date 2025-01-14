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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/charmap"
)

const tidbWaterMarkType = "TIDB_WATERMARK"

// The TiCDC Canal-JSON implementation extend the official format with a TiDB extension field.
// canalJSONMessageInterface is used to support this without affect the original format.
type canalJSONMessageInterface interface {
	getSchema() *string
	getTable() *string
	getCommitTs() uint64
	getPhysicalTableID() int64
	getTableID() int64
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

func (c *JSONMessage) getTableID() int64 {
	return 0
}

func (c *JSONMessage) getPhysicalTableID() int64 {
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
	CommitTs           uint64 `json:"commitTs,omitempty"`
	WatermarkTs        uint64 `json:"watermarkTs,omitempty"`
	OnlyHandleKey      bool   `json:"onlyHandleKey,omitempty"`
	ClaimCheckLocation string `json:"claimCheckLocation,omitempty"`
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

func (b *batchDecoder) queryTableInfo(msg canalJSONMessageInterface) *model.TableInfo {
	schema := *msg.getSchema()
	table := *msg.getTable()
	cacheKey := tableKey{
		schema: schema,
		table:  table,
	}
	tableInfo, ok := b.tableInfoCache[cacheKey]
	if !ok {
		partitionInfo := b.partitionInfoCache[cacheKey]
		tableInfo = newTableInfo(msg, partitionInfo)
		tableInfo.ID = b.tableIDAllocator.AllocateTableID(schema, table)
		if tableInfo.Partition != nil {
			for idx, partition := range tableInfo.Partition.Definitions {
				partitionID := b.tableIDAllocator.AllocatePartitionID(schema, table, partition.Name.O)
				tableInfo.Partition.Definitions[idx].ID = partitionID
			}
		}
		b.tableInfoCache[cacheKey] = tableInfo
	}
	return tableInfo
}

func newTableInfo(msg canalJSONMessageInterface, partitionInfo *timodel.PartitionInfo) *model.TableInfo {
	schema := *msg.getSchema()
	table := *msg.getTable()
	tidbTableInfo := &timodel.TableInfo{}
	tidbTableInfo.Name = pmodel.NewCIStr(table)

	rawColumns := msg.getData()
	pkNames := msg.pkNameSet()
	mysqlType := msg.getMySQLType()
	setColumnInfos(tidbTableInfo, rawColumns, mysqlType, pkNames)
	setIndexes(tidbTableInfo, pkNames)
	tidbTableInfo.Partition = partitionInfo
	return model.WrapTableInfo(100, schema, 1000, tidbTableInfo)
}

func (b *batchDecoder) setPhysicalTableID(event *model.RowChangedEvent, physicalTableID int64) error {
	if physicalTableID != 0 {
		event.PhysicalTableID = physicalTableID
		return nil
	}
	if event.TableInfo.Partition == nil {
		event.PhysicalTableID = event.TableInfo.ID
		return nil
	}
	switch event.TableInfo.Partition.Type {
	case pmodel.PartitionTypeRange:
		targetColumnID := event.TableInfo.ForceGetColumnIDByName(strings.ReplaceAll(event.TableInfo.Partition.Expr, "`", ""))
		columns := event.Columns
		if columns == nil {
			columns = event.PreColumns
		}
		var columnValue string
		for _, col := range columns {
			if col.ColumnID == targetColumnID {
				columnValue = model.ColumnValueString(col.Value)
				break
			}
		}
		for _, partition := range event.TableInfo.Partition.Definitions {
			lessThan := partition.LessThan[0]
			if lessThan == "MAXVALUE" {
				event.PhysicalTableID = partition.ID
				return nil
			}
			if len(columnValue) < len(lessThan) {
				event.PhysicalTableID = partition.ID
				return nil
			}
			if strings.Compare(columnValue, lessThan) == -1 {
				event.PhysicalTableID = partition.ID
				return nil
			}
		}
		return fmt.Errorf("cannot found partition for column value %s", columnValue)
	// todo: support following rule if meet the corresponding workload
	case pmodel.PartitionTypeHash:
		targetColumnID := event.TableInfo.ForceGetColumnIDByName(strings.ReplaceAll(event.TableInfo.Partition.Expr, "`", ""))
		columns := event.Columns
		if columns == nil {
			columns = event.PreColumns
		}
		var columnValue int64
		for _, col := range columns {
			if col.ColumnID == targetColumnID {
				columnValue = col.Value.(int64)
				break
			}
		}
		result := columnValue % int64(len(event.TableInfo.Partition.Definitions))
		partitionID := event.TableInfo.GetPartitionInfo().Definitions[result].ID
		event.PhysicalTableID = partitionID
		return nil
	case pmodel.PartitionTypeKey:
	case pmodel.PartitionTypeList:
	case pmodel.PartitionTypeNone:
	default:
	}
	return fmt.Errorf("manually set partition id for partition type %s not supported yet", event.TableInfo.Partition.Type)
}

func (b *batchDecoder) canalJSONMessage2RowChange() (*model.RowChangedEvent, error) {
	msg := b.msg
	result := new(model.RowChangedEvent)
	result.TableInfo = b.queryTableInfo(msg)
	result.CommitTs = msg.getCommitTs()

	mysqlType := msg.getMySQLType()
	var err error
	if msg.eventType() == canal.EventType_DELETE {
		// for `DELETE` event, `data` contain the old data, set it as the `PreColumns`
		result.PreColumns, err = canalJSONColumnMap2RowChangeColumns(msg.getData(), mysqlType, result.TableInfo)
		if err != nil {
			return nil, err
		}
		err = b.setPhysicalTableID(result, msg.getPhysicalTableID())
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// for `INSERT` and `UPDATE`, `data` contain fresh data, set it as the `Columns`
	result.Columns, err = canalJSONColumnMap2RowChangeColumns(msg.getData(), mysqlType, result.TableInfo)
	if err != nil {
		return nil, err
	}

	// for `UPDATE`, `old` contain old data, set it as the `PreColumns`
	if msg.eventType() == canal.EventType_UPDATE {
		preCols, err := canalJSONColumnMap2RowChangeColumns(msg.getOld(), mysqlType, result.TableInfo)
		if err != nil {
			return nil, err
		}
		if len(preCols) < len(result.Columns) {
			newPreCols := make([]*model.ColumnData, 0, len(preCols))
			j := 0
			// Columns are ordered by name
			for _, col := range result.Columns {
				if j < len(preCols) && col.ColumnID == preCols[j].ColumnID {
					newPreCols = append(newPreCols, preCols[j])
					j += 1
				} else {
					newPreCols = append(newPreCols, col)
				}
			}
			preCols = newPreCols
		}
		result.PreColumns = preCols
		if len(preCols) != len(result.Columns) {
			log.Panic("column count mismatch", zap.Any("preCols", preCols), zap.Any("cols", result.Columns))
		}
	}
	err = b.setPhysicalTableID(result, msg.getPhysicalTableID())
	if err != nil {
		return nil, err
	}
	return result, nil
}

func canalJSONColumnMap2RowChangeColumns(
	cols map[string]interface{},
	mysqlType map[string]string,
	tableInfo *model.TableInfo,
) ([]*model.ColumnData, error) {
	result := make([]*model.ColumnData, 0, len(cols))
	for _, columnInfo := range tableInfo.Columns {
		name := columnInfo.Name.O
		value, ok := cols[name]
		if !ok {
			continue
		}
		mysqlTypeStr, ok := mysqlType[name]
		if !ok {
			// this should not happen, else we have to check encoding for mysqlType.
			return nil, cerrors.ErrCanalDecodeFailed.GenWithStack(
				"mysql type does not found, column: %+v, mysqlType: %+v", name, mysqlType)
		}
		col := canalJSONFormatColumn(columnInfo.ID, value, mysqlTypeStr)
		result = append(result, col)
	}

	return result, nil
}

func canalJSONFormatColumn(columnID int64, value interface{}, mysqlTypeStr string) *model.ColumnData {
	mysqlType := utils.ExtractBasicMySQLType(mysqlTypeStr)
	result := &model.ColumnData{
		ColumnID: columnID,
		Value:    value,
	}
	if result.Value == nil {
		return result
	}

	data, ok := value.(string)
	if !ok {
		log.Panic("canal-json encoded message should have type in `string`")
	}

	var err error
	if utils.IsBinaryMySQLType(mysqlTypeStr) {
		// when encoding the `JavaSQLTypeBLOB`, use `ISO8859_1` decoder, now reverse it back.
		encoder := charmap.ISO8859_1.NewEncoder()
		value, err = encoder.String(data)
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.Any("col", result), zap.Error(err))
		}
		result.Value = value
		return result
	}

	switch mysqlType {
	case mysql.TypeBit, mysql.TypeSet:
		value, err = strconv.ParseUint(data, 10, 64)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeYear:
		value, err = strconv.ParseInt(data, 10, 64)
		if err != nil {
			log.Panic("invalid column value for int", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeEnum:
		value, err = strconv.ParseInt(data, 10, 64)
		if err != nil {
			log.Panic("invalid column value for enum", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeLonglong:
		value, err = strconv.ParseInt(data, 10, 64)
		if err != nil {
			value, err = strconv.ParseUint(data, 10, 64)
			if err != nil {
				log.Panic("invalid column value for bigint", zap.Any("col", result), zap.Error(err))
			}
		}
	case mysql.TypeFloat:
		value, err = strconv.ParseFloat(data, 32)
		if err != nil {
			log.Panic("invalid column value for float", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeDouble:
		value, err = strconv.ParseFloat(data, 64)
		if err != nil {
			log.Panic("invalid column value for double", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeTiDBVectorFloat32:
	}

	result.Value = value
	return result
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
