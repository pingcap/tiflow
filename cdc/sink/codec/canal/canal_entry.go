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
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto" // nolint:staticcheck
	"github.com/pingcap/errors"
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	canal "github.com/pingcap/tiflow/proto/canal"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

// compatible with canal-1.1.4
// https://github.com/alibaba/canal/tree/canal-1.1.4
const (
	CanalPacketVersion   int32  = 1
	CanalProtocolVersion int32  = 1
	CanalServerEncode    string = "UTF-8"
)

type canalEntryBuilder struct {
	bytesDecoder *encoding.Decoder // default charset is ISO-8859-1
}

// newCanalEntryBuilder creates a new canalEntryBuilder
func newCanalEntryBuilder() *canalEntryBuilder {
	return &canalEntryBuilder{
		bytesDecoder: charmap.ISO8859_1.NewDecoder(),
	}
}

// build the header of a canal entry
func (b *canalEntryBuilder) buildHeader(commitTs uint64, schema string, table string, eventType canal.EventType, rowCount int) *canal.Header {
	t := convertToCanalTs(commitTs)
	h := &canal.Header{
		VersionPresent:    &canal.Header_Version{Version: CanalProtocolVersion},
		ServerenCode:      CanalServerEncode,
		ExecuteTime:       t,
		SourceTypePresent: &canal.Header_SourceType{SourceType: canal.Type_MYSQL},
		SchemaName:        schema,
		TableName:         table,
		EventTypePresent:  &canal.Header_EventType{EventType: eventType},
	}
	if rowCount > 0 {
		p := &canal.Pair{
			Key:   "rowsCount",
			Value: strconv.Itoa(rowCount),
		}
		h.Props = append(h.Props, p)
	}
	return h
}

// In the official canal-json implementation, value were extracted from binlog buffer.
// see https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogBuffer.java#L276-L1147
// all value will be represented in string type
// see https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L760-L855
func (b *canalEntryBuilder) formatValue(value interface{}, javaType internal.JavaSQLType) (result string, err error) {
	// value would be nil, if no value insert for the column.
	if value == nil {
		return "", nil
	}

	switch v := value.(type) {
	case int64:
		result = strconv.FormatInt(v, 10)
	case uint64:
		result = strconv.FormatUint(v, 10)
	case float32:
		result = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		result = strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		result = v
	case []byte:
		//  JavaSQLTypeVARCHAR / JavaSQLTypeCHAR / JavaSQLTypeBLOB / JavaSQLTypeCLOB /
		// special handle for text and blob
		// see https://github.com/alibaba/canal/blob/9f6021cf36f78cc8ac853dcf37a1769f359b868b/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L801
		switch javaType {
		// for normal text
		case internal.JavaSQLTypeVARCHAR, internal.JavaSQLTypeCHAR, internal.JavaSQLTypeCLOB:
			result = string(v)
		default:
			// JavaSQLTypeBLOB
			decoded, err := b.bytesDecoder.Bytes(v)
			if err != nil {
				return "", err
			}
			result = string(decoded)
		}
	default:
		result = fmt.Sprintf("%v", v)
	}
	return result, nil
}

// build the Column in the canal RowData
// see https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L756-L872
func (b *canalEntryBuilder) buildColumn(c *model.Column, colName string, updated bool) (*canal.Column, error) {
	mysqlType := getMySQLType(c)
	javaType, err := getJavaSQLType(c, mysqlType)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	value, err := b.formatValue(c.Value, javaType)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	canalColumn := &canal.Column{
		SqlType:       int32(javaType),
		Name:          colName,
		IsKey:         c.Flag.IsPrimaryKey(),
		Updated:       updated,
		IsNullPresent: &canal.Column_IsNull{IsNull: c.Value == nil},
		Value:         value,
		MysqlType:     mysqlType,
	}
	return canalColumn, nil
}

// build the RowData of a canal entry
func (b *canalEntryBuilder) buildRowData(e *model.RowChangedEvent, onlyHandleKeyColumns bool) (*canal.RowData, error) {
	var columns []*canal.Column
	for _, column := range e.Columns {
		if column == nil {
			continue
		}
		c, err := b.buildColumn(column, column.Name, !e.IsDelete())
		if err != nil {
			return nil, errors.Trace(err)
		}
		columns = append(columns, c)
	}

	onlyHandleKeyColumns = onlyHandleKeyColumns && e.IsDelete()
	var preColumns []*canal.Column
	for _, column := range e.PreColumns {
		if column == nil {
			continue
		}
		if onlyHandleKeyColumns && !column.Flag.IsHandleKey() {
			continue
		}
		c, err := b.buildColumn(column, column.Name, !e.IsDelete())
		if err != nil {
			return nil, errors.Trace(err)
		}
		preColumns = append(preColumns, c)
	}

	rowData := &canal.RowData{}
	rowData.BeforeColumns = preColumns
	rowData.AfterColumns = columns
	return rowData, nil
}

// fromRowEvent builds canal entry from cdc RowChangedEvent
func (b *canalEntryBuilder) fromRowEvent(e *model.RowChangedEvent, onlyHandleKeyColumns bool) (*canal.Entry, error) {
	eventType := convertRowEventType(e)
	header := b.buildHeader(e.CommitTs, e.Table.Schema, e.Table.Table, eventType, 1)
	isDdl := isCanalDDL(eventType) // false
	rowData, err := b.buildRowData(e, onlyHandleKeyColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rc := &canal.RowChange{
		EventTypePresent: &canal.RowChange_EventType{EventType: eventType},
		IsDdlPresent:     &canal.RowChange_IsDdl{IsDdl: isDdl},
		RowDatas:         []*canal.RowData{rowData},
	}
	rcBytes, err := proto.Marshal(rc)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	// build entry
	entry := &canal.Entry{
		Header:           header,
		EntryTypePresent: &canal.Entry_EntryType{EntryType: canal.EntryType_ROWDATA},
		StoreValue:       rcBytes,
	}
	return entry, nil
}

// fromDDLEvent builds canal entry from cdc DDLEvent
func (b *canalEntryBuilder) fromDDLEvent(e *model.DDLEvent) (*canal.Entry, error) {
	eventType := convertDdlEventType(e)
	header := b.buildHeader(e.CommitTs, e.TableInfo.TableName.Schema, e.TableInfo.TableName.Table, eventType, -1)
	isDdl := isCanalDDL(eventType)
	rc := &canal.RowChange{
		EventTypePresent: &canal.RowChange_EventType{EventType: eventType},
		IsDdlPresent:     &canal.RowChange_IsDdl{IsDdl: isDdl},
		Sql:              e.Query,
		RowDatas:         nil,
		DdlSchemaName:    e.TableInfo.TableName.Schema,
	}
	rcBytes, err := proto.Marshal(rc)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	// build entry
	entry := &canal.Entry{
		Header:           header,
		EntryTypePresent: &canal.Entry_EntryType{EntryType: canal.EntryType_ROWDATA},
		StoreValue:       rcBytes,
	}
	return entry, nil
}

// convert ts in tidb to timestamp(in ms) in canal
func convertToCanalTs(commitTs uint64) int64 {
	return int64(commitTs >> 18)
}

// get the canal EventType according to the RowChangedEvent
func convertRowEventType(e *model.RowChangedEvent) canal.EventType {
	if e.IsDelete() {
		return canal.EventType_DELETE
	}
	if len(e.PreColumns) == 0 {
		return canal.EventType_INSERT
	}
	return canal.EventType_UPDATE
}

// get the canal EventType according to the DDLEvent
func convertDdlEventType(e *model.DDLEvent) canal.EventType {
	// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/ddl/DruidDdlParser.java#L59-L178
	switch e.Type {
	case mm.ActionCreateSchema, mm.ActionDropSchema, mm.ActionShardRowID, mm.ActionCreateView,
		mm.ActionDropView, mm.ActionRecoverTable, mm.ActionModifySchemaCharsetAndCollate,
		mm.ActionLockTable, mm.ActionUnlockTable, mm.ActionRepairTable, mm.ActionSetTiFlashReplica,
		mm.ActionUpdateTiFlashReplicaStatus, mm.ActionCreateSequence, mm.ActionAlterSequence,
		mm.ActionDropSequence, mm.ActionModifyTableAutoIdCache, mm.ActionRebaseAutoRandomBase:
		return canal.EventType_QUERY
	case mm.ActionCreateTable:
		return canal.EventType_CREATE
	case mm.ActionRenameTable, mm.ActionRenameTables:
		return canal.EventType_RENAME
	case mm.ActionAddIndex, mm.ActionAddForeignKey, mm.ActionAddPrimaryKey:
		return canal.EventType_CINDEX
	case mm.ActionDropIndex, mm.ActionDropForeignKey, mm.ActionDropPrimaryKey:
		return canal.EventType_DINDEX
	case mm.ActionAddColumn, mm.ActionDropColumn, mm.ActionModifyColumn, mm.ActionRebaseAutoID,
		mm.ActionSetDefaultValue, mm.ActionModifyTableComment, mm.ActionRenameIndex, mm.ActionAddTablePartition,
		mm.ActionDropTablePartition, mm.ActionModifyTableCharsetAndCollate, mm.ActionTruncateTablePartition,
		mm.ActionAlterIndexVisibility, mm.ActionMultiSchemaChange,
		// AddColumns and DropColumns are removed in TiDB v6.2.0, see https://github.com/pingcap/tidb/pull/35862.
		mm.ActionAddColumns, mm.ActionDropColumns:
		return canal.EventType_ALTER
	case mm.ActionDropTable:
		return canal.EventType_ERASE
	case mm.ActionTruncateTable:
		return canal.EventType_TRUNCATE
	default:
		return canal.EventType_QUERY
	}
}

func isCanalDDL(t canal.EventType) bool {
	// see https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L297
	switch t {
	case canal.EventType_CREATE,
		canal.EventType_RENAME,
		canal.EventType_CINDEX,
		canal.EventType_DINDEX,
		canal.EventType_ALTER,
		canal.EventType_ERASE,
		canal.EventType_TRUNCATE,
		canal.EventType_QUERY:
		return true
	}
	return false
}

func getJavaSQLType(c *model.Column, mysqlType string) (result internal.JavaSQLType, err error) {
	javaType := internal.MySQLType2JavaType(c.Type, c.Flag.IsBinary())

	switch javaType {
	case internal.JavaSQLTypeBINARY, internal.JavaSQLTypeVARBINARY, internal.JavaSQLTypeLONGVARBINARY:
		if strings.Contains(mysqlType, "text") {
			return internal.JavaSQLTypeCLOB, nil
		}
		return internal.JavaSQLTypeBLOB, nil
	}

	// flag `isUnsigned` only for `numerical` and `bit`, `year` data type.
	if !c.Flag.IsUnsigned() {
		return javaType, nil
	}

	// for year, to `int64`, others to `uint64`.
	// no need to promote type for `year` and `bit`
	if c.Type == mysql.TypeYear || c.Type == mysql.TypeBit {
		return javaType, nil
	}

	if c.Type == mysql.TypeFloat || c.Type == mysql.TypeDouble || c.Type == mysql.TypeNewDecimal {
		return javaType, nil
	}

	// for **unsigned** integral types, type would be `uint64` or `string`. see reference:
	// https://github.com/pingcap/tiflow/blob/1e3dd155049417e3fd7bf9b0a0c7b08723b33791/cdc/entry/mounter.go#L501
	// https://github.com/pingcap/tidb/blob/6495a5a116a016a3e077d181b8c8ad81f76ac31b/types/datum.go#L423-L455
	if c.Value == nil {
		return javaType, nil
	}
	var number uint64
	switch v := c.Value.(type) {
	case uint64:
		number = v
	case string:
		a, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return javaType, err
		}
		number = a
	default:
		return javaType, errors.Errorf("unexpected type for unsigned value: %+v, column: %+v", reflect.TypeOf(v), c)
	}

	// Some special cases handled in canal
	// see https://github.com/alibaba/canal/blob/d53bfd7ee76f8fe6eb581049d64b07d4fcdd692d/parse/src/main/java/com/alibaba/otter/canal/parse/inbound/mysql/dbsync/LogEventConvert.java#L733
	// For unsigned type, promote by the following rule:
	// TinyInt,  1byte, [-128, 127], [0, 255], if a > 127
	// SmallInt, 2byte, [-32768, 32767], [0, 65535], if a > 32767
	// Int,      4byte, [-2147483648, 2147483647], [0, 4294967295], if a > 2147483647
	// BigInt,   8byte, [-2<<63, 2 << 63 - 1], [0, 2 << 64 - 1], if a > 2 << 63 - 1
	switch c.Type {
	case mysql.TypeTiny:
		if number > math.MaxInt8 {
			javaType = internal.JavaSQLTypeSMALLINT
		}
	case mysql.TypeShort:
		if number > math.MaxInt16 {
			javaType = internal.JavaSQLTypeINTEGER
		}
	case mysql.TypeLong:
		if number > math.MaxInt32 {
			javaType = internal.JavaSQLTypeBIGINT
		}
	case mysql.TypeLonglong:
		if number > math.MaxInt64 {
			javaType = internal.JavaSQLTypeDECIMAL
		}
	}

	return javaType, nil
}

// when encoding the canal format, for unsigned mysql type, add `unsigned` keyword.
// it should have the form `t unsigned`, such as `int unsigned`
func withUnsigned4MySQLType(mysqlType string, unsigned bool) string {
	if unsigned && mysqlType != "bit" && mysqlType != "year" {
		return mysqlType + " unsigned"
	}
	return mysqlType
}

// when decoding the canal format, remove `unsigned` to get the original `mysql type`.
func trimUnsignedFromMySQLType(mysqlType string) string {
	return strings.TrimSuffix(mysqlType, " unsigned")
}

func getMySQLType(c *model.Column) string {
	mysqlType := types.TypeStr(c.Type)
	// make `mysqlType` representation keep the same as the canal official implementation
	mysqlType = withUnsigned4MySQLType(mysqlType, c.Flag.IsUnsigned())

	if !c.Flag.IsBinary() {
		return mysqlType
	}

	if types.IsTypeBlob(c.Type) {
		return strings.Replace(mysqlType, "text", "blob", 1)
	}

	if types.IsTypeChar(c.Type) {
		return strings.Replace(mysqlType, "char", "binary", 1)
	}

	return mysqlType
}
