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

package csv

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

// a csv row should at least contain operation-type, table-name, schema-name and one table column
const minimumColsCnt = 4

// operation specifies the operation type
type operation int

// enum types of operation
const (
	operationInsert operation = iota
	operationDelete
	operationUpdate
)

func (o operation) String() string {
	switch o {
	case operationInsert:
		return "I"
	case operationDelete:
		return "D"
	case operationUpdate:
		return "U"
	default:
		return "unknown"
	}
}

func (o *operation) FromString(op string) error {
	switch op {
	case "I":
		*o = operationInsert
	case "D":
		*o = operationDelete
	case "U":
		*o = operationUpdate
	default:
		return fmt.Errorf("invalid operation type %s", op)
	}

	return nil
}

type csvMessage struct {
	// config hold the codec configuration items.
	config *common.Config
	// opType denotes the specific operation type.
	opType     operation
	tableName  string
	schemaName string
	commitTs   uint64
	columns    []any
	preColumns []any
	// newRecord indicates whether we encounter a new record.
	newRecord bool
	HandleKey kv.Handle
}

func newCSVMessage(config *common.Config) *csvMessage {
	return &csvMessage{
		config:    config,
		newRecord: true,
	}
}

// encode returns a byte slice composed of the columns as follows:
// Col1: The operation-type indicator: I, D, U.
// Col2: Table name, the name of the source table.
// Col3: Schema name, the name of the source schema.
// Col4: Commit TS, the commit-ts of the source txn (optional).
// Col5-n: one or more columns that represent the data to be changed.
func (c *csvMessage) encode() []byte {
	strBuilder := new(strings.Builder)
	if c.opType == operationUpdate && c.config.OutputOldValue && len(c.preColumns) != 0 {
		// Encode the old value first as a dedicated row.
		c.encodeMeta("D", strBuilder)
		c.encodeColumns(c.preColumns, strBuilder)

		// Encode the after value as a dedicated row.
		c.newRecord = true // reset newRecord to true, so that the first column will not start with delimiter.
		c.encodeMeta("I", strBuilder)
		c.encodeColumns(c.columns, strBuilder)
	} else {
		c.encodeMeta(c.opType.String(), strBuilder)
		c.encodeColumns(c.columns, strBuilder)
	}
	return []byte(strBuilder.String())
}

func (c *csvMessage) encodeMeta(opType string, b *strings.Builder) {
	c.formatValue(opType, b)
	c.formatValue(c.tableName, b)
	c.formatValue(c.schemaName, b)
	if c.config.IncludeCommitTs {
		c.formatValue(c.commitTs, b)
	}
	if c.config.OutputOldValue {
		// When c.config.OutputOldValue, we need an extra column "is-updated"
		// to indicate whether the row is updated or just original insert/delete
		if c.opType == operationUpdate {
			c.formatValue(true, b)
		} else {
			c.formatValue(false, b)
		}
	}
	if c.config.OutputHandleKey {
		c.formatValue(c.HandleKey.String(), b)
	}
}

func (c *csvMessage) encodeColumns(columns []any, b *strings.Builder) {
	for _, col := range columns {
		c.formatValue(col, b)
	}
	b.WriteString(c.config.Terminator)
}

func (c *csvMessage) decode(datums []types.Datum) error {
	var dataColIdx int
	if len(datums) < minimumColsCnt {
		return cerror.WrapError(cerror.ErrCSVDecodeFailed,
			errors.New("the csv row should have at least four columns"+
				"(operation-type, table-name, schema-name, commit-ts)"))
	}

	if err := c.opType.FromString(datums[0].GetString()); err != nil {
		return cerror.WrapError(cerror.ErrCSVDecodeFailed, err)
	}
	dataColIdx++
	c.tableName = datums[1].GetString()
	dataColIdx++
	c.schemaName = datums[2].GetString()
	dataColIdx++
	if c.config.IncludeCommitTs {
		commitTs, err := strconv.ParseUint(datums[3].GetString(), 10, 64)
		if err != nil {
			return cerror.WrapError(cerror.ErrCSVDecodeFailed,
				fmt.Errorf("the 4th column(%s) of csv row should be a valid commit-ts", datums[3].GetString()))
		}
		c.commitTs = commitTs
		dataColIdx++
	} else {
		c.commitTs = 0
	}
	if c.config.OutputOldValue {
		// When c.config.OutputOldValue, we need an extra column "is-updated".
		// TODO: use this flag to guarantee data consistency in update uk/pk scenario.
		dataColIdx++
	}
	c.columns = c.columns[:0]

	for i := dataColIdx; i < len(datums); i++ {
		if datums[i].IsNull() {
			c.columns = append(c.columns, nil)
		} else {
			c.columns = append(c.columns, datums[i].GetString())
		}
	}

	return nil
}

// as stated in https://datatracker.ietf.org/doc/html/rfc4180,
// if double-quotes are used to enclose fields, then a double-quote
// appearing inside a field must be escaped by preceding it with
// another double quote.
func (c *csvMessage) formatWithQuotes(value string, strBuilder *strings.Builder) {
	quote := c.config.Quote

	strBuilder.WriteString(quote)
	// replace any quote in csv column with two quotes.
	strBuilder.WriteString(strings.ReplaceAll(value, quote, quote+quote))
	strBuilder.WriteString(quote)
}

// formatWithEscapes escapes the csv column if necessary.
func (c *csvMessage) formatWithEscapes(value string, strBuilder *strings.Builder) {
	lastPos := 0
	delimiter := c.config.Delimiter

	for i := 0; i < len(value); i++ {
		ch := value[i]
		isDelimiterStart := strings.HasPrefix(value[i:], delimiter)
		// if '\r', '\n', '\' or the delimiter (may have multiple characters) are contained in
		// csv column, we should escape these characters.
		if ch == config.CR || ch == config.LF || ch == config.Backslash || isDelimiterStart {
			// write out characters up until this position.
			strBuilder.WriteString(value[lastPos:i])
			switch ch {
			case config.LF:
				ch = 'n'
			case config.CR:
				ch = 'r'
			}
			strBuilder.WriteRune(config.Backslash)
			strBuilder.WriteRune(rune(ch))

			// escape each characters in delimiter.
			if isDelimiterStart {
				for k := 1; k < len(c.config.Delimiter); k++ {
					strBuilder.WriteRune(config.Backslash)
					strBuilder.WriteRune(rune(delimiter[k]))
				}
				lastPos = i + len(delimiter)
			} else {
				lastPos = i + 1
			}
		}
	}
	strBuilder.WriteString(value[lastPos:])
}

// formatValue formats the csv column and appends it to a string builder.
func (c *csvMessage) formatValue(value any, strBuilder *strings.Builder) {
	defer func() {
		// reset newRecord to false after handing the first csv column
		c.newRecord = false
	}()

	if !c.newRecord {
		strBuilder.WriteString(c.config.Delimiter)
	}

	if value == nil {
		strBuilder.WriteString(c.config.NullString)
		return
	}

	switch v := value.(type) {
	case string:
		// if quote is configured, format the csv column with quotes,
		// otherwise escape this csv column.
		if len(c.config.Quote) != 0 {
			c.formatWithQuotes(v, strBuilder)
		} else {
			c.formatWithEscapes(v, strBuilder)
		}
	default:
		strBuilder.WriteString(fmt.Sprintf("%v", v))
	}
}

func fromCsvValToColValue(csvConfig *common.Config, csvVal any, ft types.FieldType) (any, error) {
	str, ok := csvVal.(string)
	if !ok {
		return csvVal, nil
	}

	switch ft.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if ft.GetCharset() == charset.CharsetBin {
			switch csvConfig.BinaryEncodingMethod {
			case config.BinaryEncodingBase64:
				return base64.StdEncoding.DecodeString(str)
			case config.BinaryEncodingHex:
				return hex.DecodeString(str)
			default:
				return nil, cerror.WrapError(cerror.ErrCSVEncodeFailed,
					errors.Errorf("unsupported binary encoding method %s",
						csvConfig.BinaryEncodingMethod))
			}
		}

		return []byte(str), nil
	case mysql.TypeFloat:
		val, err := strconv.ParseFloat(str, 32)
		return val, err
	case mysql.TypeDouble:
		val, err := strconv.ParseFloat(str, 64)
		return val, err
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			val, err := strconv.ParseUint(str, 10, 64)
			return val, err
		}
		val, err := strconv.ParseInt(str, 10, 64)
		return val, err
	case mysql.TypeBit:
		val, err := strconv.ParseUint(str, 10, 64)
		return val, err
	default:
		return str, nil
	}
}

// fromColValToCsvVal converts column from TiDB type to csv type.
func fromColValToCsvVal(csvConfig *common.Config, col model.ColumnDataX, ft *types.FieldType) (any, error) {
	if col.Value == nil {
		return nil, nil
	}

	switch col.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if col.GetFlag().IsBinary() {
			if v, ok := col.Value.([]byte); ok {
				switch csvConfig.BinaryEncodingMethod {
				case config.BinaryEncodingBase64:
					return base64.StdEncoding.EncodeToString(v), nil
				case config.BinaryEncodingHex:
					return hex.EncodeToString(v), nil
				default:
					return nil, cerror.WrapError(cerror.ErrCSVEncodeFailed,
						errors.Errorf("unsupported binary encoding method %s",
							csvConfig.BinaryEncodingMethod))
				}
			}
			return col.Value, nil
		}
		if v, ok := col.Value.([]byte); ok {
			return string(v), nil
		}
		return col.Value, nil
	case mysql.TypeEnum:
		if v, ok := col.Value.(string); ok {
			return v, nil
		}
		enumVar, err := types.ParseEnumValue(ft.GetElems(), col.Value.(uint64))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrCSVEncodeFailed, err)
		}
		return enumVar.Name, nil
	case mysql.TypeSet:
		if v, ok := col.Value.(string); ok {
			return v, nil
		}
		setVar, err := types.ParseSetValue(ft.GetElems(), col.Value.(uint64))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrCSVEncodeFailed, err)
		}
		return setVar.Name, nil
	case mysql.TypeTiDBVectorFloat32:
		if vec, ok := col.Value.(types.VectorFloat32); ok {
			return vec.String(), nil
		}
		return nil, cerror.ErrCSVEncodeFailed
	default:
		return col.Value, nil
	}
}

// rowChangedEvent2CSVMsg converts a RowChangedEvent to a csv record.
func rowChangedEvent2CSVMsg(csvConfig *common.Config, e *model.RowChangedEvent) (*csvMessage, error) {
	var err error

	csvMsg := &csvMessage{
		config:     csvConfig,
		tableName:  e.TableInfo.GetTableName(),
		schemaName: e.TableInfo.GetSchemaName(),
		commitTs:   e.CommitTs,
		newRecord:  true,
	}

	if csvConfig.OutputHandleKey {
		csvMsg.HandleKey = e.HandleKey
	}

	if e.IsDelete() {
		csvMsg.opType = operationDelete
		csvMsg.columns, err = rowChangeColumns2CSVColumns(csvConfig, e.PreColumns, e.TableInfo)
		if err != nil {
			return nil, err
		}
	} else {
		if e.PreColumns == nil {
			// This is a insert operation.
			csvMsg.opType = operationInsert
			csvMsg.columns, err = rowChangeColumns2CSVColumns(csvConfig, e.Columns, e.TableInfo)
			if err != nil {
				return nil, err
			}
		} else {
			// This is a update operation.
			csvMsg.opType = operationUpdate
			if csvConfig.OutputOldValue {
				if len(e.PreColumns) != len(e.Columns) {
					return nil, cerror.WrapError(cerror.ErrCSVDecodeFailed,
						fmt.Errorf("the column length of preColumns %d doesn't equal to that of columns %d",
							len(e.PreColumns), len(e.Columns)))
				}
				csvMsg.preColumns, err = rowChangeColumns2CSVColumns(csvConfig, e.PreColumns, e.TableInfo)
				if err != nil {
					return nil, err
				}
			}
			csvMsg.columns, err = rowChangeColumns2CSVColumns(csvConfig, e.Columns, e.TableInfo)
			if err != nil {
				return nil, err
			}
		}
	}
	return csvMsg, nil
}

func csvMsg2RowChangedEvent(csvConfig *common.Config, csvMsg *csvMessage, tableInfo *model.TableInfo) (*model.RowChangedEvent, error) {
	var err error
	if len(csvMsg.columns) != len(tableInfo.Columns) {
		return nil, cerror.WrapError(cerror.ErrCSVDecodeFailed,
			fmt.Errorf("the column length of csv message %d doesn't equal to that of tableInfo %d",
				len(csvMsg.columns), len(tableInfo.Columns)))
	}

	e := new(model.RowChangedEvent)
	e.CommitTs = csvMsg.commitTs
	e.TableInfo = tableInfo
	if csvMsg.opType == operationDelete {
		e.PreColumns, err = csvColumns2RowChangeColumns(csvConfig, csvMsg.columns, tableInfo.Columns)
	} else {
		e.Columns, err = csvColumns2RowChangeColumns(csvConfig, csvMsg.columns, tableInfo.Columns)
	}

	if err != nil {
		return nil, err
	}
	return e, nil
}

func rowChangeColumns2CSVColumns(csvConfig *common.Config, cols []*model.ColumnData, tableInfo *model.TableInfo) ([]any, error) {
	var csvColumns []any
	colInfos := tableInfo.GetColInfosForRowChangedEvent()
	for i, column := range cols {
		// column could be nil in a condition described in
		// https://github.com/pingcap/tiflow/issues/6198#issuecomment-1191132951
		if column == nil {
			continue
		}

		converted, err := fromColValToCsvVal(csvConfig, model.GetColumnDataX(column, tableInfo), colInfos[i].Ft)
		if err != nil {
			return nil, errors.Trace(err)
		}
		csvColumns = append(csvColumns, converted)
	}

	return csvColumns, nil
}

func csvColumns2RowChangeColumns(csvConfig *common.Config, csvCols []any, ticols []*timodel.ColumnInfo) ([]*model.ColumnData, error) {
	cols := make([]*model.ColumnData, 0, len(csvCols))
	for idx, csvCol := range csvCols {
		col := new(model.ColumnData)

		ticol := ticols[idx]
		col.ColumnID = ticol.ID

		val, err := fromCsvValToColValue(csvConfig, csvCol, ticol.FieldType)
		if err != nil {
			return cols, err
		}
		col.Value = val
		cols = append(cols, col)
	}

	return cols, nil
}
