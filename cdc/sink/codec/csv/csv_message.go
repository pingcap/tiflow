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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/charset"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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
	// newRecord indicates whether we encounter a new record.
	newRecord bool
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
	c.formatValue(c.opType.String(), strBuilder)
	c.formatValue(c.tableName, strBuilder)
	c.formatValue(c.schemaName, strBuilder)
	if c.config.IncludeCommitTs {
		c.formatValue(c.commitTs, strBuilder)
	}
	for _, col := range c.columns {
		c.formatValue(col, strBuilder)
	}
	strBuilder.WriteString(c.config.Terminator)
	return []byte(strBuilder.String())
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

func fromCsvValToColValue(csvVal any, ft types.FieldType) (any, error) {
	str, ok := csvVal.(string)
	if !ok {
		return csvVal, nil
	}

	switch ft.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if ft.GetCharset() == charset.CharsetBin {
			blob, err := base64.StdEncoding.DecodeString(str)
			return blob, err
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
func fromColValToCsvVal(col *model.Column, ft *types.FieldType) (any, error) {
	if col.Value == nil {
		return nil, nil
	}

	switch col.Type {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if col.Flag.IsBinary() {
			if v, ok := col.Value.([]byte); ok {
				return base64.StdEncoding.EncodeToString(v), nil
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
	default:
		return col.Value, nil
	}
}

// rowChangedEvent2CSVMsg converts a RowChangedEvent to a csv record.
func rowChangedEvent2CSVMsg(csvConfig *common.Config, e *model.RowChangedEvent) (*csvMessage, error) {
	var err error

	csvMsg := &csvMessage{
		config:     csvConfig,
		tableName:  e.Table.Table,
		schemaName: e.Table.Schema,
		commitTs:   e.CommitTs,
		newRecord:  true,
	}
	if e.IsDelete() {
		csvMsg.opType = operationDelete
		csvMsg.columns, err = rowChangeColumns2CSVColumns(e.PreColumns, e.ColInfos)
		if err != nil {
			return nil, err
		}
	} else {
		if e.PreColumns == nil {
			csvMsg.opType = operationInsert
		} else {
			csvMsg.opType = operationUpdate
		}
		// for insert and update operation, we only record the after columns.
		csvMsg.columns, err = rowChangeColumns2CSVColumns(e.Columns, e.ColInfos)
		if err != nil {
			return nil, err
		}
	}
	return csvMsg, nil
}

func csvMsg2RowChangedEvent(csvMsg *csvMessage, ticols []*timodel.ColumnInfo) (*model.RowChangedEvent, error) {
	var err error
	if len(csvMsg.columns) != len(ticols) {
		return nil, cerror.WrapError(cerror.ErrCSVDecodeFailed,
			fmt.Errorf("the column length of csv message %d doesn't equal to that of tableInfo %d",
				len(csvMsg.columns), len(ticols)))
	}

	e := new(model.RowChangedEvent)
	e.CommitTs = csvMsg.commitTs
	e.Table = &model.TableName{
		Schema: csvMsg.schemaName,
		Table:  csvMsg.tableName,
	}
	if csvMsg.opType == operationDelete {
		e.PreColumns, err = csvColumns2RowChangeColumns(csvMsg.columns, ticols)
	} else {
		e.Columns, err = csvColumns2RowChangeColumns(csvMsg.columns, ticols)
	}

	if err != nil {
		return nil, err
	}

	return e, nil
}

func rowChangeColumns2CSVColumns(cols []*model.Column, colInfos []rowcodec.ColInfo) ([]any, error) {
	var csvColumns []any
	for i, column := range cols {
		// column could be nil in a condition described in
		// https://github.com/pingcap/tiflow/issues/6198#issuecomment-1191132951
		if column == nil {
			continue
		}

		converted, err := fromColValToCsvVal(column, colInfos[i].Ft)
		if err != nil {
			return nil, errors.Trace(err)
		}
		csvColumns = append(csvColumns, converted)
	}

	return csvColumns, nil
}

func csvColumns2RowChangeColumns(csvCols []any, ticols []*timodel.ColumnInfo) ([]*model.Column, error) {
	cols := make([]*model.Column, 0, len(csvCols))
	for idx, csvCol := range csvCols {
		col := new(model.Column)

		ticol := ticols[idx]
		col.Type = ticol.GetType()
		col.Charset = ticol.GetCharset()
		col.Name = ticol.Name.O
		if mysql.HasPriKeyFlag(ticol.GetFlag()) {
			col.Flag.SetIsHandleKey()
			col.Flag.SetIsPrimaryKey()
		}

		val, err := fromCsvValToColValue(csvCol, ticol.FieldType)
		if err != nil {
			return cols, err
		}
		col.Value = val
		cols = append(cols, col)
	}

	return cols, nil
}
