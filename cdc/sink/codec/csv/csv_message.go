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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	insertOperation = "I"
	deleteOperation = "D"
	updateOperation = "U"
)

type csvMessage struct {
	csvConfig  *config.CSVConfig
	opType     string
	tableName  string
	schemaName string
	commitTs   uint64
	columns    []any
	newRecord  bool
}

func newCSVMessage(config *config.CSVConfig) *csvMessage {
	return &csvMessage{
		csvConfig: config,
		newRecord: true,
	}
}

func (c *csvMessage) encode() []byte {
	strBuilder := new(strings.Builder)
	c.formatValue(c.opType, strBuilder)
	c.formatValue(c.tableName, strBuilder)
	c.formatValue(c.schemaName, strBuilder)
	if c.csvConfig.IncludeCommitTs {
		c.formatValue(c.commitTs, strBuilder)
	}
	for _, col := range c.columns {
		c.formatValue(col, strBuilder)
	}
	strBuilder.WriteString(c.csvConfig.Terminator)
	return []byte(strBuilder.String())
}

func (c *csvMessage) formatWithQuotes(value string, strBuilder *strings.Builder) {
	quote := c.csvConfig.Quote

	strBuilder.WriteString(quote)
	strBuilder.WriteString(strings.ReplaceAll(value, quote, quote+quote))
	strBuilder.WriteString(quote)
}

func (c *csvMessage) formatWithEscapes(value string, strBuilder *strings.Builder) {
	lastPos := 0
	delimiter := c.csvConfig.Delimiter

	for i := 0; i < len(value); i++ {
		ch := value[i]
		isDelimiterStart := strings.HasPrefix(value[i:], delimiter)
		if ch == config.CR || ch == config.LF || ch == config.BackSlash || isDelimiterStart {
			// write out characters up until this position
			strBuilder.WriteString(value[lastPos:i])
			switch ch {
			case config.LF:
				ch = 'n'
			case config.CR:
				ch = 'r'
			}
			strBuilder.WriteRune(config.BackSlash)
			strBuilder.WriteRune(rune(ch))

			if isDelimiterStart {
				for k := 1; k < len(c.csvConfig.Delimiter); k++ {
					strBuilder.WriteRune(config.BackSlash)
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

func (c *csvMessage) formatValue(value any, strBuilder *strings.Builder) {
	defer func() {
		c.newRecord = false
	}()

	if !c.newRecord {
		strBuilder.WriteString(c.csvConfig.Delimiter)
	}

	if value == nil {
		strBuilder.WriteString(c.csvConfig.NullString)
		return
	}

	switch v := value.(type) {
	case string:
		if c.csvConfig.Quote != "" {
			c.formatWithQuotes(v, strBuilder)
		} else {
			c.formatWithEscapes(v, strBuilder)
		}
	default:
		strBuilder.WriteString(fmt.Sprintf("%v", v))
	}
}

func convertToCSVType(col *model.Column, ft *types.FieldType) (any, error) {
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
			return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
		}
		return enumVar.Name, nil
	case mysql.TypeSet:
		if v, ok := col.Value.(string); ok {
			return v, nil
		}
		setVar, err := types.ParseSetValue(ft.GetElems(), col.Value.(uint64))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
		}
		return setVar.Name, nil
	default:
		return col.Value, nil
	}
}

func buildRowData(csvConfig *config.CSVConfig, e *model.RowChangedEvent) ([]byte, error) {
	var cols []any

	csvMsg := &csvMessage{
		csvConfig:  csvConfig,
		tableName:  e.Table.Table,
		schemaName: e.Table.Schema,
		commitTs:   e.CommitTs,
		newRecord:  true,
	}
	colInfos := e.ColInfos
	if e.IsDelete() {
		csvMsg.opType = deleteOperation
		for i, column := range e.PreColumns {
			// TODO: check when would this condition happen
			if column == nil {
				continue
			}

			converted, err := convertToCSVType(column, colInfos[i].Ft)
			if err != nil {
				return nil, errors.Trace(err)
			}
			cols = append(cols, converted)
		}
		csvMsg.columns = cols
	} else {
		if e.PreColumns == nil {
			csvMsg.opType = insertOperation
		} else {
			csvMsg.opType = updateOperation
		}
		// for insert and update operation, we only record the after columns.
		for i, column := range e.Columns {
			if column == nil {
				continue
			}

			converted, err := convertToCSVType(column, colInfos[i].Ft)
			if err != nil {
				return nil, errors.Trace(err)
			}
			cols = append(cols, converted)
		}
		csvMsg.columns = cols
	}
	return csvMsg.encode(), nil
}
