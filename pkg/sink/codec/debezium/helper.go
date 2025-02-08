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
	"encoding/binary"
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

<<<<<<< HEAD
=======
type visiter struct {
	columnsMap map[pmodel.CIStr]*timodel.ColumnInfo
}

func (v *visiter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	return n, false
}

func (v *visiter) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch col := n.(type) {
	case *ast.ColumnDef:
		c := v.columnsMap[col.Name.Name]
		if col.Tp != nil {
			parseType(c, col)
		}
		c.Comment = "" // disable comment
	}
	return n, true
}

func extractValue(expr ast.ExprNode) any {
	switch v := expr.(type) {
	case *driver.ValueExpr:
		return fmt.Sprintf("%v", v.GetValue())
	case *ast.FuncCallExpr:
		return v.FnName.String()
	}
	return nil
}

func parseType(c *timodel.ColumnInfo, col *ast.ColumnDef) {
	ft := col.Tp
	switch ft.GetType() {
	case mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp, mysql.TypeYear:
		if ft.GetType() == mysql.TypeYear {
			c.SetFlen(ft.GetFlen())
		} else {
			c.SetDecimal(ft.GetDecimal())
		}
		parseOptions(col.Options, c)
	default:
	}
}

func parseOptions(options []*ast.ColumnOption, c *timodel.ColumnInfo) {
	for _, option := range options {
		switch option.Tp {
		case ast.ColumnOptionDefaultValue:
			defaultValue := extractValue(option.Expr)
			if defaultValue == nil {
				continue
			}
			if err := c.SetDefaultValue(defaultValue); err != nil {
				log.Error("failed to set default value")
			}
		}
	}
}

func parseColumns(sql string, columns []*timodel.ColumnInfo) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, mysql.DefaultCharset, mysql.DefaultCollationName)
	if err != nil {
		log.Error("format query parse one stmt failed", zap.Error(err))
	}

	columnsMap := make(map[pmodel.CIStr]*timodel.ColumnInfo, len(columns))
	for _, col := range columns {
		columnsMap[col.Name] = col
	}
	stmt.Accept(&visiter{columnsMap: columnsMap})
}

>>>>>>> 600286c56d (sink(ticdc): fix incorrect `default` field (#12038))
func parseBit(s string, n int) string {
	var result string
	if len(s) > 0 {
		// Leading zeros may be omitted
		result = fmt.Sprintf("%0*b", n%8, s[0])
	}
	for i := 1; i < len(s); i++ {
		result += fmt.Sprintf("%08b", s[i])
	}
	return result
}

func getBitFromUint64(n int, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	numBytes := n / 8
	if n%8 != 0 {
		numBytes += 1
	}
	return buf[:numBytes]
}

<<<<<<< HEAD
func getValue(col model.ColumnDataX) any {
	if col.Value == nil {
		return col.GetDefaultValue()
	}
	return col.Value
=======
func getDBTableName(e *model.DDLEvent) (string, string) {
	if e.TableInfo == nil {
		return "", ""
	}
	return e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName()
>>>>>>> 600286c56d (sink(ticdc): fix incorrect `default` field (#12038))
}

func getSchemaTopicName(namespace string, schema string, table string) string {
	return fmt.Sprintf("%s.%s.%s",
		common.SanitizeName(namespace),
		common.SanitizeName(schema),
		common.SanitizeTopicName(table))
}
