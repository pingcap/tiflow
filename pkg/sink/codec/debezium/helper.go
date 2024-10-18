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
	"fmt"
	"strings"

	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"go.uber.org/zap"
)

type visiter struct {
	columnsMap map[model.CIStr]*timodel.ColumnInfo
}

func (v *visiter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch col := n.(type) {
	case *ast.ColumnDef:
		c := v.columnsMap[col.Name.Name]
		if col.Options != nil {
			parseOptions(col.Options, c)
		}
		if col.Tp.GetCharset() != "" {
			c.SetCharset(col.Tp.GetCharset())
		}
		parseType(c, col)
		c.Comment = ""
	}
	return n, false
}

func (v *visiter) Leave(n ast.Node) (node ast.Node, ok bool) {
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
	case mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp:
		c.SetDecimal(ft.GetDecimal())
		if c.OriginDefaultValue != nil {
			c.SetDefaultValue(c.OriginDefaultValue)
		}
	case mysql.TypeYear:
		c.SetFlen(ft.GetFlen())
		if c.OriginDefaultValue != nil {
			c.SetDefaultValue(c.OriginDefaultValue)
		}
	default:
	}
}

func parseOptions(options []*ast.ColumnOption, c *timodel.ColumnInfo) {
	for _, option := range options {
		switch option.Tp {
		case ast.ColumnOptionDefaultValue, ast.ColumnOptionComment:
			defaultValue := extractValue(option.Expr)
			if defaultValue == nil {
				continue
			}
			if option.Tp == ast.ColumnOptionDefaultValue {
				if err := c.SetOriginDefaultValue(defaultValue); err != nil {
					log.Error("failed to set default value")
				}
			}
		case ast.ColumnOptionCollate:
			c.SetCollate(option.StrValue)
		}
	}
}

func getColumns(sql string, columns []*timodel.ColumnInfo) []*timodel.ColumnInfo {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		log.Error("format query parse one stmt failed", zap.Error(err))
	}

	columnsMap := make(map[model.CIStr]*timodel.ColumnInfo, len(columns))
	for _, col := range columns {
		columnsMap[col.Name] = col
	}
	stmt.Accept(&visiter{columnsMap: columnsMap})
	return columns
}

func getCharset(ft types.FieldType) string {
	if ft.GetCharset() == "binary" {
		return ""
	}
	switch ft.GetType() {
	case mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeNewDecimal, mysql.TypeString, mysql.TypeVarchar,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeEnum, mysql.TypeSet:
		if ft.GetCollate() == "utf8_unicode_ci" {
			return charset.CharsetUTF8MB4
		}
		return ft.GetCharset()
	}
	return ""
}

func getLen(ft types.FieldType) int {
	decimal := ft.GetDecimal()
	flen := ft.GetFlen()
	switch ft.GetType() {
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		return decimal
	case mysql.TypeBit, mysql.TypeVarchar, mysql.TypeString, mysql.TypeNewDecimal, mysql.TypeSet,
		mysql.TypeVarString, mysql.TypeTiDBVectorFloat32, mysql.TypeTiny, mysql.TypeYear, mysql.TypeShort:
		return flen
	case mysql.TypeEnum:
		return 1
	// case mysql.TypeLonglong, mysql.TypeInt24:
	// 	if mysql.HasNotNullFlag(ft.GetFlag()) {
	// 		return -1
	// 	}
	// 	return flen
	case mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		defaultFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
		if flen != defaultFlen {
			return flen
		}
	}
	return -1
}

func getScale(ft types.FieldType) float64 {
	switch ft.GetType() {
	case mysql.TypeNewDecimal:
		return float64(ft.GetDecimal())
	}
	return -1
}

func getSuffix(ft types.FieldType) string {
	suffix := ""
	decimal := ft.GetDecimal()
	flen := ft.GetFlen()
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.GetType())
	isDecimalNotDefault := decimal != defaultDecimal && decimal != 0 && decimal != -1

	// displayFlen and displayDecimal are flen and decimal values with `-1` substituted with default value.
	displayFlen, displayDecimal := flen, decimal
	if displayFlen == -1 {
		displayFlen = defaultFlen
	}
	if displayDecimal == -1 {
		displayDecimal = defaultDecimal
	}

	switch ft.GetType() {
	case mysql.TypeDouble, mysql.TypeFloat:
		// 1. flen Not Default, decimal Not Default -> Valid
		// 2. flen Not Default, decimal Default (-1) -> Invalid
		// 3. flen Default, decimal Not Default -> Valid
		// 4. flen Default, decimal Default -> Valid (hide)W
		if isDecimalNotDefault {
			suffix = fmt.Sprintf("(%d,%d)", displayFlen, displayDecimal)
		}
	case mysql.TypeNewDecimal:
		suffix = fmt.Sprintf("(%d,%d)", displayFlen, displayDecimal)
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		if !mysql.HasBinaryFlag(ft.GetFlag()) {
			suffix = fmt.Sprintf("(%d)", displayFlen)
		}
	case mysql.TypeYear:
		suffix = fmt.Sprintf("(%d)", flen)
	case mysql.TypeTiDBVectorFloat32:
		if flen != -1 {
			suffix = fmt.Sprintf("(%d)", flen)
		}
	case mysql.TypeNull:
		suffix = "(0)"
	}
	return suffix
}

func getExpressionAndName(ft types.FieldType) (string, string) {
	prefix := strings.ToUpper(types.TypeToStr(ft.GetType(), ft.GetCharset()))
	ft.CompactStr()
	switch ft.GetType() {
	case mysql.TypeYear, mysql.TypeBit, mysql.TypeVarchar, mysql.TypeNewDecimal:
		return prefix, prefix
	}
	cs := prefix + getSuffix(ft)
	suf := ""
	flag := ft.GetFlag()

	if mysql.HasZerofillFlag(flag) {
		suf = " UNSIGNED ZEROFILL"
	} else if mysql.HasUnsignedFlag(flag) {
		suf = " UNSIGNED"
	}
	return cs + suf, prefix + suf
}
