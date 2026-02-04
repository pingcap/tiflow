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

package rules

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbtypes "github.com/pingcap/tidb/pkg/types"
)

// ZeroTimestampRule removes zero timestamp defaults that TiDB rejects in strict mode.
type ZeroTimestampRule struct{}

func (r *ZeroTimestampRule) Name() string { return "ZeroTimestamp" }

func (r *ZeroTimestampRule) Description() string {
	return "Remove zero timestamp defaults"
}

func (r *ZeroTimestampRule) Priority() int { return 70 }

func (r *ZeroTimestampRule) ShouldApply(node ast.Node) bool {
	col, ok := node.(*ast.ColumnDef)
	if !ok {
		return false
	}
	if !isTimeType(col.Tp.GetType()) {
		return false
	}
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue && isZeroTimeDefaultExpr(opt.Expr) {
			return true
		}
	}
	return false
}

func (r *ZeroTimestampRule) Apply(node ast.Node) (ast.Node, error) {
	col := node.(*ast.ColumnDef)
	opts := col.Options[:0]
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionDefaultValue && isZeroTimeDefaultExpr(opt.Expr) {
			continue
		}
		opts = append(opts, opt)
	}
	col.Options = opts
	return col, nil
}

func isTimeType(tp byte) bool {
	switch tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return true
	default:
		return false
	}
}

func isZeroTimeDefaultExpr(expr ast.ExprNode) bool {
	valExpr, ok := expr.(ast.ValueExpr)
	if !ok {
		return false
	}
	switch v := valExpr.GetValue().(type) {
	case tidbtypes.Time:
		return v.IsZero() || v.InvalidZero()
	case string:
		return isZeroTimeString(v)
	case []byte:
		return isZeroTimeString(string(v))
	case int64:
		return v == 0
	case uint64:
		return v == 0
	case int:
		return v == 0
	}
	return false
}

func isZeroTimeString(value string) bool {
	s := strings.TrimSpace(value)
	if !strings.HasPrefix(s, "0000-00-00") {
		return false
	}
	rest := strings.TrimPrefix(s, "0000-00-00")
	if rest == "" {
		return true
	}
	if strings.HasPrefix(rest, " ") {
		rest = strings.TrimPrefix(rest, " ")
	}
	if rest == "00:00:00" {
		return true
	}
	if strings.HasPrefix(rest, "00:00:00.") {
		for _, ch := range rest[len("00:00:00."):] {
			if ch != '0' {
				return false
			}
		}
		return true
	}
	return false
}
