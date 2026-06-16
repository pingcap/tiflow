// Copyright 2026 PingCAP, Inc.
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

package rewriter

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// WithStrictCollation enables collation-compatible=strict AST rewrite rules.
func WithStrictCollation(
	statusVars []byte,
	charsetAndDefaultCollation map[string]string,
	idAndCollationMap map[int]string,
	originSQL string,
	logger log.Logger,
) Option {
	return optionFunc(func(options *rewriteOptions) {
		options.rules = append(options.rules, strictCollationRule{
			statusVars:                 statusVars,
			charsetAndDefaultCollation: charsetAndDefaultCollation,
			idAndCollationMap:          idAndCollationMap,
			originSQL:                  originSQL,
			logger:                     logger,
		})
	})
}

// strictCollationRule adds explicit collations from upstream INFORMATION_SCHEMA.COLLATIONS.
type strictCollationRule struct {
	statusVars                 []byte
	charsetAndDefaultCollation map[string]string
	idAndCollationMap          map[int]string
	originSQL                  string
	logger                     log.Logger
}

func (r strictCollationRule) Apply(node ast.Node) (bool, error) {
	switch createStmt := node.(type) {
	case *ast.CreateTableStmt:
		return r.rewriteCreateTable(createStmt), nil
	case *ast.CreateDatabaseStmt:
		return r.rewriteCreateDatabase(createStmt), nil
	default:
		return false, nil
	}
}

func (r strictCollationRule) rewriteCreateTable(createStmt *ast.CreateTableStmt) bool {
	if createStmt.ReferTable != nil {
		return false
	}

	changed := r.rewriteColumnCollations(createStmt)
	var justCharset string
	for _, tableOption := range createStmt.Options {
		if tableOption.Tp == ast.TableOptionCollate {
			return changed
		}
		if tableOption.Tp == ast.TableOptionCharset {
			justCharset = tableOption.StrValue
		}
	}
	if justCharset == "" {
		r.warn("detect create table risk which use implicit charset and collation")
		return changed
	}

	collation, ok := r.charsetAndDefaultCollation[strings.ToLower(justCharset)]
	if !ok {
		r.warn("not found charset default collation.", zap.String("charset", strings.ToLower(justCharset)))
		return changed
	}
	r.info(
		"detect create table risk which use explicit charset and implicit collation, we will add collation by INFORMATION_SCHEMA.COLLATIONS",
		zap.String("collation", collation),
	)
	createStmt.Options = append(createStmt.Options, &ast.TableOption{Tp: ast.TableOptionCollate, StrValue: collation})
	return true
}

func (r strictCollationRule) rewriteCreateDatabase(createStmt *ast.CreateDatabaseStmt) bool {
	var justCharset string
	for _, createOption := range createStmt.Options {
		if createOption.Tp == ast.DatabaseOptionCollate {
			return false
		}
		if createOption.Tp == ast.DatabaseOptionCharset {
			justCharset = createOption.Value
		}
	}

	var collation string
	if justCharset != "" {
		var ok bool
		collation, ok = r.charsetAndDefaultCollation[strings.ToLower(justCharset)]
		if !ok {
			r.warn("not found charset default collation.", zap.String("charset", strings.ToLower(justCharset)))
			return false
		}
		r.info(
			"detect create database risk which use explicit charset and implicit collation, we will add collation by INFORMATION_SCHEMA.COLLATIONS",
			zap.String("collation", collation),
		)
	} else {
		var err error
		collation, err = event.GetServerCollationByStatusVars(r.statusVars, r.idAndCollationMap)
		if err != nil {
			r.error("can not get charset server collation from binlog statusVars.", zap.Error(err))
		}
		if collation == "" {
			r.error("get server collation from binlog statusVars is nil.", zap.Error(err))
			return false
		}
		r.info(
			"detect create database risk which use implicit charset and collation, we will add collation by binlog status_vars",
			zap.String("collation", collation),
		)
	}
	createStmt.Options = append(createStmt.Options, &ast.DatabaseOption{Tp: ast.DatabaseOptionCollate, Value: collation})
	return true
}

func (r strictCollationRule) rewriteColumnCollations(createStmt *ast.CreateTableStmt) bool {
	changed := false
ColumnLoop:
	for _, col := range createStmt.Cols {
		for _, options := range col.Options {
			if options.Tp == ast.ColumnOptionCollate {
				continue ColumnLoop
			}
		}
		fieldType := col.Tp
		if fieldType.GetCollate() != "" || fieldType.GetCharset() == "" {
			continue
		}

		collation, ok := r.charsetAndDefaultCollation[strings.ToLower(fieldType.GetCharset())]
		if !ok {
			r.warn(
				"not found charset default collation for column.",
				zap.String("table", createStmt.Table.Name.String()),
				zap.String("column", col.Name.String()),
				zap.String("charset", strings.ToLower(fieldType.GetCharset())),
			)
			continue
		}
		col.Options = append(col.Options, &ast.ColumnOption{Tp: ast.ColumnOptionCollate, StrValue: collation})
		changed = true
	}
	return changed
}

func (r strictCollationRule) info(msg string, fields ...zap.Field) {
	if r.logger.Logger == nil {
		return
	}
	r.logger.Info(msg, append([]zap.Field{zap.String("originSQL", r.originSQL)}, fields...)...)
}

func (r strictCollationRule) warn(msg string, fields ...zap.Field) {
	if r.logger.Logger == nil {
		return
	}
	r.logger.Warn(msg, append([]zap.Field{zap.String("originSQL", r.originSQL)}, fields...)...)
}

func (r strictCollationRule) error(msg string, fields ...zap.Field) {
	if r.logger.Logger == nil {
		return
	}
	r.logger.Error(msg, append(fields, zap.String("originSQL", r.originSQL))...)
}
