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

package ddl

import (
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/format"
	// NOTE: Do not remove the `test_driver` import.
	// For details, refer to: https://github.com/pingcap/parser/issues/43
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// SplitQueries takes a string containing multiple SQL statements and splits them into individual SQL statements.
// This function is designed for scenarios like batch creation of tables, where multiple `CREATE TABLE` statements
// might be combined into a single query string.
func SplitQueries(queries string) ([]string, error) {
	// Note: The parser is not thread-safe, so we create a new instance of the parser for each use.
	// However, the overhead of creating a new parser is minimal, so there is no need to worry about performance.
	p := parser.New()
	stmts, warns, err := p.ParseSQL(queries)
	for _, w := range warns {
		log.Warn("parse sql warnning", zap.Error(w))
	}
	if err != nil {
		return nil, errors.WrapError(errors.ErrTiDBUnexpectedJobMeta, err)
	}

	var res []string
	for _, stmt := range stmts {
		var sb strings.Builder
		err := stmt.Restore(&format.RestoreCtx{
			Flags: format.DefaultRestoreFlags,
			In:    &sb,
		})
		if err != nil {
			return nil, errors.WrapError(errors.ErrTiDBUnexpectedJobMeta, err)
		}
		// The (ast.Node).Restore function generates a SQL string representation of the AST (Abstract Syntax Tree) node.
		// By default, the resulting SQL string does not include a trailing semicolon ";".
		// Therefore, we explicitly append a semicolon here to ensure the SQL statement is complete.
		sb.WriteByte(';')
		res = append(res, sb.String())
	}

	return res, nil
}
