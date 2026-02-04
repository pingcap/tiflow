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

package parser

import (
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/utils"
	"go.uber.org/zap"
)

// Writer handles writing AST back to formatted SQL
type Writer struct {
	logger *zap.Logger
}

// NewWriter creates a new SQL writer
func NewWriter() *Writer {
	return &Writer{
		logger: utils.GetLogger(),
	}
}

// WriteStatements writes multiple statements to formatted SQL
func (w *Writer) WriteStatements(stmts []ast.StmtNode) (string, error) {
	w.logger.Debug("Writing statements to SQL", zap.Int("count", len(stmts)))

	var result strings.Builder

	for i, stmt := range stmts {
		if i > 0 {
			result.WriteString("\n\n")
		}

		formatted, err := w.WriteStatement(stmt)
		if err != nil {
			w.logger.Error("Failed to write statement", zap.Int("index", i), zap.Error(err))
			return "", err
		}

		result.WriteString(formatted)
	}

	return result.String(), nil
}

// WriteStatement writes a single statement to formatted SQL
func (w *Writer) WriteStatement(stmt ast.StmtNode) (string, error) {
	w.logger.Debug("Writing statement to SQL", zap.String("type", stmt.Text()))

	var sb strings.Builder

	// Configure restore flags for readable output.
	flags := format.DefaultRestoreFlags |
		format.RestoreTiDBSpecialComment |
		format.RestoreStringWithoutDefaultCharset

	ctx := format.NewRestoreCtx(flags, &sb)

	err := stmt.Restore(ctx)
	if err != nil {
		w.logger.Error("Failed to restore statement", zap.Error(err))
		return "", err
	}

	result := sb.String()

	// Remove charset prefixes on empty string defaults
	result = regexp.MustCompile(`(?i)DEFAULT\s+_utf8mb4\s+''`).ReplaceAllString(result, "DEFAULT ''")

	// Ensure statement ends with semicolon
	if !strings.HasSuffix(result, ";") {
		result += ";"
	}

	return result, nil
}
