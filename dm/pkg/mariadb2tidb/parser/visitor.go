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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/utils"
	"go.uber.org/zap"
)

// Visitor interface for traversing AST nodes
type Visitor interface {
	// Enter is called when entering a node
	Enter(node ast.Node) (ast.Node, bool)
	// Leave is called when leaving a node
	Leave(node ast.Node) (ast.Node, bool)
}

// BaseVisitor provides a default implementation of Visitor
type BaseVisitor struct {
	logger *zap.Logger
}

// NewBaseVisitor creates a new base visitor
func NewBaseVisitor() *BaseVisitor {
	return &BaseVisitor{
		logger: utils.GetLogger(),
	}
}

// Enter implements Visitor interface
func (v *BaseVisitor) Enter(node ast.Node) (ast.Node, bool) {
	v.logger.Debug("Entering AST node", zap.String("type", node.Text()))
	return node, false
}

// Leave implements Visitor interface
func (v *BaseVisitor) Leave(node ast.Node) (ast.Node, bool) {
	v.logger.Debug("Leaving AST node", zap.String("type", node.Text()))
	return node, true
}

// Walker provides functionality to walk the AST with a visitor
type Walker struct {
	logger *zap.Logger
}

// NewWalker creates a new AST walker
func NewWalker() *Walker {
	return &Walker{
		logger: utils.GetLogger(),
	}
}

// Walk traverses the AST with the given visitor
func (w *Walker) Walk(stmts []ast.StmtNode, visitor Visitor) ([]ast.StmtNode, error) {
	w.logger.Debug("Walking AST", zap.Int("statements", len(stmts)))

	result := make([]ast.StmtNode, 0, len(stmts))

	for i, stmt := range stmts {
		w.logger.Debug("Walking statement", zap.Int("index", i), zap.String("type", stmt.Text()))

		// Use TiDB's Accept method to traverse the AST
		modifiedStmt, _ := stmt.Accept(visitor)

		if modifiedStmt != nil {
			if modifiedStmtNode, ok := modifiedStmt.(ast.StmtNode); ok {
				result = append(result, modifiedStmtNode)
			} else {
				w.logger.Warn("Visitor returned non-statement node", zap.Int("index", i))
				result = append(result, stmt)
			}
		} else {
			w.logger.Debug("Statement removed by visitor", zap.Int("index", i))
		}
	}

	w.logger.Info("AST walk completed", zap.Int("original", len(stmts)), zap.Int("result", len(result)))
	return result, nil
}
