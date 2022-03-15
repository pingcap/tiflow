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

package sqlgen

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/simulator/internal/config"
	"github.com/pingcap/tiflow/dm/simulator/internal/mcp"
)

type testSQLGenImplSuite struct {
	suite.Suite
	tableConfig   *config.TableConfig
	sqlParser     *parser.Parser
	ukColNameMap  map[string]struct{}
	allColNameMap map[string]struct{}
}

func (s *testSQLGenImplSuite) SetupSuite() {
	s.ukColNameMap = make(map[string]struct{})
	s.allColNameMap = make(map[string]struct{})
	s.Require().Nil(log.InitLogger(&log.Config{}))
	s.tableConfig = &config.TableConfig{
		DatabaseName: "games",
		TableName:    "members",
		Columns: []*config.ColumnDefinition{
			&config.ColumnDefinition{
				ColumnName: "id",
				DataType:   "int",
				DataLen:    11,
			},
			&config.ColumnDefinition{
				ColumnName: "name",
				DataType:   "varchar",
				DataLen:    255,
			},
			&config.ColumnDefinition{
				ColumnName: "age",
				DataType:   "int",
				DataLen:    11,
			},
			&config.ColumnDefinition{
				ColumnName: "team_id",
				DataType:   "int",
				DataLen:    11,
			},
		},
		UniqueKeyColumnNames: []string{"id"},
	}
	for _, colName := range s.tableConfig.UniqueKeyColumnNames {
		s.ukColNameMap[fmt.Sprintf("`%s`", colName)] = struct{}{}
	}
	for _, colInfo := range s.tableConfig.Columns {
		s.allColNameMap[fmt.Sprintf("`%s`", colInfo.ColumnName)] = struct{}{}
	}
	s.sqlParser = parser.New()
}

func (s *testSQLGenImplSuite) checkLoadUKsSQL(sql string) {
	var err error
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Assert().Nilf(err, "parse statement error: %s", sql) {
		return
	}
	selectAST, ok := theAST.(*ast.SelectStmt)
	if !ok {
		s.Assert().Fail("cannot convert the AST to select AST")
		return
	}
	s.checkTableName(selectAST.From)
	if !s.Assert().Equal(len(s.tableConfig.UniqueKeyColumnNames), len(selectAST.Fields.Fields)) {
		return
	}
	for _, field := range selectAST.Fields.Fields {
		fieldNameStr, err := outputString(field)
		if !s.Assert().Nil(err) {
			continue
		}
		if _, ok := s.ukColNameMap[fieldNameStr]; !ok {
			s.Assert().Fail(
				"the parsed column name cannot be found in the UK names",
				"parsed column name: %s", fieldNameStr,
			)
		}
	}
}

func (s *testSQLGenImplSuite) checkTruncateSQL(sql string) {
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Assert().Nilf(err, "parse statement error: %s", sql) {
		return
	}
	truncateAST, ok := theAST.(*ast.TruncateTableStmt)
	if !ok {
		s.Assert().Fail("cannot convert the AST to truncate AST")
		return
	}
	s.checkTableName(truncateAST.Table)
}

func (s *testSQLGenImplSuite) checkInsertSQL(sql string) {
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Assert().Nilf(err, "parse statement error: %s", sql) {
		return
	}
	insertAST, ok := theAST.(*ast.InsertStmt)
	if !ok {
		s.Assert().Fail("cannot convert the AST to insert AST")
		return
	}
	s.checkTableName(insertAST.Table)
	if !s.Assert().Equal(len(s.tableConfig.Columns), len(insertAST.Columns)) {
		return
	}
	for _, col := range insertAST.Columns {
		colNameStr, err := outputString(col)
		if !s.Assert().Nil(err) {
			continue
		}
		if _, ok := s.allColNameMap[colNameStr]; !ok {
			s.Assert().Fail(
				"the parsed column name cannot be found in the column names",
				"parsed column name: %s", colNameStr,
			)
		}
	}
}

func (s *testSQLGenImplSuite) checkUpdateSQL(sql string) {
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Assert().Nilf(err, "parse statement error: %s", sql) {
		return
	}
	updateAST, ok := theAST.(*ast.UpdateStmt)
	if !ok {
		s.Assert().Fail("cannot convert the AST to update AST")
		return
	}
	s.checkTableName(updateAST.TableRefs)
	s.Assert().Greater(len(updateAST.List), 0)
	s.checkWhereClause(updateAST.Where)
}

func (s *testSQLGenImplSuite) checkDeleteSQL(sql string) {
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Assert().Nilf(err, "parse statement error: %s", sql) {
		return
	}
	deleteAST, ok := theAST.(*ast.DeleteStmt)
	if !ok {
		s.Assert().Fail("cannot convert the AST to delete AST")
		return
	}
	s.checkTableName(deleteAST.TableRefs)
	s.checkWhereClause(deleteAST.Where)
}

func (s *testSQLGenImplSuite) checkTableName(astNode ast.Node) {
	tableNameStr, err := outputString(astNode)
	if !s.Assert().Nil(err) {
		return
	}
	s.Assert().Equal(
		fmt.Sprintf("`%s`.`%s`", s.tableConfig.DatabaseName, s.tableConfig.TableName),
		tableNameStr,
	)
}

func (s *testSQLGenImplSuite) checkWhereClause(astNode ast.Node) {
	whereClauseStr, err := outputString(astNode)
	if !s.Assert().Nil(err) {
		return
	}
	for colName := range s.ukColNameMap {
		if !s.Assert().Truef(
			strings.Contains(whereClauseStr, fmt.Sprintf("%s=", colName)),
			"cannot find the column name in the where clause: where clause string: %s; column name: %s",
			whereClauseStr, colName,
		) {
			continue
		}
	}
}

func (s *testSQLGenImplSuite) TestDMLBasic() {
	var (
		err error
		sql string
		uk  *mcp.UniqueKey
	)
	g := NewSQLGeneratorImpl(s.tableConfig)

	sql, _, err = g.GenLoadUniqueKeySQL()
	s.Assert().Nil(err, "generate load UK SQL error")
	s.T().Logf("Generated SELECT SQL: %s\n", sql)
	s.checkLoadUKsSQL(sql)

	sql, err = g.GenTruncateTable()
	s.Assert().Nil(err, "generate truncate table SQL error")
	s.T().Logf("Generated Truncate Table SQL: %s\n", sql)
	s.checkTruncateSQL(sql)

	theMCP := mcp.NewModificationCandidatePool(8192)
	for i := 0; i < 4096; i++ {
		s.Assert().Nil(
			theMCP.AddUK(mcp.NewUniqueKey(i, map[string]interface{}{
				"id": i,
			})),
		)
	}
	for i := 0; i < 10; i++ {
		uk = theMCP.NextUK()
		sql, err = g.GenUpdateRow(uk)
		s.Assert().Nil(err, "generate update sql error")
		s.T().Logf("Generated SQL: %s\n", sql)
		s.checkUpdateSQL(sql)

		sql, uk, err = g.GenInsertRow()
		s.Assert().Nil(err, "generate insert sql error")
		s.T().Logf("Generated SQL: %s\n; Unique key: %v\n", sql, uk)
		s.checkInsertSQL(sql)

		uk = theMCP.NextUK()
		sql, err = g.GenDeleteRow(uk)
		s.Assert().Nil(err, "generate delete sql error")
		s.T().Logf("Generated SQL: %s\n; Unique key: %v\n", sql, uk)
		s.checkDeleteSQL(sql)
	}
}

func (s *testSQLGenImplSuite) TestDMLAbnormalUK() {
	var (
		sql string
		err error
		uk  *mcp.UniqueKey
	)
	g := NewSQLGeneratorImpl(s.tableConfig)
	uk = mcp.NewUniqueKey(-1, map[string]interface{}{
		"abcdefg": 123,
	})
	_, err = g.GenUpdateRow(uk)
	s.Assert().NotNil(err)
	_, err = g.GenDeleteRow(uk)
	s.Assert().NotNil(err)

	uk = mcp.NewUniqueKey(-1, map[string]interface{}{
		"id":      123,
		"abcdefg": 321,
	})
	sql, err = g.GenUpdateRow(uk)
	s.Assert().Nil(err)
	s.T().Logf("Generated SQL: %s\n", sql)
	s.checkUpdateSQL(sql)

	sql, err = g.GenDeleteRow(uk)
	s.Assert().Nil(err)
	s.T().Logf("Generated SQL: %s\n", sql)
	s.checkDeleteSQL(sql)
}

func TestSQLGenImplSuite(t *testing.T) {
	suite.Run(t, &testSQLGenImplSuite{})
}
