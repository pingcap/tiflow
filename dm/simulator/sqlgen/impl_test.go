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

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/simulator/config"
	"github.com/pingcap/tiflow/dm/simulator/mcp"
	"github.com/stretchr/testify/suite"
)

type testSQLGenImplSuite struct {
	suite.Suite
	tableConfig   *config.TableConfig
	sqlParser     *parser.Parser
	allColNameMap map[string]struct{}
}

func (s *testSQLGenImplSuite) SetupSuite() {
	s.allColNameMap = make(map[string]struct{})
	s.Require().Nil(log.InitLogger(&log.Config{}))
	s.tableConfig = &config.TableConfig{
		DatabaseName: "games",
		TableName:    "members",
		Columns: []*config.ColumnDefinition{
			{
				ColumnName: "id",
				DataType:   "int",
				DataLen:    11,
			},
			{
				ColumnName: "name",
				DataType:   "varchar",
				DataLen:    255,
			},
			{
				ColumnName: "age",
				DataType:   "int",
				DataLen:    11,
			},
			{
				ColumnName: "team_id",
				DataType:   "int",
				DataLen:    11,
			},
		},
		UniqueKeyColumnNames: []string{"id"},
	}
	for _, colInfo := range s.tableConfig.Columns {
		s.allColNameMap[fmt.Sprintf("`%s`", colInfo.ColumnName)] = struct{}{}
	}
	s.sqlParser = parser.New()
}

func generateUKColNameMap(ukColNames []string) map[string]struct{} {
	ukColNameMap := make(map[string]struct{})
	for _, colName := range ukColNames {
		ukColNameMap[fmt.Sprintf("`%s`", colName)] = struct{}{}
	}
	return ukColNameMap
}

func (s *testSQLGenImplSuite) checkLoadUKsSQL(sql string, ukColNames []string) {
	var err error
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Nilf(err, "parse statement error: %s", sql) {
		return
	}
	selectAST, ok := theAST.(*ast.SelectStmt)
	if !ok {
		s.Fail("cannot convert the AST to select AST")
		return
	}
	s.checkTableName(selectAST.From)
	if !s.Equal(len(s.tableConfig.UniqueKeyColumnNames), len(selectAST.Fields.Fields)) {
		return
	}
	ukColNameMap := generateUKColNameMap(ukColNames)
	for _, field := range selectAST.Fields.Fields {
		fieldNameStr, err := outputString(field)
		if !s.Nil(err) {
			continue
		}
		if _, ok := ukColNameMap[fieldNameStr]; !ok {
			s.Fail(
				"the parsed column name cannot be found in the UK names",
				"parsed column name: %s", fieldNameStr,
			)
		}
	}
}

func (s *testSQLGenImplSuite) checkTruncateSQL(sql string) {
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Nilf(err, "parse statement error: %s", sql) {
		return
	}
	truncateAST, ok := theAST.(*ast.TruncateTableStmt)
	if !ok {
		s.Fail("cannot convert the AST to truncate AST")
		return
	}
	s.checkTableName(truncateAST.Table)
}

func (s *testSQLGenImplSuite) checkInsertSQL(sql string) {
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Nilf(err, "parse statement error: %s", sql) {
		return
	}
	insertAST, ok := theAST.(*ast.InsertStmt)
	if !ok {
		s.Fail("cannot convert the AST to insert AST")
		return
	}
	s.checkTableName(insertAST.Table)
	if !s.Equal(len(s.tableConfig.Columns), len(insertAST.Columns)) {
		return
	}
	for _, col := range insertAST.Columns {
		colNameStr, err := outputString(col)
		if !s.Nil(err) {
			continue
		}
		if _, ok := s.allColNameMap[colNameStr]; !ok {
			s.Fail(
				"the parsed column name cannot be found in the column names",
				"parsed column name: %s", colNameStr,
			)
		}
	}
}

func (s *testSQLGenImplSuite) checkUpdateSQL(sql string, ukColNames []string) {
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Nilf(err, "parse statement error: %s", sql) {
		return
	}
	updateAST, ok := theAST.(*ast.UpdateStmt)
	if !ok {
		s.Fail("cannot convert the AST to update AST")
		return
	}
	s.checkTableName(updateAST.TableRefs)
	s.Greater(len(updateAST.List), 0)
	s.checkWhereClause(updateAST.Where, ukColNames)
}

func (s *testSQLGenImplSuite) checkDeleteSQL(sql string, ukColNames []string) {
	theAST, err := s.sqlParser.ParseOneStmt(sql, "", "")
	if !s.Nilf(err, "parse statement error: %s", sql) {
		return
	}
	deleteAST, ok := theAST.(*ast.DeleteStmt)
	if !ok {
		s.Fail("cannot convert the AST to delete AST")
		return
	}
	s.checkTableName(deleteAST.TableRefs)
	s.checkWhereClause(deleteAST.Where, ukColNames)
}

func (s *testSQLGenImplSuite) checkTableName(astNode ast.Node) {
	tableNameStr, err := outputString(astNode)
	if !s.Nil(err) {
		return
	}
	s.Equal(
		fmt.Sprintf("`%s`.`%s`", s.tableConfig.DatabaseName, s.tableConfig.TableName),
		tableNameStr,
	)
}

func (s *testSQLGenImplSuite) checkWhereClause(astNode ast.Node, ukColNames []string) {
	whereClauseStr, err := outputString(astNode)
	if !s.Nil(err) {
		return
	}
	ukColNameMap := generateUKColNameMap(ukColNames)
	for colName := range ukColNameMap {
		if !s.Truef(
			strings.Contains(whereClauseStr, fmt.Sprintf("%s=", colName)) ||
				strings.Contains(whereClauseStr, fmt.Sprintf("%s IS NULL", colName)),
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
	s.Nil(err, "generate load UK SQL error")
	s.T().Logf("Generated SELECT SQL: %s\n", sql)
	s.checkLoadUKsSQL(sql, s.tableConfig.UniqueKeyColumnNames)

	sql, err = g.GenTruncateTable()
	s.Nil(err, "generate truncate table SQL error")
	s.T().Logf("Generated Truncate Table SQL: %s\n", sql)
	s.checkTruncateSQL(sql)

	theMCP := mcp.NewModificationCandidatePool(8192)
	for i := 0; i < 4096; i++ {
		s.Nil(
			theMCP.AddUK(mcp.NewUniqueKey(i, map[string]interface{}{
				"id": i,
			})),
		)
	}
	for i := 0; i < 10; i++ {
		uk = theMCP.NextUK()
		sql, err = g.GenUpdateRow(uk)
		s.Nil(err, "generate update sql error")
		s.T().Logf("Generated SQL: %s\n", sql)
		s.checkUpdateSQL(sql, s.tableConfig.UniqueKeyColumnNames)

		sql, uk, err = g.GenInsertRow()
		s.Nil(err, "generate insert sql error")
		s.T().Logf("Generated SQL: %s\n; Unique key: %v\n", sql, uk)
		s.checkInsertSQL(sql)

		uk = theMCP.NextUK()
		sql, err = g.GenDeleteRow(uk)
		s.Nil(err, "generate delete sql error")
		s.T().Logf("Generated SQL: %s\n; Unique key: %v\n", sql, uk)
		s.checkDeleteSQL(sql, s.tableConfig.UniqueKeyColumnNames)
	}
}

func (s *testSQLGenImplSuite) TestWhereNULL() {
	var (
		err error
		sql string
	)
	theTableConfig := &config.TableConfig{
		DatabaseName:         s.tableConfig.DatabaseName,
		TableName:            s.tableConfig.TableName,
		Columns:              s.tableConfig.Columns,
		UniqueKeyColumnNames: []string{"name", "team_id"},
	}
	g := NewSQLGeneratorImpl(theTableConfig)
	theUK := mcp.NewUniqueKey(-1, map[string]interface{}{
		"name":    "ABCDEFG",
		"team_id": nil,
	})
	sql, err = g.GenUpdateRow(theUK)
	s.Require().Nil(err)
	s.T().Logf("Generated UPDATE SQL: %s\n", sql)
	s.checkUpdateSQL(sql, theTableConfig.UniqueKeyColumnNames)

	sql, err = g.GenDeleteRow(theUK)
	s.Require().Nil(err)
	s.T().Logf("Generated DELETE SQL: %s\n", sql)
	s.checkDeleteSQL(sql, theTableConfig.UniqueKeyColumnNames)
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
	s.NotNil(err)
	_, err = g.GenDeleteRow(uk)
	s.NotNil(err)

	uk = mcp.NewUniqueKey(-1, map[string]interface{}{
		"id":      123,
		"abcdefg": 321,
	})
	sql, err = g.GenUpdateRow(uk)
	s.Nil(err)
	s.T().Logf("Generated SQL: %s\n", sql)
	s.checkUpdateSQL(sql, s.tableConfig.UniqueKeyColumnNames)

	sql, err = g.GenDeleteRow(uk)
	s.Nil(err)
	s.T().Logf("Generated SQL: %s\n", sql)
	s.checkDeleteSQL(sql, s.tableConfig.UniqueKeyColumnNames)

	uk = mcp.NewUniqueKey(-1, map[string]interface{}{})
	_, err = g.GenUpdateRow(uk)
	s.NotNil(err)
}

func (s *testSQLGenImplSuite) TestDMLWithNoUK() {
	var (
		err   error
		sql   string
		theUK *mcp.UniqueKey
	)
	theTableConfig := &config.TableConfig{
		DatabaseName:         s.tableConfig.DatabaseName,
		TableName:            s.tableConfig.TableName,
		Columns:              s.tableConfig.Columns,
		UniqueKeyColumnNames: []string{},
	}
	g := NewSQLGeneratorImpl(theTableConfig)

	sql, theUK, err = g.GenInsertRow()
	s.Nil(err, "generate insert sql error")
	s.T().Logf("Generated SQL: %s\n; Unique key: %v\n", sql, theUK)
	s.checkInsertSQL(sql)

	theUK = mcp.NewUniqueKey(-1, map[string]interface{}{})
	_, err = g.GenUpdateRow(theUK)
	s.NotNil(err)
	_, err = g.GenDeleteRow(theUK)
	s.NotNil(err)

	theUK = mcp.NewUniqueKey(-1, map[string]interface{}{
		"id": 123, // the column is filtered out by the UK configs
	})
	_, err = g.GenUpdateRow(theUK)
	s.NotNil(err)
	_, err = g.GenDeleteRow(theUK)
	s.NotNil(err)
}

func TestSQLGenImplSuite(t *testing.T) {
	suite.Run(t, &testSQLGenImplSuite{})
}
