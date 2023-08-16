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

package sqlmodel

import (
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	timodel "github.com/pingcap/tidb/parser/model"
	timock "github.com/pingcap/tidb/util/mock"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func mockTableInfo(t *testing.T, sql string) *timodel.TableInfo {
	p := parser.New()
	se := timock.NewContext()
	node, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	dbChs, dbColl := charset.GetDefaultCharsetAndCollate()
	ti, err := ddl.BuildTableInfoWithStmt(se, node.(*ast.CreateTableStmt), dbChs, dbColl, nil)
	require.NoError(t, err)
	return ti
}

type dpanicSuite struct {
	suite.Suite
}

func (s *dpanicSuite) SetupSuite() {
	err := log.InitLogger(&log.Config{Level: "debug"})
	s.NoError(err)
}

func TestDpanicSuite(t *testing.T) {
	suite.Run(t, new(dpanicSuite))
}

func TestNewRowChange(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tbl"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tbl_routed"}
	sourceTI := mockTableInfo(t, "CREATE TABLE tbl (id INT PRIMARY KEY, name INT)")
	targetTI := mockTableInfo(t, "CREATE TABLE tbl_routed (id INT PRIMARY KEY, name INT)")
	tiSession := utils.NewSessionCtx(map[string]string{
		"time_zone": "+08:00",
	})

	expected := &RowChange{
		sourceTable:     source,
		targetTable:     target,
		preValues:       []interface{}{1, 2},
		postValues:      []interface{}{1, 3},
		sourceTableInfo: sourceTI,
		targetTableInfo: targetTI,
		tiSessionCtx:    tiSession,
		tp:              RowChangeUpdate,
		whereHandle:     nil,
	}

	actual := NewRowChange(source, target, []interface{}{1, 2}, []interface{}{1, 3}, sourceTI, targetTI, tiSession)
	require.Equal(t, expected, actual)

	actual.lazyInitWhereHandle()
	require.NotNil(t, actual.whereHandle)

	// test some arguments of NewRowChange can be nil

	expected.targetTable = expected.sourceTable
	expected.targetTableInfo = expected.sourceTableInfo
	expected.tiSessionCtx = utils.ZeroSessionCtx
	expected.whereHandle = nil
	actual = NewRowChange(source, nil, []interface{}{1, 2}, []interface{}{1, 3}, sourceTI, nil, nil)
	require.Equal(t, expected, actual)
}

func (s *dpanicSuite) TestRowChangeType() {
	change := &RowChange{preValues: []interface{}{1}}
	change.calculateType()
	s.Equal(RowChangeDelete, change.tp)
	change = &RowChange{preValues: []interface{}{1}, postValues: []interface{}{2}}
	change.calculateType()
	s.Equal(RowChangeUpdate, change.tp)
	change = &RowChange{postValues: []interface{}{1}}
	change.calculateType()
	s.Equal(RowChangeInsert, change.tp)

	s.Panics(func() {
		change = &RowChange{}
		change.calculateType()
	})
}

func (s *dpanicSuite) TestGenDelete() {
	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb2"}

	cases := []struct {
		sourceCreateSQL string
		targetCreateSQL string
		preValues       []interface{}

		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			"CREATE TABLE tb1 (id INT PRIMARY KEY, name INT)",
			"CREATE TABLE tb2 (id INT PRIMARY KEY, name INT, extra VARCHAR(20))",
			[]interface{}{1, 2},

			"DELETE FROM `db`.`tb2` WHERE `id` = ? LIMIT 1",
			[]interface{}{1},
		},
		{
			"CREATE TABLE tb1 (c INT, c2 INT UNIQUE)",
			"CREATE TABLE tb2 (c INT, c2 INT UNIQUE)",
			[]interface{}{1, 2},

			"DELETE FROM `db`.`tb2` WHERE `c2` = ? LIMIT 1",
			[]interface{}{2},
		},
		// next 2 cases test NULL value
		{
			"CREATE TABLE tb1 (c INT, c2 INT UNIQUE)",
			"CREATE TABLE tb2 (c INT, c2 INT UNIQUE)",
			[]interface{}{1, nil},

			"DELETE FROM `db`.`tb2` WHERE `c` = ? AND `c2` IS ? LIMIT 1",
			[]interface{}{1, nil},
		},
		{
			"CREATE TABLE tb1 (c INT, c2 INT)",
			"CREATE TABLE tb2 (c INT, c2 INT)",
			[]interface{}{1, nil},

			"DELETE FROM `db`.`tb2` WHERE `c` = ? AND `c2` IS ? LIMIT 1",
			[]interface{}{1, nil},
		},
		// next 2 cases test using downstream table to generate WHERE
		{
			"CREATE TABLE tb1 (id INT PRIMARY KEY, user_id INT NOT NULL UNIQUE)",
			"CREATE TABLE tb2 (new_id INT PRIMARY KEY, id INT, user_id INT NOT NULL UNIQUE)",
			[]interface{}{1, 2},

			"DELETE FROM `db`.`tb2` WHERE `user_id` = ? LIMIT 1",
			[]interface{}{2},
		},
		{
			"CREATE TABLE tb1 (id INT PRIMARY KEY, c2 INT)",
			"CREATE TABLE tb2 (new_id INT PRIMARY KEY, id INT, c2 INT)",
			[]interface{}{1, 2},

			"DELETE FROM `db`.`tb2` WHERE `id` = ? AND `c2` = ? LIMIT 1",
			[]interface{}{1, 2},
		},
	}

	for _, c := range cases {
		sourceTI := mockTableInfo(s.T(), c.sourceCreateSQL)
		targetTI := mockTableInfo(s.T(), c.targetCreateSQL)
		change := NewRowChange(source, target, c.preValues, nil, sourceTI, targetTI, nil)
		sql, args := change.GenSQL(DMLDelete)
		s.Equal(c.expectedSQL, sql)
		s.Equal(c.expectedArgs, args)
	}

	// a RowChangeUpdate can still generate DELETE SQL
	sourceTI := mockTableInfo(s.T(), "CREATE TABLE tb1 (id INT PRIMARY KEY, name INT)")
	change := NewRowChange(source, nil, []interface{}{1, 2}, []interface{}{3, 4}, sourceTI, nil, nil)
	sql, args := change.GenSQL(DMLDelete)
	s.Equal("DELETE FROM `db`.`tb1` WHERE `id` = ? LIMIT 1", sql)
	s.Equal([]interface{}{1}, args)

	change = NewRowChange(source, nil, nil, []interface{}{3, 4}, sourceTI, nil, nil)
	s.Panics(func() {
		change.GenSQL(DMLDelete)
	})
}

func (s *dpanicSuite) TestGenUpdate() {
	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb2"}

	cases := []struct {
		sourceCreateSQL string
		targetCreateSQL string
		preValues       []interface{}
		postValues      []interface{}

		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			"CREATE TABLE tb1 (id INT PRIMARY KEY, name INT)",
			"CREATE TABLE tb2 (id INT PRIMARY KEY, name INT, extra VARCHAR(20))",
			[]interface{}{1, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1",
			[]interface{}{3, 4, 1},
		},
		{
			"CREATE TABLE tb1 (id INT UNIQUE, name INT)",
			"CREATE TABLE tb2 (id INT UNIQUE, name INT)",
			[]interface{}{nil, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `id` = ?, `name` = ? WHERE `id` IS ? AND `name` = ? LIMIT 1",
			[]interface{}{3, 4, nil, 2},
		},
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)",
			"CREATE TABLE tb2 (c INT, c2 INT)",
			[]interface{}{1, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `c` = ?, `c2` = ? WHERE `c` = ? AND `c2` = ? LIMIT 1",
			[]interface{}{3, 4, 1, 2},
		},
		// next 2 cases test generated column
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			[]interface{}{1, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `c` = ? WHERE `c` = ? LIMIT 1",
			[]interface{}{3, 1},
		},
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT)",
			[]interface{}{1, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `c` = ?, `c2` = ? WHERE `c` = ? LIMIT 1",
			[]interface{}{3, 4, 1},
		},
	}

	for _, c := range cases {
		sourceTI := mockTableInfo(s.T(), c.sourceCreateSQL)
		targetTI := mockTableInfo(s.T(), c.targetCreateSQL)
		change := NewRowChange(source, target, c.preValues, c.postValues, sourceTI, targetTI, nil)
		sql, args := change.GenSQL(DMLUpdate)
		s.Equal(c.expectedSQL, sql)
		s.Equal(c.expectedArgs, args)
	}

	sourceTI := mockTableInfo(s.T(), "CREATE TABLE tb1 (id INT PRIMARY KEY, name INT)")
	change := NewRowChange(source, nil, nil, []interface{}{3, 4}, sourceTI, nil, nil)
	s.Panics(func() {
		change.GenSQL(DMLUpdate)
	})
}

func TestGenInsert(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb2"}

	cases := []struct {
		sourceCreateSQL string
		targetCreateSQL string
		postValues      []interface{}

		expectedInsertSQL      string
		expectedReplaceSQL     string
		expectedInsertOnDupSQL string
		expectedArgs           []interface{}
	}{
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT, extra VARCHAR(20))",
			[]interface{}{1, 2},

			"INSERT INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?)",
			"REPLACE INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?)",
			"INSERT INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?) ON DUPLICATE KEY UPDATE `c`=VALUES(`c`),`c2`=VALUES(`c2`)",
			[]interface{}{1, 2},
		},
		// next 2 cases test generated column
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			[]interface{}{1, 2},

			"INSERT INTO `db`.`tb2` (`c`) VALUES (?)",
			"REPLACE INTO `db`.`tb2` (`c`) VALUES (?)",
			"INSERT INTO `db`.`tb2` (`c`) VALUES (?) ON DUPLICATE KEY UPDATE `c`=VALUES(`c`)",
			[]interface{}{1},
		},
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT)",
			[]interface{}{1, 2},

			"INSERT INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?)",
			"REPLACE INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?)",
			"INSERT INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?) ON DUPLICATE KEY UPDATE `c`=VALUES(`c`),`c2`=VALUES(`c2`)",
			[]interface{}{1, 2},
		},
	}

	for _, c := range cases {
		sourceTI := mockTableInfo(t, c.sourceCreateSQL)
		targetTI := mockTableInfo(t, c.targetCreateSQL)
		change := NewRowChange(source, target, nil, c.postValues, sourceTI, targetTI, nil)
		sql, args := change.GenSQL(DMLInsert)
		require.Equal(t, c.expectedInsertSQL, sql)
		require.Equal(t, c.expectedArgs, args)
		sql, args = change.GenSQL(DMLReplace)
		require.Equal(t, c.expectedReplaceSQL, sql)
		require.Equal(t, c.expectedArgs, args)
		sql, args = change.GenSQL(DMLInsertOnDuplicateUpdate)
		require.Equal(t, c.expectedInsertOnDupSQL, sql)
		require.Equal(t, c.expectedArgs, args)
	}
}
