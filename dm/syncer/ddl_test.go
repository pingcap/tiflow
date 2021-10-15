// Copyright 2019 PingCAP, Inc.
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

package syncer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
	"github.com/pingcap/tidb/br/pkg/mock"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	parserpkg "github.com/pingcap/dm/pkg/parser"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
	onlineddl "github.com/pingcap/dm/syncer/online-ddl-tools"
)

var _ = Suite(&testDDLSuite{})

type testDDLSuite struct{}

func (s *testDDLSuite) newSubTaskCfg(dbCfg config.DBConfig) *config.SubTaskConfig {
	return &config.SubTaskConfig{
		From:             dbCfg,
		To:               dbCfg,
		ServerID:         101,
		MetaSchema:       "test",
		Name:             "syncer_ut",
		Mode:             config.ModeIncrement,
		Flavor:           "mysql",
		ShadowTableRules: []string{config.DefaultShadowTableRules},
		TrashTableRules:  []string{config.DefaultTrashTableRules},
	}
}

func (s *testDDLSuite) TestAnsiQuotes(c *C) {
	ansiQuotesCases := []string{
		"create database `test`",
		"create table `test`.`test`(id int)",
		"create table `test`.\"test\" (id int)",
		"create table \"test\".`test` (id int)",
		"create table \"test\".\"test\"",
		"create table test.test (\"id\" int)",
		"insert into test.test (\"id\") values('a')",
	}

	db, mock, err := sqlmock.New()
	mock.ExpectQuery("SHOW VARIABLES LIKE").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "ANSI_QUOTES"))
	c.Assert(err, IsNil)

	parser, err := utils.GetParser(context.Background(), db)
	c.Assert(err, IsNil)

	for _, sql := range ansiQuotesCases {
		_, err = parser.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
	}
}

func (s *testDDLSuite) TestDDLWithDashComments(c *C) {
	sql := `--
-- this is a comment.
--
CREATE TABLE test.test_table_with_c (id int);
`
	parser := parser.New()
	_, err := parserpkg.Parse(parser, sql, "", "")
	c.Assert(err, IsNil)
}

func (s *testDDLSuite) TestCommentQuote(c *C) {
	sql := "ALTER TABLE schemadb.ep_edu_course_message_auto_reply MODIFY answer JSON COMMENT '回复的内容-格式为list，有两个字段：\"answerType\"：//''发送客服消息类型：1-文本消息，2-图片，3-图文链接''；  answer：回复内容';"
	expectedSQL := "ALTER TABLE `schemadb`.`ep_edu_course_message_auto_reply` MODIFY COLUMN `answer` JSON COMMENT '回复的内容-格式为list，有两个字段：\"answerType\"：//''发送客服消息类型：1-文本消息，2-图片，3-图文链接''；  answer：回复内容'"

	parser := parser.New()

	stmt, err := parser.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)

	syncer := &Syncer{}
	tctx := tcontext.Background().WithLogger(log.With(zap.String("test", "TestCommentQuote")))
	ec := eventContext{
		tctx: tctx,
	}
	sqls, _, err := syncer.splitAndFilterDDL(ec, parser, stmt, "schemadb")
	c.Assert(err, IsNil)
	c.Assert(len(sqls), Equals, 1)
	c.Assert(sqls[0], Equals, expectedSQL)
}

func (s *testDDLSuite) TestResolveDDLSQL(c *C) {
	// duplicate with pkg/parser
	sqls := []string{
		"create schema `s1`",
		"create schema if not exists `s1`",
		"drop schema `s1`",
		"drop schema if exists `s1`",
		"drop table `s1`.`t1`",
		"drop table `s1`.`t1`, `s2`.`t2`",
		"drop table `s1`.`t1`, `s2`.`t2`, `xx`",
		"create table `s1`.`t1` (id int)",
		"create table `t1` (id int)",
		"create table `t1` like `t2`",
		"create table `s1`.`t1` like `t2`",
		"create table `s1`.`t1` like `s1`.`t2`",
		"create table `t1` like `xx`.`t2`",
		"truncate table `t1`",
		"truncate table `s1`.`t1`",
		"rename table `s1`.`t1` to `s2`.`t2`",
		"rename table `t1` to `t2`, `s1`.`t1` to `s1`.`t2`",
		"drop index i1 on `s1`.`t1`",
		"drop index i1 on `t1`",
		"create index i1 on `t1`(`c1`)",
		"create index i1 on `s1`.`t1`(`c1`)",
		"alter table `t1` add column c1 int, drop column c2",
		"alter table `s1`.`t1` add column c1 int, rename to `t2`, drop column c2",
		"alter table `s1`.`t1` add column c1 int, rename to `xx`.`t2`, drop column c2",
	}

	expectedSQLs := [][]string{
		{"CREATE DATABASE IF NOT EXISTS `s1`"},
		{"CREATE DATABASE IF NOT EXISTS `s1`"},
		{"DROP DATABASE IF EXISTS `s1`"},
		{"DROP DATABASE IF EXISTS `s1`"},
		{"DROP TABLE IF EXISTS `s1`.`t1`"},
		{"DROP TABLE IF EXISTS `s1`.`t1`"},
		{"DROP TABLE IF EXISTS `s1`.`t1`"},
		{"CREATE TABLE IF NOT EXISTS `s1`.`t1` (`id` INT)"},
		{},
		{},
		{},
		{"CREATE TABLE IF NOT EXISTS `s1`.`t1` LIKE `s1`.`t2`"},
		{},
		{},
		{"TRUNCATE TABLE `s1`.`t1`"},
		{},
		{"RENAME TABLE `s1`.`t1` TO `s1`.`t2`"},
		{"DROP INDEX IF EXISTS `i1` ON `s1`.`t1`"},
		{},
		{},
		{"CREATE INDEX `i1` ON `s1`.`t1` (`c1`)"},
		{},
		{"ALTER TABLE `s1`.`t1` ADD COLUMN `c1` INT"},
		{"ALTER TABLE `s1`.`t1` ADD COLUMN `c1` INT"},
	}

	targetSQLs := [][]string{
		{"CREATE DATABASE IF NOT EXISTS `xs1`"},
		{"CREATE DATABASE IF NOT EXISTS `xs1`"},
		{"DROP DATABASE IF EXISTS `xs1`"},
		{"DROP DATABASE IF EXISTS `xs1`"},
		{"DROP TABLE IF EXISTS `xs1`.`t1`"},
		{"DROP TABLE IF EXISTS `xs1`.`t1`"},
		{"DROP TABLE IF EXISTS `xs1`.`t1`"},
		{"CREATE TABLE IF NOT EXISTS `xs1`.`t1` (`id` INT)"},
		{},
		{},
		{},
		{"CREATE TABLE IF NOT EXISTS `xs1`.`t1` LIKE `xs1`.`t2`"},
		{},
		{},
		{"TRUNCATE TABLE `xs1`.`t1`"},
		{},
		{"RENAME TABLE `xs1`.`t1` TO `xs1`.`t2`"},
		{"DROP INDEX IF EXISTS `i1` ON `xs1`.`t1`"},
		{},
		{},
		{"CREATE INDEX `i1` ON `xs1`.`t1` (`c1`)"},
		{},
		{"ALTER TABLE `xs1`.`t1` ADD COLUMN `c1` INT"},
		{"ALTER TABLE `xs1`.`t1` ADD COLUMN `c1` INT"},
	}

	p := parser.New()
	cfg := &config.SubTaskConfig{
		BAList: &filter.Rules{
			DoDBs: []string{"s1"},
		},
	}
	var err error
	syncer := NewSyncer(cfg, nil)
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)

	syncer.tableRouter, err = router.NewTableRouter(false, []*router.TableRule{
		{
			SchemaPattern: "s1",
			TargetSchema:  "xs1",
		},
	})
	c.Assert(err, IsNil)

	tctx := tcontext.Background().WithLogger(log.With(zap.String("test", "TestResolveDDLSQL")))
	ec := eventContext{
		tctx: tctx,
	}

	for i, sql := range sqls {
		result, err := syncer.parseDDLSQL(sql, p, "test")
		c.Assert(err, IsNil)
		c.Assert(result.needSkip, IsFalse)
		c.Assert(result.isDDL, IsTrue)

		statements, _, err := syncer.splitAndFilterDDL(ec, p, result.stmt, "test")
		c.Assert(err, IsNil)
		c.Assert(statements, DeepEquals, expectedSQLs[i])
		c.Assert(targetSQLs[i], HasLen, len(statements))

		for j, statement := range statements {
			ddlInfo, err := syncer.routeDDL(p, "test", statement)
			c.Assert(err, IsNil)
			c.Assert(ddlInfo.sql, Equals, targetSQLs[i][j])
		}
	}
}

func (s *testDDLSuite) TestParseDDLSQL(c *C) {
	cases := []struct {
		sql      string
		schema   string
		needSkip bool
		isDDL    bool
		hasError bool
	}{
		{
			sql:      "FLUSH",
			schema:   "",
			needSkip: true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "BEGIN",
			schema:   "",
			needSkip: false,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "CREATE TABLE do_db.do_table (c1 INT)",
			schema:   "",
			needSkip: false,
			isDDL:    true,
			hasError: false,
		},
		{
			sql:      "INSERT INTO do_db.do_table VALUES (1)",
			schema:   "",
			needSkip: false,
			isDDL:    false,
			hasError: true,
		},
		{
			sql:      "INSERT INTO ignore_db.ignore_table VALUES (1)",
			schema:   "",
			needSkip: true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "UPDATE `ignore_db`.`ignore_table` SET c1=2 WHERE c1=1",
			schema:   "ignore_db",
			needSkip: true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "DELETE FROM `ignore_table` WHERE c1=2",
			schema:   "ignore_db",
			needSkip: true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "SELECT * FROM ignore_db.ignore_table",
			schema:   "",
			needSkip: false,
			isDDL:    false,
			hasError: true,
		},
		{
			sql:      "#",
			schema:   "",
			needSkip: false,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "# this is a comment",
			schema:   "",
			needSkip: false,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "# a comment with DDL\nCREATE TABLE do_db.do_table (c1 INT)",
			schema:   "",
			needSkip: false,
			isDDL:    true,
			hasError: false,
		},
		{
			sql:      "# a comment with DML\nUPDATE `ignore_db`.`ignore_table` SET c1=2 WHERE c1=1",
			schema:   "ignore_db",
			needSkip: true,
			isDDL:    false,
			hasError: false,
		},
		{
			sql:      "NOT A SQL",
			schema:   "",
			needSkip: false,
			isDDL:    false,
			hasError: true,
		},
	}

	cfg := &config.SubTaskConfig{
		BAList: &filter.Rules{
			IgnoreDBs: []string{"ignore_db"},
		},
	}
	var err error
	syncer := NewSyncer(cfg, nil)
	syncer.baList, err = filter.New(syncer.cfg.CaseSensitive, syncer.cfg.BAList)
	c.Assert(err, IsNil)

	parser := parser.New()

	for _, cs := range cases {
		pr, err := syncer.parseDDLSQL(cs.sql, parser, cs.schema)
		if cs.hasError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(pr.needSkip, Equals, cs.needSkip)
		c.Assert(pr.isDDL, Equals, cs.isDDL)
	}
}

func (s *testDDLSuite) TestResolveGeneratedColumnSQL(c *C) {
	testCases := []struct {
		sql      string
		expected string
	}{
		{
			"ALTER TABLE `test`.`test` ADD COLUMN d int(11) GENERATED ALWAYS AS (c + 1) VIRTUAL",
			"ALTER TABLE `test`.`test` ADD COLUMN `d` INT(11) GENERATED ALWAYS AS(`c`+1) VIRTUAL",
		},
		{
			"ALTER TABLE `test`.`test` ADD COLUMN d int(11) AS (1 + 1) STORED",
			"ALTER TABLE `test`.`test` ADD COLUMN `d` INT(11) GENERATED ALWAYS AS(1+1) STORED",
		},
	}

	syncer := &Syncer{}
	parser := parser.New()
	tctx := tcontext.Background().WithLogger(log.With(zap.String("test", "TestResolveGeneratedColumnSQL")))
	ec := eventContext{
		tctx: tctx,
	}

	for _, tc := range testCases {
		ast1, err := parser.ParseOneStmt(tc.sql, "", "")
		c.Assert(err, IsNil)

		sqls, _, err := syncer.splitAndFilterDDL(ec, parser, ast1, "test")
		c.Assert(err, IsNil)

		c.Assert(len(sqls), Equals, 1)
		getSQL := sqls[0]
		c.Assert(getSQL, Equals, tc.expected)
	}
}

func (s *testDDLSuite) TestResolveOnlineDDL(c *C) {
	cases := []struct {
		onlineType string
		trashName  string
		ghostname  string
	}{
		{
			config.GHOST,
			"_t1_del",
			"_t1_gho",
		},
		{
			config.PT,
			"_t1_old",
			"_t1_new",
		},
	}
	tctx := tcontext.Background().WithLogger(log.With(zap.String("test", "TestResolveOnlineDDL")))
	p := parser.New()

	ec := eventContext{tctx: tctx}
	cluster, err := mock.NewCluster()
	c.Assert(err, IsNil)
	c.Assert(cluster.Start(), IsNil)
	mysqlConfig, err := mysql.ParseDSN(cluster.DSN)
	c.Assert(err, IsNil)
	mockClusterPort, err := strconv.Atoi(strings.Split(mysqlConfig.Addr, ":")[1])
	c.Assert(err, IsNil)
	dbCfg := config.GetDBConfigForTest()
	dbCfg.Port = mockClusterPort
	dbCfg.Password = ""
	cfg := s.newSubTaskCfg(dbCfg)
	for _, ca := range cases {
		plugin, err := onlineddl.NewRealOnlinePlugin(tctx, cfg)
		c.Assert(err, IsNil)
		syncer := NewSyncer(cfg, nil)
		syncer.onlineDDL = plugin
		c.Assert(plugin.Clear(tctx), IsNil)
		// real table
		sql := "ALTER TABLE `test`.`t1` ADD COLUMN `n` INT"
		stmt, err := p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		sqls, tables, err := syncer.splitAndFilterDDL(ec, p, stmt, "test")
		c.Assert(err, IsNil)
		c.Assert(sqls, HasLen, 1)
		c.Assert(sqls[0], Equals, sql)
		c.Assert(tables, HasLen, 0)

		// trash table
		sql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `test`.`%s` (`n` INT)", ca.trashName)
		stmt, err = p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		sqls, tables, err = syncer.splitAndFilterDDL(ec, p, stmt, "test")
		c.Assert(err, IsNil)
		c.Assert(sqls, HasLen, 0)
		c.Assert(tables, HasLen, 0)

		// ghost table
		sql = fmt.Sprintf("ALTER TABLE `test`.`%s` ADD COLUMN `n` INT", ca.ghostname)
		newSQL := "ALTER TABLE `test`.`t1` ADD COLUMN `n` INT"
		stmt, err = p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		sqls, tables, err = syncer.splitAndFilterDDL(ec, p, stmt, "test")
		c.Assert(err, IsNil)
		c.Assert(sqls, HasLen, 0)
		c.Assert(tables, HasLen, 0)
		sql = fmt.Sprintf("RENAME TABLE `test`.`t1` TO `test`.`%s`, `test`.`%s` TO `test`.`t1`", ca.trashName, ca.ghostname)
		stmt, err = p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		sqls, tables, err = syncer.splitAndFilterDDL(ec, p, stmt, "test")
		c.Assert(err, IsNil)
		c.Assert(sqls, HasLen, 1)
		c.Assert(sqls[0], Equals, newSQL)
		tableName := &filter.Table{Schema: "test", Name: ca.ghostname}
		c.Assert(tables, DeepEquals, map[string]*filter.Table{tableName.String(): tableName})
	}
	cluster.Stop()
}

func (s *testDDLSuite) TestMistakeOnlineDDLRegex(c *C) {
	cases := []struct {
		onlineType string
		trashName  string
		ghostname  string
		matchGho   bool
	}{
		{
			config.GHOST,
			"_t1_del",
			"_t1_gho_invalid",
			false,
		},
		{
			config.GHOST,
			"_t1_del_invalid",
			"_t1_gho",
			true,
		},
		{
			config.PT,
			"_t1_old",
			"_t1_new_invalid",
			false,
		},
		{
			config.PT,
			"_t1_old_invalid",
			"_t1_new",
			true,
		},
	}
	tctx := tcontext.Background().WithLogger(log.With(zap.String("test", "TestMistakeOnlineDDLRegex")))
	p := parser.New()

	ec := eventContext{tctx: tctx}
	cluster, err := mock.NewCluster()
	c.Assert(err, IsNil)
	c.Assert(cluster.Start(), IsNil)
	mysqlConfig, err := mysql.ParseDSN(cluster.DSN)
	c.Assert(err, IsNil)
	mockClusterPort, err := strconv.Atoi(strings.Split(mysqlConfig.Addr, ":")[1])
	c.Assert(err, IsNil)
	dbCfg := config.GetDBConfigForTest()
	dbCfg.Port = mockClusterPort
	dbCfg.Password = ""
	cfg := s.newSubTaskCfg(dbCfg)
	for _, ca := range cases {
		plugin, err := onlineddl.NewRealOnlinePlugin(tctx, cfg)
		c.Assert(err, IsNil)
		syncer := NewSyncer(cfg, nil)
		syncer.onlineDDL = plugin
		c.Assert(plugin.Clear(tctx), IsNil)

		// ghost table
		sql := fmt.Sprintf("ALTER TABLE `test`.`%s` ADD COLUMN `n` INT", ca.ghostname)
		stmt, err := p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		sqls, tables, err := syncer.splitAndFilterDDL(ec, p, stmt, "test")
		c.Assert(err, IsNil)
		c.Assert(tables, HasLen, 0)
		table := ca.ghostname
		matchRules := config.ShadowTableRules
		if ca.matchGho {
			c.Assert(sqls, HasLen, 0)
			table = ca.trashName
			matchRules = config.TrashTableRules
		} else {
			c.Assert(sqls, HasLen, 1)
			c.Assert(sqls[0], Equals, sql)
		}
		sql = fmt.Sprintf("RENAME TABLE `test`.`t1` TO `test`.`%s`, `test`.`%s` TO `test`.`t1`", ca.trashName, ca.ghostname)
		stmt, err = p.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		sqls, tables, err = syncer.splitAndFilterDDL(ec, p, stmt, "test")
		c.Assert(terror.ErrConfigOnlineDDLMistakeRegex.Equal(err), IsTrue)
		c.Assert(sqls, HasLen, 0)
		c.Assert(tables, HasLen, 0)
		c.Assert(err, ErrorMatches, ".*"+sql+".*"+table+".*"+matchRules+".*")
	}
	cluster.Stop()
}

func (s *testDDLSuite) TestDropSchemaInSharding(c *C) {
	var (
		targetTable = &filter.Table{
			Schema: "target_db",
			Name:   "tbl",
		}
		sourceDB = "db1"
		source1  = "`db1`.`tbl1`"
		source2  = "`db1`.`tbl2`"
		tctx     = tcontext.Background()
	)
	dbCfg := config.GetDBConfigForTest()
	cfg := s.newSubTaskCfg(dbCfg)
	cfg.ShardMode = config.ShardPessimistic
	syncer := NewSyncer(cfg, nil)
	// nolint:dogsled
	_, _, _, _, err := syncer.sgk.AddGroup(targetTable, []string{source1}, nil, true)
	c.Assert(err, IsNil)
	// nolint:dogsled
	_, _, _, _, err = syncer.sgk.AddGroup(targetTable, []string{source2}, nil, true)
	c.Assert(err, IsNil)
	c.Assert(syncer.sgk.Groups(), HasLen, 2)
	c.Assert(syncer.dropSchemaInSharding(tctx, sourceDB), IsNil)
	c.Assert(syncer.sgk.Groups(), HasLen, 0)
}

func (s *testDDLSuite) TestClearOnlineDDL(c *C) {
	var (
		targetTable = &filter.Table{
			Schema: "target_db",
			Name:   "tbl",
		}
		source1 = "`db1`.`tbl1`"
		key1    = "db1tbl1"
		source2 = "`db1`.`tbl2`"
		key2    = "db1tbl2"
		tctx    = tcontext.Background()
	)
	dbCfg := config.GetDBConfigForTest()
	cfg := s.newSubTaskCfg(dbCfg)
	cfg.ShardMode = config.ShardPessimistic
	syncer := NewSyncer(cfg, nil)
	mock := mockOnlinePlugin{
		map[string]struct{}{key1: {}, key2: {}},
	}
	syncer.onlineDDL = mock

	// nolint:dogsled
	_, _, _, _, err := syncer.sgk.AddGroup(targetTable, []string{source1}, nil, true)
	c.Assert(err, IsNil)
	// nolint:dogsled
	_, _, _, _, err = syncer.sgk.AddGroup(targetTable, []string{source2}, nil, true)
	c.Assert(err, IsNil)

	c.Assert(syncer.clearOnlineDDL(tctx, targetTable), IsNil)
	c.Assert(mock.toFinish, HasLen, 0)
}

type mockOnlinePlugin struct {
	toFinish map[string]struct{}
}

func (m mockOnlinePlugin) Apply(tctx *tcontext.Context, tables []*filter.Table, statement string, stmt ast.StmtNode) ([]string, error) {
	return nil, nil
}

func (m mockOnlinePlugin) Finish(tctx *tcontext.Context, table *filter.Table) error {
	tableID := table.Schema + table.Name
	if _, ok := m.toFinish[tableID]; !ok {
		return errors.New("finish table not found")
	}
	delete(m.toFinish, tableID)
	return nil
}

func (m mockOnlinePlugin) TableType(table string) onlineddl.TableType {
	// 5 is _ _gho/ghc/del or _ _old/new
	if len(table) > 5 && strings.HasPrefix(table, "_") {
		if strings.HasSuffix(table, "_gho") || strings.HasSuffix(table, "_new") {
			return onlineddl.GhostTable
		}

		if strings.HasSuffix(table, "_ghc") || strings.HasSuffix(table, "_del") || strings.HasSuffix(table, "_old") {
			return onlineddl.TrashTable
		}
	}
	return onlineddl.RealTable
}

func (m mockOnlinePlugin) RealName(table string) string {
	return ""
}

func (m mockOnlinePlugin) ResetConn(tctx *tcontext.Context) error {
	return nil
}

func (m mockOnlinePlugin) Clear(tctx *tcontext.Context) error {
	return nil
}

func (m mockOnlinePlugin) Close() {
}

func (m mockOnlinePlugin) CheckAndUpdate(tctx *tcontext.Context, schemas map[string]string, tables map[string]map[string]string) error {
	return nil
}

func (m mockOnlinePlugin) CheckRegex(stmt ast.StmtNode, schema string, flavor utils.LowerCaseTableNamesFlavor) error {
	return nil
}
