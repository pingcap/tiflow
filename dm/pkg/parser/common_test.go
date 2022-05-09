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

package parser

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/filter"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var _ = Suite(&testParserSuite{})

type testCase struct {
	sql                string
	expectedSQLs       []string
	expectedTableNames [][]*filter.Table
	targetTableNames   [][]*filter.Table
	targetSQLs         []string
}

var testCases = []testCase{
	{
		"create schema `s1`",
		[]string{"CREATE DATABASE IF NOT EXISTS `s1`"},
		[][]*filter.Table{{genTableName("s1", "")}},
		[][]*filter.Table{{genTableName("xs1", "")}},
		[]string{"CREATE DATABASE IF NOT EXISTS `xs1`"},
	},
	{
		"create schema if not exists `s1`",
		[]string{"CREATE DATABASE IF NOT EXISTS `s1`"},
		[][]*filter.Table{{genTableName("s1", "")}},
		[][]*filter.Table{{genTableName("xs1", "")}},
		[]string{"CREATE DATABASE IF NOT EXISTS `xs1`"},
	},
	{
		"drop schema `s1`",
		[]string{"DROP DATABASE IF EXISTS `s1`"},
		[][]*filter.Table{{genTableName("s1", "")}},
		[][]*filter.Table{{genTableName("xs1", "")}},
		[]string{"DROP DATABASE IF EXISTS `xs1`"},
	},
	{
		"drop schema if exists `s1`",
		[]string{"DROP DATABASE IF EXISTS `s1`"},
		[][]*filter.Table{{genTableName("s1", "")}},
		[][]*filter.Table{{genTableName("xs1", "")}},
		[]string{"DROP DATABASE IF EXISTS `xs1`"},
	},
	{
		"drop table `Ss1`.`tT1`",
		[]string{"DROP TABLE IF EXISTS `Ss1`.`tT1`"},
		[][]*filter.Table{{genTableName("Ss1", "tT1")}},
		[][]*filter.Table{{genTableName("xSs1", "xtT1")}},
		[]string{"DROP TABLE IF EXISTS `xSs1`.`xtT1`"},
	},
	{
		"drop table `s1`.`t1`, `s2`.`t2`",
		[]string{"DROP TABLE IF EXISTS `s1`.`t1`", "DROP TABLE IF EXISTS `s2`.`t2`"},
		[][]*filter.Table{{genTableName("s1", "t1")}, {genTableName("s2", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}, {genTableName("xs2", "xt2")}},
		[]string{"DROP TABLE IF EXISTS `xs1`.`xt1`", "DROP TABLE IF EXISTS `xs2`.`xt2`"},
	},
	{
		"drop table `s1`.`t1`, `s2`.`t2`, `xx`",
		[]string{"DROP TABLE IF EXISTS `s1`.`t1`", "DROP TABLE IF EXISTS `s2`.`t2`", "DROP TABLE IF EXISTS `test`.`xx`"},
		[][]*filter.Table{{genTableName("s1", "t1")}, {genTableName("s2", "t2")}, {genTableName("test", "xx")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}, {genTableName("xs2", "xt2")}, {genTableName("xtest", "xxx")}},
		[]string{"DROP TABLE IF EXISTS `xs1`.`xt1`", "DROP TABLE IF EXISTS `xs2`.`xt2`", "DROP TABLE IF EXISTS `xtest`.`xxx`"},
	},
	{
		"create table `s1`.`t1` (id int)",
		[]string{"CREATE TABLE IF NOT EXISTS `s1`.`t1` (`id` INT)"},
		[][]*filter.Table{{genTableName("s1", "t1")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}},
		[]string{"CREATE TABLE IF NOT EXISTS `xs1`.`xt1` (`id` INT)"},
	},
	{
		"create table `t1` (id int)",
		[]string{"CREATE TABLE IF NOT EXISTS `test`.`t1` (`id` INT)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"CREATE TABLE IF NOT EXISTS `xtest`.`xt1` (`id` INT)"},
	},
	{
		"create table `t1` like `t2`",
		[]string{"CREATE TABLE IF NOT EXISTS `test`.`t1` LIKE `test`.`t2`"},
		[][]*filter.Table{{genTableName("test", "t1"), genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xtest", "xt1"), genTableName("xtest", "xt2")}},
		[]string{"CREATE TABLE IF NOT EXISTS `xtest`.`xt1` LIKE `xtest`.`xt2`"},
	},
	{
		"create table `s1`.`t1` like `t2`",
		[]string{"CREATE TABLE IF NOT EXISTS `s1`.`t1` LIKE `test`.`t2`"},
		[][]*filter.Table{{genTableName("s1", "t1"), genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}},
		[]string{"CREATE TABLE IF NOT EXISTS `xs1`.`xt1` LIKE `xtest`.`xt2`"},
	},
	{
		"create table `t1` like `xx`.`t2`",
		[]string{"CREATE TABLE IF NOT EXISTS `test`.`t1` LIKE `xx`.`t2`"},
		[][]*filter.Table{{genTableName("test", "t1"), genTableName("xx", "t2")}},
		[][]*filter.Table{{genTableName("xtest", "xt1"), genTableName("xxx", "xt2")}},
		[]string{"CREATE TABLE IF NOT EXISTS `xtest`.`xt1` LIKE `xxx`.`xt2`"},
	},
	{
		"truncate table `t1`",
		[]string{"TRUNCATE TABLE `test`.`t1`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"TRUNCATE TABLE `xtest`.`xt1`"},
	},
	{
		"truncate table `s1`.`t1`",
		[]string{"TRUNCATE TABLE `s1`.`t1`"},
		[][]*filter.Table{{genTableName("s1", "t1")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}},
		[]string{"TRUNCATE TABLE `xs1`.`xt1`"},
	},
	{
		"rename table `s1`.`t1` to `s2`.`t2`",
		[]string{"RENAME TABLE `s1`.`t1` TO `s2`.`t2`"},
		[][]*filter.Table{{genTableName("s1", "t1"), genTableName("s2", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1"), genTableName("xs2", "xt2")}},
		[]string{"RENAME TABLE `xs1`.`xt1` TO `xs2`.`xt2`"},
	},
	{
		"rename table `t1` to `t2`, `s1`.`t1` to `t2`",
		[]string{"RENAME TABLE `test`.`t1` TO `test`.`t2`", "RENAME TABLE `s1`.`t1` TO `test`.`t2`"},
		[][]*filter.Table{{genTableName("test", "t1"), genTableName("test", "t2")}, {genTableName("s1", "t1"), genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xtest", "xt1"), genTableName("xtest", "xt2")}, {genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}},
		[]string{"RENAME TABLE `xtest`.`xt1` TO `xtest`.`xt2`", "RENAME TABLE `xs1`.`xt1` TO `xtest`.`xt2`"},
	},
	{
		"drop index i1 on `s1`.`t1`",
		[]string{"DROP INDEX /*T! IF EXISTS  */`i1` ON `s1`.`t1`"},
		[][]*filter.Table{{genTableName("s1", "t1")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}},
		[]string{"DROP INDEX /*T! IF EXISTS  */`i1` ON `xs1`.`xt1`"},
	},
	{
		"drop index i1 on `t1`",
		[]string{"DROP INDEX /*T! IF EXISTS  */`i1` ON `test`.`t1`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"DROP INDEX /*T! IF EXISTS  */`i1` ON `xtest`.`xt1`"},
	},
	{
		"create index i1 on `t1`(`c1`)",
		[]string{"CREATE INDEX `i1` ON `test`.`t1` (`c1`)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"CREATE INDEX `i1` ON `xtest`.`xt1` (`c1`)"},
	},
	{
		"create index i1 on `s1`.`t1`(`c1`)",
		[]string{"CREATE INDEX `i1` ON `s1`.`t1` (`c1`)"},
		[][]*filter.Table{{genTableName("s1", "t1")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}},
		[]string{"CREATE INDEX `i1` ON `xs1`.`xt1` (`c1`)"},
	},
	{
		"alter table `t1` add column c1 int, drop column c2",
		[]string{"ALTER TABLE `test`.`t1` ADD COLUMN `c1` INT", "ALTER TABLE `test`.`t1` DROP COLUMN `c2`"},
		[][]*filter.Table{{genTableName("test", "t1")}, {genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}, {genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD COLUMN `c1` INT", "ALTER TABLE `xtest`.`xt1` DROP COLUMN `c2`"},
	},
	{
		"alter table `s1`.`t1` add column c1 int, rename to `t2`, drop column c2",
		[]string{"ALTER TABLE `s1`.`t1` ADD COLUMN `c1` INT", "ALTER TABLE `s1`.`t1` RENAME AS `test`.`t2`", "ALTER TABLE `test`.`t2` DROP COLUMN `c2`"},
		[][]*filter.Table{{genTableName("s1", "t1")}, {genTableName("s1", "t1"), genTableName("test", "t2")}, {genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}, {genTableName("xs1", "xt1"), genTableName("xtest", "xt2")}, {genTableName("xtest", "xt2")}},
		[]string{"ALTER TABLE `xs1`.`xt1` ADD COLUMN `c1` INT", "ALTER TABLE `xs1`.`xt1` RENAME AS `xtest`.`xt2`", "ALTER TABLE `xtest`.`xt2` DROP COLUMN `c2`"},
	},
	{
		"alter table `s1`.`t1` add column c1 int, rename to `xx`.`t2`, drop column c2",
		[]string{"ALTER TABLE `s1`.`t1` ADD COLUMN `c1` INT", "ALTER TABLE `s1`.`t1` RENAME AS `xx`.`t2`", "ALTER TABLE `xx`.`t2` DROP COLUMN `c2`"},
		[][]*filter.Table{{genTableName("s1", "t1")}, {genTableName("s1", "t1"), genTableName("xx", "t2")}, {genTableName("xx", "t2")}},
		[][]*filter.Table{{genTableName("xs1", "xt1")}, {genTableName("xs1", "xt1"), genTableName("xxx", "xt2")}, {genTableName("xxx", "xt2")}},
		[]string{"ALTER TABLE `xs1`.`xt1` ADD COLUMN `c1` INT", "ALTER TABLE `xs1`.`xt1` RENAME AS `xxx`.`xt2`", "ALTER TABLE `xxx`.`xt2` DROP COLUMN `c2`"},
	},
	{
		"alter table `t1` add column if not exists c1 int",
		[]string{"ALTER TABLE `test`.`t1` ADD COLUMN IF NOT EXISTS `c1` INT"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD COLUMN IF NOT EXISTS `c1` INT"},
	},
	{
		"alter table `t1` add index if not exists (a) using btree comment 'a'",
		[]string{"ALTER TABLE `test`.`t1` ADD INDEX IF NOT EXISTS(`a`) USING BTREE COMMENT 'a'"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD INDEX IF NOT EXISTS(`a`) USING BTREE COMMENT 'a'"},
	},
	{
		"alter table `t1` add constraint fk_t2_id foreign key if not exists (t2_id) references t2(id)",
		[]string{"ALTER TABLE `test`.`t1` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY IF NOT EXISTS (`t2_id`) REFERENCES `t2`(`id`)"},
		[][]*filter.Table{{genTableName("test", "t1"), genTableName("test", "t2")}},
		[][]*filter.Table{{genTableName("xtest", "xt1"), genTableName("xtest", "xt2")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD CONSTRAINT `fk_t2_id` FOREIGN KEY IF NOT EXISTS (`t2_id`) REFERENCES `xtest`.`xt2`(`id`)"},
	},
	{
		"create index if not exists i1 on `t1`(`c1`)",
		[]string{"CREATE INDEX IF NOT EXISTS `i1` ON `test`.`t1` (`c1`)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"CREATE INDEX IF NOT EXISTS `i1` ON `xtest`.`xt1` (`c1`)"},
	},
	{
		"alter table `t1` add partition if not exists ( partition p2 values less than maxvalue)",
		[]string{"ALTER TABLE `test`.`t1` ADD PARTITION IF NOT EXISTS (PARTITION `p2` VALUES LESS THAN (MAXVALUE))"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` ADD PARTITION IF NOT EXISTS (PARTITION `p2` VALUES LESS THAN (MAXVALUE))"},
	},
	{
		"alter table `t1` drop column if exists c2",
		[]string{"ALTER TABLE `test`.`t1` DROP COLUMN IF EXISTS `c2`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` DROP COLUMN IF EXISTS `c2`"},
	},
	{
		"alter table `t1` change column if exists a b varchar(255)",
		[]string{"ALTER TABLE `test`.`t1` CHANGE COLUMN IF EXISTS `a` `b` VARCHAR(255)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` CHANGE COLUMN IF EXISTS `a` `b` VARCHAR(255)"},
	},
	{
		"alter table `t1` modify column if exists a varchar(255)",
		[]string{"ALTER TABLE `test`.`t1` MODIFY COLUMN IF EXISTS `a` VARCHAR(255)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` MODIFY COLUMN IF EXISTS `a` VARCHAR(255)"},
	},
	{
		"alter table `t1` drop index if exists i1",
		[]string{"ALTER TABLE `test`.`t1` DROP INDEX IF EXISTS `i1`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` DROP INDEX IF EXISTS `i1`"},
	},
	{
		"alter table `t1` drop foreign key if exists fk_t2_id",
		[]string{"ALTER TABLE `test`.`t1` DROP FOREIGN KEY IF EXISTS `fk_t2_id`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` DROP FOREIGN KEY IF EXISTS `fk_t2_id`"},
	},
	{
		"alter table `t1` drop partition if exists p2",
		[]string{"ALTER TABLE `test`.`t1` DROP PARTITION IF EXISTS `p2`"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` DROP PARTITION IF EXISTS `p2`"},
	},
	{
		"alter table `t1` partition by hash(a)",
		[]string{"ALTER TABLE `test`.`t1` PARTITION BY HASH (`a`) PARTITIONS 1"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY HASH (`a`) PARTITIONS 1"},
	},
	{
		"alter table `t1` partition by key(a)",
		[]string{"ALTER TABLE `test`.`t1` PARTITION BY KEY (`a`) PARTITIONS 1"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY KEY (`a`) PARTITIONS 1"},
	},
	{
		"alter table `t1` partition by range(a) (partition x values less than (75))",
		[]string{"ALTER TABLE `test`.`t1` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY RANGE (`a`) (PARTITION `x` VALUES LESS THAN (75))"},
	},
	{
		"alter table `t1` partition by list columns (a, b) (partition x values in ((10, 20)))",
		[]string{"ALTER TABLE `test`.`t1` PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY LIST COLUMNS (`a`,`b`) (PARTITION `x` VALUES IN ((10, 20)))"},
	},
	{
		"alter table `t1` partition by list (a) (partition x default)",
		[]string{"ALTER TABLE `test`.`t1` PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY LIST (`a`) (PARTITION `x` DEFAULT)"},
	},
	{
		"alter table `t1` partition by system_time (partition x history, partition y current)",
		[]string{"ALTER TABLE `test`.`t1` PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)"},
		[][]*filter.Table{{genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "xt1")}},
		[]string{"ALTER TABLE `xtest`.`xt1` PARTITION BY SYSTEM_TIME (PARTITION `x` HISTORY,PARTITION `y` CURRENT)"},
	},
	{
		"alter database `test` charset utf8mb4",
		[]string{"ALTER DATABASE `test` CHARACTER SET = utf8mb4"},
		[][]*filter.Table{{genTableName("test", "")}},
		[][]*filter.Table{{genTableName("xtest", "")}},
		[]string{"ALTER DATABASE `xtest` CHARACTER SET = utf8mb4"},
	},
	{
		"alter table `t1` add column (c1 int, c2 int)",
		[]string{"ALTER TABLE `test`.`t1` ADD COLUMN `c1` INT", "ALTER TABLE `test`.`t1` ADD COLUMN `c2` INT"},
		[][]*filter.Table{{genTableName("test", "t1")}, {genTableName("test", "t1")}},
		[][]*filter.Table{{genTableName("xtest", "t1")}, {genTableName("xtest", "t1")}},
		[]string{"ALTER TABLE `xtest`.`t1` ADD COLUMN `c1` INT", "ALTER TABLE `xtest`.`t1` ADD COLUMN `c2` INT"},
	},
}

var nonDDLs = []string{
	"GRANT CREATE TABLESPACE ON *.* TO `root`@`%` WITH GRANT OPTION",
}

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testParserSuite struct{}

func (t *testParserSuite) TestParser(c *C) {
	p := parser.New()

	for _, ca := range testCases {
		sql := ca.sql
		stmts, err := Parse(p, sql, "", "")
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
	}

	for _, sql := range nonDDLs {
		stmts, err := Parse(p, sql, "", "")
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
	}

	unsupportedSQLs := []string{
		"alter table bar ADD SPATIAL INDEX (`g`)",
	}

	for _, sql := range unsupportedSQLs {
		_, err := Parse(p, sql, "", "")
		c.Assert(err, NotNil)
	}
}

func (t *testParserSuite) TestError(c *C) {
	p := parser.New()

	// DML will report ErrUnknownTypeDDL
	dml := "INSERT INTO `t1` VALUES (1)"

	stmts, err := Parse(p, dml, "", "")
	c.Assert(err, IsNil)
	_, err = FetchDDLTables("test", stmts[0], utils.LCTableNamesInsensitive)
	c.Assert(terror.ErrUnknownTypeDDL.Equal(err), IsTrue)

	_, err = RenameDDLTable(stmts[0], nil)
	c.Assert(terror.ErrUnknownTypeDDL.Equal(err), IsTrue)

	// tableRenameVisitor with less `targetNames` won't panic
	ddl := "create table `s1`.`t1` (id int)"
	stmts, err = Parse(p, ddl, "", "")
	c.Assert(err, IsNil)
	_, err = RenameDDLTable(stmts[0], nil)
	c.Assert(terror.ErrRewriteSQL.Equal(err), IsTrue)
}

func (t *testParserSuite) TestResolveDDL(c *C) {
	p := parser.New()

	for _, ca := range testCases {
		stmts, err := Parse(p, ca.sql, "", "")
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)

		statements, err := SplitDDL(stmts[0], "test")
		c.Assert(err, IsNil)
		c.Assert(statements, DeepEquals, ca.expectedSQLs)

		tbs := ca.expectedTableNames
		for j, statement := range statements {
			s, err := Parse(p, statement, "", "")
			c.Assert(err, IsNil)
			c.Assert(s, HasLen, 1)

			tableNames, err := FetchDDLTables("test", s[0], utils.LCTableNamesSensitive)
			c.Assert(err, IsNil)
			c.Assert(tableNames, DeepEquals, tbs[j])

			targetSQL, err := RenameDDLTable(s[0], ca.targetTableNames[j])
			c.Assert(err, IsNil)
			c.Assert(targetSQL, Equals, ca.targetSQLs[j])
		}
	}
}

func (t *testParserSuite) TestCheckIsDDL(c *C) {
	var (
		cases = []struct {
			sql   string
			isDDL bool
		}{
			{
				sql:   "CREATE DATABASE test_is_ddl",
				isDDL: true,
			},
			{
				sql:   "BEGIN",
				isDDL: false,
			},
			{
				sql:   "INSERT INTO test_is_ddl.test_is_ddl_table VALUES (1)",
				isDDL: false,
			},
			{
				sql:   "INVAID SQL STATEMENT",
				isDDL: false,
			},
		}
		parser2 = parser.New()
	)

	for _, cs := range cases {
		c.Assert(CheckIsDDL(cs.sql, parser2), Equals, cs.isDDL)
	}
}
