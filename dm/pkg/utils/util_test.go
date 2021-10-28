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

package utils

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/errno"
)

func (t *testUtilsSuite) TestDecodeBinlogPosition(c *C) {
	testCases := []struct {
		pos      string
		isErr    bool
		expecetd *mysql.Position
	}{
		{"()", true, nil},
		{"(,}", true, nil},
		{"(,)", true, nil},
		{"(mysql-bin.00001,154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
		{"(mysql-bin.00001, 154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
		{"(mysql-bin.00001\t,  154)", false, &mysql.Position{Name: "mysql-bin.00001", Pos: 154}},
	}

	for _, tc := range testCases {
		pos, err := DecodeBinlogPosition(tc.pos)
		if tc.isErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(pos, DeepEquals, tc.expecetd)
		}
	}
}

func (t *testUtilsSuite) TestWaitSomething(c *C) {
	var (
		backoff  = 10
		waitTime = 10 * time.Millisecond
		count    = 0
	)

	// wait fail
	f1 := func() bool {
		count++
		return false
	}
	c.Assert(WaitSomething(backoff, waitTime, f1), IsFalse)
	c.Assert(count, Equals, backoff)

	count = 0 // reset
	// wait success
	f2 := func() bool {
		count++
		return count >= 5
	}

	c.Assert(WaitSomething(backoff, waitTime, f2), IsTrue)
	c.Assert(count, Equals, 5)
}

func (t *testUtilsSuite) TestHidePassword(c *C) {
	strs := []struct {
		old string
		new string
	}{
		{ // operate source
			`from:\n  host: 127.0.0.1\n  user: root\n  password: /Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=\n  port: 3306\n`,
			`from:\n  host: 127.0.0.1\n  user: root\n  password: ******\n  port: 3306\n`,
		}, { // operate source empty password
			`from:\n  host: 127.0.0.1\n  user: root\n  password: \n  port: 3306\n`,
			`from:\n  host: 127.0.0.1\n  user: root\n  password: ******\n  port: 3306\n`,
		}, { // start task
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs=\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"******\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
		}, { // start task empty passowrd
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"******\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
		},
	}
	for _, str := range strs {
		c.Assert(HidePassword(str.old), Equals, str.new)
	}
}

func (t *testUtilsSuite) TestUnwrapScheme(c *C) {
	cases := []struct {
		old string
		new string
	}{
		{
			"http://0.0.0.0:123",
			"0.0.0.0:123",
		},
		{
			"https://0.0.0.0:123",
			"0.0.0.0:123",
		},
		{
			"http://abc.com:123",
			"abc.com:123",
		},
		{
			"httpsdfpoje.com",
			"httpsdfpoje.com",
		},
		{
			"",
			"",
		},
	}
	for _, ca := range cases {
		c.Assert(UnwrapScheme(ca.old), Equals, ca.new)
	}
}

func (t *testUtilsSuite) TestWrapSchemes(c *C) {
	cases := []struct {
		old   string
		http  string
		https string
	}{
		{
			"0.0.0.0:123",
			"http://0.0.0.0:123",
			"https://0.0.0.0:123",
		},
		{
			"abc.com:123",
			"http://abc.com:123",
			"https://abc.com:123",
		},
		{
			"abc.com:123,http://abc.com:123,0.0.0.0:123,https://0.0.0.0:123",
			"http://abc.com:123,http://abc.com:123,http://0.0.0.0:123,http://0.0.0.0:123",
			"https://abc.com:123,https://abc.com:123,https://0.0.0.0:123,https://0.0.0.0:123",
		},
		{
			"",
			"",
			"",
		},
	}
	for _, ca := range cases {
		c.Assert(WrapSchemes(ca.old, false), Equals, ca.http)
		c.Assert(WrapSchemes(ca.old, true), Equals, ca.https)
	}
}

func (t *testUtilsSuite) TestWrapSchemesForInitialCluster(c *C) {
	c.Assert(WrapSchemesForInitialCluster("master1=http://127.0.0.1:8291,master2=http://127.0.0.1:8292,master3=http://127.0.0.1:8293", false), Equals,
		"master1=http://127.0.0.1:8291,master2=http://127.0.0.1:8292,master3=http://127.0.0.1:8293")
	c.Assert(WrapSchemesForInitialCluster("master1=http://127.0.0.1:8291,master2=http://127.0.0.1:8292,master3=http://127.0.0.1:8293", true), Equals,
		"master1=https://127.0.0.1:8291,master2=https://127.0.0.1:8292,master3=https://127.0.0.1:8293")

	// correct `http` or `https` for some URLs
	c.Assert(WrapSchemesForInitialCluster("master1=http://127.0.0.1:8291,master2=127.0.0.1:8292,master3=https://127.0.0.1:8293", false), Equals,
		"master1=http://127.0.0.1:8291,master2=http://127.0.0.1:8292,master3=http://127.0.0.1:8293")
	c.Assert(WrapSchemesForInitialCluster("master1=http://127.0.0.1:8291,master2=127.0.0.1:8292,master3=https://127.0.0.1:8293", true), Equals,
		"master1=https://127.0.0.1:8291,master2=https://127.0.0.1:8292,master3=https://127.0.0.1:8293")

	// add `http` or `https` for all URLs
	c.Assert(WrapSchemesForInitialCluster("master1=127.0.0.1:8291,master2=127.0.0.1:8292,master3=127.0.0.1:8293", false), Equals,
		"master1=http://127.0.0.1:8291,master2=http://127.0.0.1:8292,master3=http://127.0.0.1:8293")
	c.Assert(WrapSchemesForInitialCluster("master1=127.0.0.1:8291,master2=127.0.0.1:8292,master3=127.0.0.1:8293", true), Equals,
		"master1=https://127.0.0.1:8291,master2=https://127.0.0.1:8292,master3=https://127.0.0.1:8293")
}

func (t *testUtilsSuite) TestIsContextCanceledError(c *C) {
	c.Assert(IsContextCanceledError(context.Canceled), IsTrue)
	c.Assert(IsContextCanceledError(context.DeadlineExceeded), IsFalse)
	c.Assert(IsContextCanceledError(errors.New("another error")), IsFalse)
}

func (t *testUtilsSuite) TestIgnoreErrorCheckpoint(c *C) {
	c.Assert(IgnoreErrorCheckpoint(newMysqlErr(errno.ErrDupFieldName, "Duplicate column name c1")), IsTrue)
	c.Assert(IgnoreErrorCheckpoint(newMysqlErr(errno.ErrTableExists, "Table tbl already exists")), IsFalse)
	c.Assert(IgnoreErrorCheckpoint(errors.New("another error")), IsFalse)
}

func (t *testUtilsSuite) TestIsBuildInSkipDDL(c *C) {
	c.Assert(IsBuildInSkipDDL("alter table tbl add column c1 int"), IsFalse)
	c.Assert(IsBuildInSkipDDL("DROP PROCEDURE"), IsTrue)

	cases := []struct {
		sql           string
		expectSkipped bool
	}{
		{"SAVEPOINT `a1`", true},

		// flush
		{"flush privileges", true},
		{"flush logs", true},
		{"FLUSH TABLES WITH READ LOCK", true},

		// table maintenance
		{"OPTIMIZE TABLE foo", true},
		{"ANALYZE TABLE foo", true},
		{"REPAIR TABLE foo", true},

		// temporary table
		{"DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `h2`", true},
		{"DROP TEMPORARY TABLE IF EXISTS `foo`.`bar` /* generated by server */", true},
		{"DROP TABLE foo.bar", false},
		{"DROP TABLE `TEMPORARY TABLE`", false},
		{"DROP TABLE `TEMPORARY TABLE` /* generated by server */", false},

		// trigger
		{"CREATE DEFINER=`root`@`%` TRIGGER ins_sum BEFORE INSERT ON bar FOR EACH ROW SET @sum = @sum + NEW.id", true},
		{"CREATE TRIGGER ins_sum BEFORE INSERT ON bar FOR EACH ROW SET @sum = @sum + NEW.id", true},
		{"DROP TRIGGER ins_sum", true},
		{"create table `trigger`(id int)", false},

		// procedure
		{"drop procedure if exists prepare_data", true},
		{"CREATE DEFINER=`root`@`%` PROCEDURE `simpleproc`(OUT param1 INT) BEGIN  select count(*) into param1 from shard_0001; END", true},
		{"CREATE PROCEDURE simpleproc(OUT param1 INT) BEGIN  select count(*) into param1 from shard_0001; END", true},
		{"alter procedure prepare_data comment 'i am a comment'", true},
		{"create table `procedure`(id int)", false},

		{`CREATE DEFINER=root@localhost PROCEDURE simpleproc(OUT param1 INT)
BEGIN
    SELECT COUNT(*) INTO param1 FROM t;
END`, true},

		// view
		{"CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT qty, price, qty*price AS value FROM t", true},
		{"CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT qty, price, qty*price AS value FROM t", true},
		{"ALTER ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS SELECT qty, price, qty*price AS value FROM t", true},
		{"DROP VIEW v", true},
		{"CREATE TABLE `VIEW`(id int)", false},
		{"ALTER TABLE `VIEW`(id int)", false},

		// function
		{"CREATE FUNCTION metaphon RETURNS STRING SONAME 'udf_example.so'", true},
		{"CREATE AGGREGATE FUNCTION avgcost RETURNS REAL SONAME 'udf_example.so'", true},
		{"DROP FUNCTION metaphon", true},
		{"DROP FUNCTION IF EXISTS `rand_string`", true},
		{"ALTER FUNCTION metaphon COMMENT 'hh'", true},
		{"CREATE TABLE `function` (id int)", false},

		{`CREATE DEFINER=root@localhost FUNCTION rand_string(n INT) RETURNS varchar(255) CHARSET utf8
BEGIN
          DECLARE chars_str VARCHAR(100) DEFAULT 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
          DECLARE return_str VARCHAR(255) DEFAULT '';
          DECLARE i INT DEFAULT 0;
          WHILE i<n DO
              SET return_str = CONCAT(return_str,SUBSTRING(chars_str,FLOOR(1+RAND()*52),1));
              SET i = i+1;
          END WHILE;
    RETURN return_str;
END`, true},

		// tablespace
		{"CREATE TABLESPACE `ts1` ADD DATAFILE 'ts1.ibd' ENGINE=INNODB", true},
		{"ALTER TABLESPACE `ts1` DROP DATAFILE 'ts1.idb' ENGIEN=NDB", true},
		{"DROP TABLESPACE ts1", true},

		// event
		{"CREATE DEFINER=CURRENT_USER EVENT myevent ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 HOUR DO UPDATE myschema.mytable SET mycol = mycol + 1;", true},
		{"ALTER DEFINER = CURRENT_USER EVENT myevent ON SCHEDULE EVERY 12 HOUR STARTS CURRENT_TIMESTAMP + INTERVAL 4 HOUR;", true},
		{"DROP EVENT myevent;", true},

		// account management
		{"CREATE USER 't'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*93E34F4B81FEC9E8271655EA87646ED01AF377CC'", true},
		{"ALTER USER 't'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*1114744159A0EF13B12FC371C94877763F9512D0'", true},
		{"rename user t to 1", true},
		{"drop user t1", true},
		{"GRANT ALL PRIVILEGES ON *.* TO 't2'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*12033B78389744F3F39AC4CE4CCFCAD6960D8EA0'", true},
		{"revoke reload on *.* from 't2'@'%'", true},
	}
	for _, ca := range cases {
		c.Assert(IsBuildInSkipDDL(ca.sql), Equals, ca.expectSkipped)
	}
}

func (t *testUtilsSuite) TestProxyFields(c *C) {
	revIndex := map[string]int{
		"http_proxy":  0,
		"https_proxy": 1,
		"no_proxy":    2,
	}
	envs := []string{"http_proxy", "https_proxy", "no_proxy"}
	envPreset := []string{"http://127.0.0.1:8080", "https://127.0.0.1:8443", "localhost,127.0.0.1"}

	// Exhaust all combinations of those environment variables' selection.
	// Each bit of the mask decided whether this index of `envs` would be set.
	for mask := 0; mask <= 0b111; mask++ {
		for _, env := range envs {
			c.Assert(os.Unsetenv(env), IsNil)
		}

		for i := 0; i < 3; i++ {
			if (1<<i)&mask != 0 {
				c.Assert(os.Setenv(envs[i], envPreset[i]), IsNil)
			}
		}

		for _, field := range proxyFields() {
			idx, ok := revIndex[field.Key]
			c.Assert(ok, IsTrue)
			c.Assert((1<<idx)&mask, Not(Equals), 0)
			c.Assert(field.String, Equals, envPreset[idx])
		}
	}
}
