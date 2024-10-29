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

package diff

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
)

func TestClient(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testDiffSuite{})

type testDiffSuite struct{}

func (*testDiffSuite) TestGenerateSQLs(c *check.C) {
	createTableSQL := "CREATE TABLE `diff_test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), `id_gen` int(11) GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL, primary key(`id`, `name`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	c.Assert(err, check.IsNil)

	rowsData := map[string]*dbutil.ColumnData{
		"id":          {Data: []byte("1"), IsNull: false},
		"name":        {Data: []byte("xxx"), IsNull: false},
		"birthday":    {Data: []byte("2018-01-01 00:00:00"), IsNull: false},
		"update_time": {Data: []byte("10:10:10"), IsNull: false},
		"money":       {Data: []byte("11.1111"), IsNull: false},
		"id_gen":      {Data: []byte("2"), IsNull: false}, // generated column should not be contained in fix sql
	}

	replaceSQL := generateDML("replace", rowsData, tableInfo, "diff_test")
	deleteSQL := generateDML("delete", rowsData, tableInfo, "diff_test")
	c.Assert(replaceSQL, check.Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, check.Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` = 'xxx' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")

	// test the unique key
	createTableSQL2 := "CREATE TABLE `diff_test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), unique key(`id`, `name`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	c.Assert(err, check.IsNil)
	replaceSQL = generateDML("replace", rowsData, tableInfo2, "diff_test")
	deleteSQL = generateDML("delete", rowsData, tableInfo2, "diff_test")
	c.Assert(replaceSQL, check.Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, check.Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` = 'xxx' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")

	// test value is nil
	rowsData["name"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = generateDML("replace", rowsData, tableInfo, "diff_test")
	deleteSQL = generateDML("delete", rowsData, tableInfo, "diff_test")
	c.Assert(replaceSQL, check.Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, check.Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` is NULL AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")

	rowsData["id"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = generateDML("replace", rowsData, tableInfo, "diff_test")
	deleteSQL = generateDML("delete", rowsData, tableInfo, "diff_test")
	c.Assert(replaceSQL, check.Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, check.Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` is NULL AND `name` is NULL AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")

	// test value with "'"
	rowsData["name"] = &dbutil.ColumnData{Data: []byte("a'a"), IsNull: false}
	replaceSQL = generateDML("replace", rowsData, tableInfo, "diff_test")
	deleteSQL = generateDML("delete", rowsData, tableInfo, "diff_test")
	c.Assert(replaceSQL, check.Equals, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,'a\\'a','2018-01-01 00:00:00','10:10:10',11.1111);")
	c.Assert(deleteSQL, check.Equals, "DELETE FROM `diff_test`.`atest` WHERE `id` is NULL AND `name` = 'a\\'a' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111;")
}

func (*testDiffSuite) TestConfigHash(c *check.C) {
	tbDiff := &TableDiff{
		Range:     "a > 1",
		ChunkSize: 1000,
	}
	tbDiff.setConfigHash()
	hash1 := tbDiff.configHash

	tbDiff.CheckThreadCount = 10
	tbDiff.setConfigHash()
	hash2 := tbDiff.configHash
	c.Assert(hash1, check.Equals, hash2)

	tbDiff.Range = "b < 10"
	tbDiff.setConfigHash()
	hash3 := tbDiff.configHash
	c.Assert(hash1 == hash3, check.Equals, false)
}
