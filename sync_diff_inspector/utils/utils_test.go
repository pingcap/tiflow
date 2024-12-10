// Copyright 2021 PingCAP, Inc.
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
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/stretchr/testify/require"
)

type tableCaseType struct {
	schema         string
	table          string
	createTableSQL string
	rowColumns     []string
	rows           [][]driver.Value
	indices        []string
	sels           []float64
	selected       string
}

func TestWorkerPool(t *testing.T) {
	pool := NewWorkerPool(2, "test")
	infoCh := make(chan uint64)
	doneCh := make(chan struct{})
	var v uint64 = 0
	pool.Apply(func() {
		infoCh <- 2
	})
	pool.Apply(func() {
		newV := <-infoCh
		v = newV
		doneCh <- struct{}{}
	})
	<-doneCh
	require.Equal(t, v, uint64(2))
	require.True(t, pool.HasWorker())
	pool.WaitFinished()
}

func TestStringsToInterface(t *testing.T) {
	res := []interface{}{"1", "2", "3"}
	require.Equal(t, res[0], "1")
	require.Equal(t, res[1], "2")
	require.Equal(t, res[2], "3")

	require.Equal(t, MinLenInSlices([][]string{{"123", "324", "r32"}, {"32", "23"}}), 2)

	expectSlice := []string{"2", "3", "4"}
	sliceMap := SliceToMap(expectSlice)
	for _, expect := range expectSlice {
		_, ok := sliceMap[expect]
		require.True(t, ok)
	}
	require.Equal(t, len(sliceMap), len(expectSlice))

	require.Equal(t, UniqueID("123", "456"), "`123`.`456`")
}

func TestBasicTableUtilOperation(t *testing.T) {
	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	query, orderKeyCols := GetTableRowsQueryFormat("test", "test", tableInfo, "123")
	require.Equal(t, query, "SELECT /*!40001 SQL_NO_CACHE */ `a`, `b`, round(`c`, 5-floor(log10(abs(`c`)))) as `c`, `d` FROM `test`.`test` WHERE %s ORDER BY `a`,`b` COLLATE '123'")
	expectName := []string{"a", "b"}
	for i, col := range orderKeyCols {
		require.Equal(t, col.Name.O, expectName[i])
	}

	data1 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: false},
		"b": {Data: []byte("a"), IsNull: false},
		"c": {Data: []byte("1.22"), IsNull: false},
		"d": {Data: []byte("sdf"), IsNull: false},
	}
	data2 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: false},
		"b": {Data: []byte("b"), IsNull: false},
		"c": {Data: []byte("2.22"), IsNull: false},
		"d": {Data: []byte("sdf"), IsNull: false},
	}
	data3 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("2"), IsNull: false},
		"b": {Data: []byte("a"), IsNull: false},
		"c": {Data: []byte("0.22"), IsNull: false},
		"d": {Data: []byte("asdf"), IsNull: false},
	}
	data4 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: false},
		"b": {Data: []byte("a"), IsNull: true},
		"c": {Data: []byte("0.221"), IsNull: false},
		"d": {Data: []byte("asdf"), IsNull: false},
	}
	data5 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("2"), IsNull: false},
		"b": {Data: []byte("a"), IsNull: true},
		"c": {Data: []byte("0.222"), IsNull: false},
		"d": {Data: []byte("asdf"), IsNull: false},
	}
	data6 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: true},
		"b": {Data: []byte("a"), IsNull: false},
		"c": {Data: []byte("0.2221"), IsNull: false},
		"d": {Data: []byte("asdf"), IsNull: false},
	}
	data7 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: true},
		"b": {Data: []byte("a"), IsNull: false},
		"c": {Data: []byte("0.2221"), IsNull: false},
		"d": {Data: []byte("asdf"), IsNull: false},
	}
	data8 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: false},
		"b": {Data: []byte("a"), IsNull: false},
		"c": {Data: []byte(""), IsNull: true},
		"d": {Data: []byte("sdf"), IsNull: false},
	}
	data9 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: false},
		"b": {Data: []byte("a"), IsNull: false},
		"c": {Data: []byte("0"), IsNull: false},
		"d": {Data: []byte("sdf"), IsNull: false},
	}

	columns := tableInfo.Columns

	require.Equal(t, GenerateReplaceDML(data1, tableInfo, "schema"), "REPLACE INTO `schema`.`test`(`a`,`b`,`c`,`d`) VALUES (1,'a',1.22,'sdf');")
	require.Equal(t, GenerateDeleteDML(data8, tableInfo, "schema"), "DELETE FROM `schema`.`test` WHERE `a` = 1 AND `b` = 'a' AND `c` is NULL AND `d` = 'sdf' LIMIT 1;")
	require.Equal(t, GenerateDeleteDML(data9, tableInfo, "schema"), "DELETE FROM `schema`.`test` WHERE `a` = 1 AND `b` = 'a' AND `c` = 0 AND `d` = 'sdf' LIMIT 1;")
	require.Equal(t, GenerateReplaceDMLWithAnnotation(data1, data2, tableInfo, "schema"),
		"/*\n"+
			"  DIFF COLUMNS ╏ `B` ╏ `C`   \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍╍\n"+
			"  source data  ╏ 'a' ╏ 1.22  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍╍\n"+
			"  target data  ╏ 'b' ╏ 2.22  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍╍\n"+
			"*/\n"+
			"REPLACE INTO `schema`.`test`(`a`,`b`,`c`,`d`) VALUES (1,'a',1.22,'sdf');")
	require.Equal(t, GenerateDeleteDML(data1, tableInfo, "schema"), "DELETE FROM `schema`.`test` WHERE `a` = 1 AND `b` = 'a' AND `c` = 1.22 AND `d` = 'sdf' LIMIT 1;")

	// same
	equal, cmp, err := CompareData(data1, data1, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(0))
	require.True(t, equal)

	// orderkey same but other column different
	equal, cmp, err = CompareData(data1, data3, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(-1))
	require.False(t, equal)

	equal, cmp, err = CompareData(data3, data1, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(1))
	require.False(t, equal)

	// orderKey different
	equal, cmp, err = CompareData(data1, data2, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(-1))
	require.False(t, equal)

	equal, cmp, err = CompareData(data2, data1, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(1))
	require.False(t, equal)

	equal, cmp, err = CompareData(data4, data1, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(0))
	require.False(t, equal)

	equal, cmp, err = CompareData(data1, data4, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(0))
	require.False(t, equal)

	equal, cmp, err = CompareData(data5, data4, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(1))
	require.False(t, equal)

	equal, cmp, err = CompareData(data4, data5, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(-1))
	require.False(t, equal)

	equal, cmp, err = CompareData(data4, data6, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(1))
	require.False(t, equal)

	equal, cmp, err = CompareData(data6, data4, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(-1))
	require.False(t, equal)

	equal, cmp, err = CompareData(data6, data7, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(0))
	require.True(t, equal)

	equal, cmp, err = CompareData(data1, data8, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(0))
	require.False(t, equal)

	equal, cmp, err = CompareData(data8, data1, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(0))
	require.False(t, equal)

	equal, cmp, err = CompareData(data8, data9, orderKeyCols, columns)
	require.NoError(t, err)
	require.Equal(t, cmp, int32(0))
	require.False(t, equal)

	// Test ignore columns
	createTableSQL = "create table `test`.`test`(`a` int, `c` float, `b` varchar(10), `d` datetime, `e` timestamp, primary key(`a`, `b`), key(`c`, `d`))"
	tableInfo, err = dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	require.Equal(t, len(tableInfo.Indices), 2)
	require.Equal(t, len(tableInfo.Columns), 5)
	require.Equal(t, tableInfo.Indices[0].Columns[1].Name.O, "b")
	require.Equal(t, tableInfo.Indices[0].Columns[1].Offset, 2)
	info, hasTimeStampType := ResetColumns(tableInfo, []string{"c"})
	require.True(t, hasTimeStampType)
	require.Equal(t, len(info.Indices), 1)
	require.Equal(t, len(info.Columns), 4)
	require.Equal(t, tableInfo.Indices[0].Columns[1].Name.O, "b")
	require.Equal(t, tableInfo.Indices[0].Columns[1].Offset, 1)
}

func TestGetCountAndMD5Checksum(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	createTableSQL := "create table `test`.`test`(`a` int, `c` float, `b` varchar(10), `d` datetime, primary key(`a`, `b`), key(`c`, `d`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	mock.ExpectQuery("SELECT COUNT.*FROM `test_schema`\\.`test_table` WHERE \\[23 45\\].*").WithArgs("123", "234").WillReturnRows(sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(123, 456))

	count, checksum, err := GetCountAndMD5Checksum(ctx, conn, "test_schema", "test_table", tableInfo, "[23 45]", []interface{}{"123", "234"})
	require.NoError(t, err)
	require.Equal(t, count, int64(123))
	require.Equal(t, checksum, uint64(0x1c8))
}

func TestGetApproximateMid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), primary key(`a`, `b`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"a", "b"}).AddRow("5", "10")
	mock.ExpectQuery("SELECT `a`, `b` FROM `test`.`test_utils` WHERE 2222 ORDER BY `a`, `b` LIMIT 1 OFFSET 10").WithArgs("aaaa").WillReturnRows(rows)

	data, err := GetApproximateMidBySize(ctx, conn, "test", "test_utils", tableInfo.Columns, "2222", []interface{}{"aaaa"}, 20)
	require.NoError(t, err)
	require.Equal(t, data["a"], "5")
	require.Equal(t, data["b"], "10")

	// no data
	rows = sqlmock.NewRows([]string{"a", "b"})
	mock.ExpectQuery("SELECT `a`, `b` FROM `test`\\.`test_utils` WHERE 2222.* LIMIT 1 OFFSET 10*").WithArgs("aaaa").WillReturnRows(rows)

	data, err = GetApproximateMidBySize(ctx, conn, "test", "test_utils", tableInfo.Columns, "2222", []interface{}{"aaaa"}, 20)
	require.NoError(t, err)
	require.Nil(t, data)
}

func TestGenerateSQLs(t *testing.T) {
	createTableSQL := "CREATE TABLE `diff_test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), `id_gen` int(11) GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL, primary key(`id`, `name`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	rowsData := map[string]*dbutil.ColumnData{
		"id":          {Data: []byte("1"), IsNull: false},
		"name":        {Data: []byte("xxx"), IsNull: false},
		"birthday":    {Data: []byte("2018-01-01 00:00:00"), IsNull: false},
		"update_time": {Data: []byte("10:10:10"), IsNull: false},
		"money":       {Data: []byte("11.1111"), IsNull: false},
		"id_gen":      {Data: []byte("2"), IsNull: false}, // generated column should not be contained in fix sql
	}

	replaceSQL := GenerateReplaceDML(rowsData, tableInfo, "diff_test")
	deleteSQL := GenerateDeleteDML(rowsData, tableInfo, "diff_test")
	require.Equal(t, replaceSQL, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);")
	require.Equal(t, deleteSQL, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` = 'xxx' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111 LIMIT 1;")

	// test the unique key
	createTableSQL2 := "CREATE TABLE `diff_test`.`atest` (`id` int(24), `name` varchar(24), `birthday` datetime, `update_time` time, `money` decimal(20,2), unique key(`id`, `name`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)
	replaceSQL = GenerateReplaceDML(rowsData, tableInfo2, "diff_test")
	deleteSQL = GenerateDeleteDML(rowsData, tableInfo2, "diff_test")
	require.Equal(t, replaceSQL, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,'xxx','2018-01-01 00:00:00','10:10:10',11.1111);")
	require.Equal(t, deleteSQL, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` = 'xxx' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111 LIMIT 1;")

	// test value is nil
	rowsData["name"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = GenerateReplaceDML(rowsData, tableInfo, "diff_test")
	deleteSQL = GenerateDeleteDML(rowsData, tableInfo, "diff_test")
	require.Equal(t, replaceSQL, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (1,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);")
	require.Equal(t, deleteSQL, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `name` is NULL AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111 LIMIT 1;")

	rowsData["id"] = &dbutil.ColumnData{Data: []byte(""), IsNull: true}
	replaceSQL = GenerateReplaceDML(rowsData, tableInfo, "diff_test")
	deleteSQL = GenerateDeleteDML(rowsData, tableInfo, "diff_test")
	require.Equal(t, replaceSQL, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,NULL,'2018-01-01 00:00:00','10:10:10',11.1111);")
	require.Equal(t, deleteSQL, "DELETE FROM `diff_test`.`atest` WHERE `id` is NULL AND `name` is NULL AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111 LIMIT 1;")

	// test value with "'"
	rowsData["name"] = &dbutil.ColumnData{Data: []byte("a'a"), IsNull: false}
	replaceSQL = GenerateReplaceDML(rowsData, tableInfo, "diff_test")
	deleteSQL = GenerateDeleteDML(rowsData, tableInfo, "diff_test")
	require.Equal(t, replaceSQL, "REPLACE INTO `diff_test`.`atest`(`id`,`name`,`birthday`,`update_time`,`money`) VALUES (NULL,'a\\'a','2018-01-01 00:00:00','10:10:10',11.1111);")
	require.Equal(t, deleteSQL, "DELETE FROM `diff_test`.`atest` WHERE `id` is NULL AND `name` = 'a\\'a' AND `birthday` = '2018-01-01 00:00:00' AND `update_time` = '10:10:10' AND `money` = 11.1111 LIMIT 1;")
}

func TestResetColumns(t *testing.T) {
	createTableSQL1 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`))"
	tableInfo1, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)
	tbInfo, hasTimeStampType := ResetColumns(tableInfo1, []string{"a"})
	require.Equal(t, len(tbInfo.Columns), 3)
	require.Equal(t, len(tbInfo.Indices), 0)
	require.Equal(t, tbInfo.Columns[2].Offset, 2)
	require.False(t, hasTimeStampType)

	createTableSQL2 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)
	tbInfo, _ = ResetColumns(tableInfo2, []string{"a", "b"})
	require.Equal(t, len(tbInfo.Columns), 2)
	require.Equal(t, len(tbInfo.Indices), 0)

	createTableSQL3 := "CREATE TABLE `test`.`atest` (`a` int, `b` int, `c` int, `d` int, primary key(`a`), index idx(`b`, `c`))"
	tableInfo3, err := dbutiltest.GetTableInfoBySQL(createTableSQL3, parser.New())
	require.NoError(t, err)
	tbInfo, _ = ResetColumns(tableInfo3, []string{"b", "c"})
	require.Equal(t, len(tbInfo.Columns), 2)
	require.Equal(t, len(tbInfo.Indices), 1)
}

func TestGetTableSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()
	dataRows := sqlmock.NewRows([]string{"a", "b"})
	rowNums := 1000
	for k := 0; k < rowNums; k++ {
		str := fmt.Sprintf("%d", k)
		dataRows.AddRow(str, str)
	}
	sizeRows := sqlmock.NewRows([]string{"data"})
	sizeRows.AddRow("8000")
	mock.ExpectQuery("data").WillReturnRows(sizeRows)
	size, err := GetTableSize(ctx, conn, "test", "test")
	require.NoError(t, err)
	require.Equal(t, size, int64(8000))
}

func TestGetBetterIndex(t *testing.T) {
	ctx := context.Background()
	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()
	tableCases := []*tableCaseType{
		{
			schema:         "single_index",
			table:          "test1",
			createTableSQL: "CREATE TABLE `single_index`.`test1` (`a` int, `b` char, primary key(`a`), index(`b`))",
			rowColumns:     []string{"a", "b"},
			rows: [][]driver.Value{
				{"1", "a"},
				{"2", "a"},
				{"3", "b"},
				{"4", "b"},
				{"5", "c"},
				{"6", "c"},
				{"7", "d"},
				{"8", "d"},
				{"9", "e"},
				{"A", "e"},
				{"B", "f"},
				{"C", "f"},
			},
			indices:  []string{"PRIMARY", "b"},
			sels:     []float64{1.0, 0.5},
			selected: "PRIMARY",
		}, {
			schema:         "single_index",
			table:          "test1",
			createTableSQL: "CREATE TABLE `single_index`.`test1` (`a` int, `b` char, index(a), index(b))",
			rowColumns:     []string{"a", "b"},
			rows: [][]driver.Value{
				{"1", "a"},
				{"2", "a"},
				{"3", "b"},
				{"4", "b"},
				{"5", "c"},
				{"6", "c"},
				{"7", "d"},
				{"8", "d"},
				{"9", "e"},
				{"10", "e"},
				{"11", "f"},
				{"12", "f"},
			},
			indices:  []string{"a", "b"},
			sels:     []float64{1.0, 0.5},
			selected: "a",
		},
	}
	tableCase := tableCases[0]
	tableInfo, err := dbutiltest.GetTableInfoBySQL(tableCase.createTableSQL, parser.New())
	require.NoError(t, err)
	indices := dbutil.FindAllIndex(tableInfo)
	for i, index := range indices {
		require.Equal(t, index.Name.O, tableCase.indices[i])
	}
	for i, col := range tableCase.rowColumns {
		retRows := sqlmock.NewRows([]string{"SEL"})
		retRows.AddRow(tableCase.sels[i])
		mock.ExpectQuery("SELECT").WillReturnRows(retRows)
		sel, err := GetSelectivity(ctx, conn, tableCase.schema, tableCase.table, col, tableInfo)
		require.NoError(t, err)
		require.Equal(t, sel, tableCase.sels[i])
	}
	indices, err = GetBetterIndex(ctx, conn, "single_index", "test1", tableInfo)
	require.NoError(t, err)
	require.Equal(t, indices[0].Name.O, tableCase.selected)

	tableCase = tableCases[1]
	tableInfo, err = dbutiltest.GetTableInfoBySQL(tableCase.createTableSQL, parser.New())
	require.NoError(t, err)
	indices = dbutil.FindAllIndex(tableInfo)
	for i, index := range indices {
		require.Equal(t, index.Name.O, tableCase.indices[i])
	}
	for i, col := range tableCase.rowColumns {
		retRows := sqlmock.NewRows([]string{"SEL"})
		retRows.AddRow(tableCase.sels[i])
		mock.ExpectQuery("SELECT").WillReturnRows(retRows)
		sel, err := GetSelectivity(ctx, conn, tableCase.schema, tableCase.table, col, tableInfo)
		require.NoError(t, err)
		require.Equal(t, sel, tableCase.sels[i])
	}
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT `a.*").WillReturnRows(sqlmock.NewRows([]string{"SEL"}).AddRow("5"))
	mock.ExpectQuery("SELECT COUNT\\(DISTINCT `b.*").WillReturnRows(sqlmock.NewRows([]string{"SEL"}).AddRow("2"))
	indices, err = GetBetterIndex(ctx, conn, "single_index", "test1", tableInfo)
	require.NoError(t, err)
	require.Equal(t, indices[0].Name.O, tableCase.selected)
}

func TestCalculateChunkSize(t *testing.T) {
	require.Equal(t, CalculateChunkSize(1000), int64(50000))
	require.Equal(t, CalculateChunkSize(1000000000), int64(100000))
}

func TestGetSQLFileName(t *testing.T) {
	index := &chunk.CID{
		TableIndex:       1,
		BucketIndexLeft:  2,
		BucketIndexRight: 3,
		ChunkIndex:       4,
		ChunkCnt:         10,
	}
	require.Equal(t, GetSQLFileName(index), "1:2-3:4")
}

func TestGetCIDFromSQLFileName(t *testing.T) {
	tableIndex, bucketIndexLeft, bucketIndexRight, chunkIndex, err := GetCIDFromSQLFileName("11:12-13:14")
	require.NoError(t, err)
	require.Equal(t, tableIndex, 11)
	require.Equal(t, bucketIndexLeft, 12)
	require.Equal(t, bucketIndexRight, 13)
	require.Equal(t, chunkIndex, 14)
}

func TestCompareStruct(t *testing.T) {
	createTableSQL := "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`), index(`c`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	var isEqual bool
	var isPanic bool
	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo}, tableInfo)
	require.True(t, isEqual)
	require.False(t, isPanic)

	// column length different
	createTableSQL2 := "create table `test`(`a` int, `b` varchar(10), `c` float, primary key(`a`, `b`), index(`c`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo2}, tableInfo)
	require.False(t, isEqual)
	require.True(t, isPanic)

	// column name differernt
	createTableSQL2 = "create table `test`(`aa` int, `b` varchar(10), `c` float, `d` datetime, primary key(`aa`, `b`), index(`c`))"
	tableInfo2, err = dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo2}, tableInfo)
	require.False(t, isEqual)
	require.True(t, isPanic)

	// column type compatible
	createTableSQL2 = "create table `test`(`a` int, `b` char(10), `c` float, `d` datetime, primary key(`a`, `b`), index(`c`))"
	tableInfo2, err = dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo2}, tableInfo)
	require.True(t, isEqual)
	require.False(t, isPanic)

	createTableSQL2 = "create table `test`(`a` int(11), `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`), index(`c`))"
	tableInfo2, err = dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo2}, tableInfo)
	require.True(t, isEqual)
	require.False(t, isPanic)

	// column type not compatible
	createTableSQL2 = "create table `test`(`a` int, `b` varchar(10), `c` int, `d` datetime, primary key(`a`, `b`), index(`c`))"
	tableInfo2, err = dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo2}, tableInfo)
	require.False(t, isEqual)
	require.True(t, isPanic)

	// column properties not compatible
	createTableSQL2 = "create table `test`(`a` int, `b` varchar(11), `c` int, `d` datetime, primary key(`a`, `b`), index(`c`))"
	tableInfo2, err = dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo2}, tableInfo)
	require.False(t, isEqual)
	require.True(t, isPanic)

	// index check

	// index different
	createTableSQL2 = "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo2, err = dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo2}, tableInfo)
	require.False(t, isEqual)
	require.False(t, isPanic)
	require.Equal(t, len(tableInfo.Indices), 1)
	require.Equal(t, tableInfo.Indices[0].Name.O, "PRIMARY")

	// index column different
	createTableSQL = "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`), index(`c`))"
	tableInfo, err = dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	createTableSQL2 = "create table `test`.`test`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `c`), index(`c`))"
	tableInfo2, err = dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	isEqual, isPanic = CompareStruct([]*model.TableInfo{tableInfo, tableInfo2}, tableInfo)
	require.False(t, isEqual)
	require.False(t, isPanic)
	require.Equal(t, len(tableInfo.Indices), 1)
	require.Equal(t, tableInfo.Indices[0].Name.O, "c")
}

func TestGenerateSQLBlob(t *testing.T) {
	rowsData := map[string]*dbutil.ColumnData{
		"id": {Data: []byte("1"), IsNull: false},
		"b":  {Data: []byte("foo"), IsNull: false},
	}

	cases := []struct {
		createTableSQL string
	}{
		{"CREATE TABLE `diff_test`.`atest` (`id` int primary key, `b` tinyblob)"},
		{"CREATE TABLE `diff_test`.`atest` (`id` int primary key, `b` blob)"},
		{"CREATE TABLE `diff_test`.`atest` (`id` int primary key, `b` mediumblob)"},
		{"CREATE TABLE `diff_test`.`atest` (`id` int primary key, `b` longblob)"},
	}

	for _, c := range cases {
		tableInfo, err := dbutiltest.GetTableInfoBySQL(c.createTableSQL, parser.New())
		require.NoError(t, err)

		replaceSQL := GenerateReplaceDML(rowsData, tableInfo, "diff_test")
		deleteSQL := GenerateDeleteDML(rowsData, tableInfo, "diff_test")
		require.Equal(t, replaceSQL, "REPLACE INTO `diff_test`.`atest`(`id`,`b`) VALUES (1,x'666f6f');")
		require.Equal(t, deleteSQL, "DELETE FROM `diff_test`.`atest` WHERE `id` = 1 AND `b` = x'666f6f' LIMIT 1;")
	}
}

func TestCompareBlob(t *testing.T) {
	createTableSQL := "create table `test`.`test`(`a` int primary key, `b` blob)"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	_, orderKeyCols := GetTableRowsQueryFormat("test", "test", tableInfo, "123")

	data1 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: false},
		"b": {Data: []byte{0xff, 0xfe}, IsNull: false},
	}
	data2 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: false},
		"b": {Data: []byte{0xfe, 0xff}, IsNull: false},
	}
	data3 := map[string]*dbutil.ColumnData{
		"a": {Data: []byte("1"), IsNull: false},
		"b": {Data: []byte("foobar"), IsNull: false},
	}

	columns := tableInfo.Columns

	cases := []struct {
		data1      map[string]*dbutil.ColumnData
		dataOthers []map[string]*dbutil.ColumnData
	}{
		{data1, []map[string]*dbutil.ColumnData{data2, data3}},
		{data2, []map[string]*dbutil.ColumnData{data1, data3}},
		{data3, []map[string]*dbutil.ColumnData{data1, data2}},
	}

	for _, c := range cases {
		equal, cmp, err := CompareData(c.data1, c.data1, orderKeyCols, columns)
		require.NoError(t, err)
		require.Equal(t, cmp, int32(0))
		require.True(t, equal)

		for _, data := range c.dataOthers {
			equal, cmp, err = CompareData(c.data1, data, orderKeyCols, columns)
			require.NoError(t, err)
			require.Equal(t, cmp, int32(0))
			require.False(t, equal)
		}
	}
}
