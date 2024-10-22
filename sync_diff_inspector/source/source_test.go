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

package source

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

type tableCaseType struct {
	schema         string
	table          string
	createTableSQL string
	rangeColumns   []string
	rangeLeft      []string
	rangeRight     []string
	rangeInfo      *splitter.RangeInfo
	rowQuery       string
	rowColumns     []string
	rows           [][]driver.Value
}

type MockChunkIterator struct {
	ctx       context.Context
	tableDiff *common.TableDiff
	rangeInfo *splitter.RangeInfo
	index     *chunk.CID
}

const (
	CHUNKS  = 5
	BUCKETS = 1
)

func (m *MockChunkIterator) Next() (*chunk.Range, error) {
	if m.index.ChunkIndex == m.index.ChunkCnt-1 {
		return nil, nil
	}
	m.index.ChunkIndex = m.index.ChunkIndex + 1
	return &chunk.Range{
		Index: &chunk.CID{
			TableIndex:       m.index.TableIndex,
			BucketIndexLeft:  m.index.BucketIndexLeft,
			BucketIndexRight: m.index.BucketIndexRight,
			ChunkIndex:       m.index.ChunkIndex,
			ChunkCnt:         m.index.ChunkCnt,
		},
	}, nil
}

func (m *MockChunkIterator) Close() {
}

type MockAnalyzer struct{}

func (m *MockAnalyzer) AnalyzeSplitter(ctx context.Context, tableDiff *common.TableDiff, rangeInfo *splitter.RangeInfo) (splitter.ChunkIterator, error) {
	i := &chunk.CID{
		TableIndex:       0,
		BucketIndexLeft:  0,
		BucketIndexRight: 0,
		ChunkIndex:       -1,
		ChunkCnt:         CHUNKS,
	}
	return &MockChunkIterator{
		ctx,
		tableDiff,
		rangeInfo,
		i,
	}, nil
}

func TestTiDBSource(t *testing.T) {
	ctx := context.Background()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, `d` binary(1), `e` varbinary(1), PRIMARY KEY(`a`)\n)",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT",
			rowColumns:     []string{"a", "b", "c", "d", "e"},
			rows: [][]driver.Value{
				{"1", "a", "1.2", []byte{0xaa}, []byte{0xaa}},
				{"2", "b", "3.4", []byte{0xbb}, []byte{0xbb}},
				{"3", "c", "5.6", []byte{0xcc}, []byte{0xcc}},
				{"4", "d", "6.7", []byte{0xdd}, []byte{0xdd}},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, PRIMARY KEY(`a`)\n)",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(t, tableCases)

	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("source_test"))
	mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(sqlmock.NewRows([]string{"Table", "type"}).AddRow("test1", "base").AddRow("test2", "base"))
	mock.ExpectQuery("SELECT version()*").WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow("5.7.25-TiDB-v4.0.12"))

	f, err := filter.Parse([]string{"source_test.*"})
	require.NoError(t, err)
	tidb, err := NewTiDBSource(ctx, tableDiffs, &config.DataSource{Conn: conn}, utils.NewWorkerPool(1, "bucketIter"), f, false)
	require.NoError(t, err)

	caseFn := []struct {
		check func(sqlmock.Sqlmock, Source) (bool, error)
	}{
		{
			check: func(mock sqlmock.Sqlmock, source Source) (bool, error) {
				mock.ExpectQuery("SHOW CREATE TABLE*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tableCases[0].table, tableCases[0].createTableSQL))
				mock.ExpectQuery("SELECT _tidb_rowid FROM*").WillReturnRows(sqlmock.NewRows([]string{"_tidb_rowid"}))
				mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'*").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"))
				tableInfo, err := source.GetSourceStructInfo(ctx, 0)
				if err != nil {
					return false, err
				}
				return !tableInfo[0].PKIsHandle, nil
			},
		},
		{
			check: func(mock sqlmock.Sqlmock, source Source) (bool, error) {
				mock.ExpectQuery("SHOW CREATE TABLE*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tableCases[1].table, tableCases[1].createTableSQL))
				mock.ExpectQuery("SELECT _tidb_rowid FROM*").WillReturnError(fmt.Errorf("ERROR 1054 (42S22): Unknown column '_tidb_rowid' in 'field list'"))
				mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'*").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"))
				tableInfo, err := source.GetSourceStructInfo(ctx, 0)
				if err != nil {
					return false, err
				}
				return tableInfo[0].PKIsHandle, nil
			},
		},
	}

	for n, tableCase := range tableCases {
		t.Log(n)
		check, err := caseFn[n].check(mock, tidb)
		require.NoError(t, err)
		require.True(t, check)
		require.Equal(t, n, tableCase.rangeInfo.GetTableIndex())
		countRows := sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(123, 456)
		mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
		checksum := tidb.GetCountAndMD5(ctx, tableCase.rangeInfo)
		require.NoError(t, checksum.Err)
		require.Equal(t, checksum.Count, int64(123))
		require.Equal(t, checksum.Checksum, uint64(456))
	}

	// Test ChunkIterator
	iter, err := tidb.GetRangeIterator(ctx, tableCases[0].rangeInfo, &MockAnalyzer{}, 3)
	require.NoError(t, err)
	resRecords := [][]bool{
		{false, false, false, false, false},
		{false, false, false, false, false},
	}
	for {
		ch, err := iter.Next(ctx)
		require.NoError(t, err)
		if ch == nil {
			break
		}
		require.Equal(t, ch.ChunkRange.Index.ChunkCnt, 5)
		require.Equal(t, resRecords[ch.ChunkRange.Index.TableIndex][ch.ChunkRange.Index.ChunkIndex], false)
		resRecords[ch.ChunkRange.Index.TableIndex][ch.ChunkRange.Index.ChunkIndex] = true
	}
	iter.Close()
	require.Equal(t, resRecords, [][]bool{
		{true, true, true, true, true},
		{true, true, true, true, true},
	})

	// Test RowIterator
	tableCase := tableCases[0]
	dataRows := sqlmock.NewRows(tableCase.rowColumns)
	for _, row := range tableCase.rows {
		dataRows.AddRow(row...)
	}
	mock.ExpectQuery(tableCase.rowQuery).WillReturnRows(dataRows)
	rowIter, err := tidb.GetRowsIterator(ctx, tableCase.rangeInfo)
	require.NoError(t, err)

	row := 0
	var firstRow, secondRow map[string]*dbutil.ColumnData
	for {
		columns, err := rowIter.Next()
		require.NoError(t, err)
		if columns == nil {
			require.Equal(t, row, len(tableCase.rows))
			break
		}
		for j, value := range tableCase.rows[row] {
			require.Equal(t, columns[tableCase.rowColumns[j]].IsNull, false)
			if _, ok := value.(string); ok {
				require.Equal(t, columns[tableCase.rowColumns[j]].Data, []byte(value.(string)))
			}
		}
		if row == 0 {
			firstRow = columns
		} else if row == 1 {
			secondRow = columns
		}
		row++
	}
	require.Equal(t, tidb.GenerateFixSQL(Insert, firstRow, secondRow, 0), "REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`,`d`,`e`) VALUES (1,'a',1.2,x'aa',x'aa');")
	require.Equal(t, tidb.GenerateFixSQL(Delete, firstRow, secondRow, 0), "DELETE FROM `source_test`.`test1` WHERE `a` = 2 AND `b` = 'b' AND `c` = 3.4 AND `d` = x'bb' AND `e` = x'bb' LIMIT 1;")
	require.Equal(t, tidb.GenerateFixSQL(Replace, firstRow, secondRow, 0),
		"/*\n"+
			"  DIFF COLUMNS ╏ `A` ╏ `B` ╏ `C` ╏  `D`  ╏  `E`   \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍╍╋╍╍╍╍╍╍╍╍\n"+
			"  source data  ╏ 1   ╏ 'a' ╏ 1.2 ╏ x'aa' ╏ x'aa'  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍╍╋╍╍╍╍╍╍╍╍\n"+
			"  target data  ╏ 2   ╏ 'b' ╏ 3.4 ╏ x'aa' ╏ x'aa'  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍╍╋╍╍╍╍╍╍╍╍\n"+
			"*/\n"+
			"REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`,`d`,`e`) VALUES (1,'a',1.2,x'aa',x'aa');")

	rowIter.Close()

	analyze := tidb.GetTableAnalyzer()
	countRows := sqlmock.NewRows([]string{"Cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	chunkIter, err := analyze.AnalyzeSplitter(ctx, tableDiffs[0], tableCase.rangeInfo)
	require.NoError(t, err)
	chunkIter.Close()
	tidb.Close()
}

func TestFallbackToRandomIfRangeIsSet(t *testing.T) {
	ctx := context.Background()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("source_test"))
	mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(sqlmock.NewRows([]string{"Table", "type"}).AddRow("test1", "base"))
	statsRows := sqlmock.NewRows([]string{"Db_name", "Table_name", "Column_name", "Is_index", "Bucket_id", "Count", "Repeats", "Lower_Bound", "Upper_Bound"})
	for i := 0; i < 5; i++ {
		statsRows.AddRow("source_test", "test1", "PRIMARY", 1, (i+1)*64, (i+1)*64, 1,
			fmt.Sprintf("(%d, %d)", i*64, i*12), fmt.Sprintf("(%d, %d)", (i+1)*64-1, (i+1)*12-1))
	}
	mock.ExpectQuery("SELECT version()*").WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow("5.7.25-TiDB-v4.0.12"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) cnt")).WillReturnRows(sqlmock.NewRows([]string{"cnt"}).AddRow(100))

	f, err := filter.Parse([]string{"source_test.*"})
	require.NoError(t, err)

	createTableSQL1 := "CREATE TABLE `test1` " +
		"(`id` int(11) NOT NULL AUTO_INCREMENT, " +
		" `k` int(11) NOT NULL DEFAULT '0', " +
		"`c` char(120) NOT NULL DEFAULT '', " +
		"PRIMARY KEY (`id`), KEY `k_1` (`k`))"

	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)

	table1 := &common.TableDiff{
		Schema: "source_test",
		Table:  "test1",
		Info:   tableInfo,
		Range:  "id < 10", // This should prevent using BucketIterator
	}

	tidb, err := NewTiDBSource(ctx, []*common.TableDiff{table1}, &config.DataSource{Conn: conn}, utils.NewWorkerPool(1, "bucketIter"), f, false)
	require.NoError(t, err)

	analyze := tidb.GetTableAnalyzer()
	chunkIter, err := analyze.AnalyzeSplitter(ctx, table1, nil)
	require.NoError(t, err)
	require.IsType(t, &splitter.RandomIterator{}, chunkIter)

	chunkIter.Close()
	tidb.Close()
}

func TestMysqlShardSources(t *testing.T) {
	ctx := context.Background()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT.*",
			rowColumns:     []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"1", "a", "1.2"},
				{"2", "b", "2.2"},
				{"3", "c", "3.2"},
				{"4", "d", "4.2"},
				{"5", "e", "5.2"},
				{"6", "f", "6.2"},
				{"7", "g", "7.2"},
				{"8", "h", "8.2"},
				{"9", "i", "9.2"},
				{"10", "j", "10.2"},
				{"11", "k", "11.2"},
				{"12", "l", "12.2"},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(t, tableCases)

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	dbs := []*sql.DB{
		conn, conn, conn, conn,
	}

	cs := make([]*config.DataSource, 4)
	for i := range dbs {
		mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("source_test"))
		mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(sqlmock.NewRows([]string{"Table", "type"}).AddRow("test1", "base").AddRow("test2", "base"))
		cs[i] = &config.DataSource{Conn: conn}
	}

	f, err := filter.Parse([]string{"source_test.*"})
	require.NoError(t, err)
	shard, err := NewMySQLSources(ctx, tableDiffs, cs, 4, f, false)
	require.NoError(t, err)

	for i := 0; i < len(dbs); i++ {
		infoRows := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test_t", "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))")
		variableRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")

		mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(infoRows)
		mock.ExpectQuery("SHOW VARIABLE.*").WillReturnRows(variableRows)
	}
	info, err := shard.GetSourceStructInfo(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, info[0].Name.O, "test1")

	for n, tableCase := range tableCases {
		require.Equal(t, n, tableCase.rangeInfo.GetTableIndex())
		var resChecksum uint64 = 0
		for i := 0; i < len(dbs); i++ {
			resChecksum = resChecksum + 1<<i
			countRows := sqlmock.NewRows([]string{"CNT", "CHECKSUM"}).AddRow(1, 1<<i)
			mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
		}

		checksum := shard.GetCountAndMD5(ctx, tableCase.rangeInfo)
		require.NoError(t, checksum.Err)
		require.Equal(t, checksum.Count, int64(len(dbs)))
		require.Equal(t, checksum.Checksum, resChecksum)
	}

	// Test RowIterator
	tableCase := tableCases[0]
	rowNums := len(tableCase.rows) / len(dbs)
	i := 0
	for j := 0; j < len(dbs); j++ {
		dataRows := sqlmock.NewRows(tableCase.rowColumns)
		for k := 0; k < rowNums; k++ {
			dataRows.AddRow(tableCase.rows[i]...)
			i++
		}
		mock.ExpectQuery(tableCase.rowQuery).WillReturnRows(dataRows)
	}

	rowIter, err := shard.GetRowsIterator(ctx, tableCase.rangeInfo)
	require.NoError(t, err)

	i = 0
	for {
		columns, err := rowIter.Next()
		require.NoError(t, err)
		if columns == nil {
			require.Equal(t, i, len(tableCase.rows))
			break
		}
		for j, value := range tableCase.rows[i] {
			// c.Log(j)
			require.Equal(t, columns[tableCase.rowColumns[j]].IsNull, false)
			require.Equal(t, columns[tableCase.rowColumns[j]].Data, []byte(value.(string)))
		}

		i++
	}
	rowIter.Close()

	shard.Close()
}

func TestMysqlRouter(t *testing.T) {
	ctx := context.Background()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT",
			rowColumns:     []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"1", "a", "1.2"},
				{"2", "b", "3.4"},
				{"3", "c", "5.6"},
				{"4", "d", "6.7"},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(t, tableCases)

	routeRuleList := []*router.TableRule{
		{
			SchemaPattern: "source_test_t",
			TablePattern:  "test_t",
			TargetSchema:  "source_test",
			TargetTable:   "test1",
		},
	}
	router, err := router.NewTableRouter(false, routeRuleList)
	require.NoError(t, err)
	ds := &config.DataSource{
		Router: router,
		Conn:   conn,
	}

	databasesRows := sqlmock.NewRows([]string{"Database"}).AddRow("source_test").AddRow("source_test_t")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(databasesRows)
	tablesRows := sqlmock.NewRows([]string{"Tables_in_test", "Table_type"}).AddRow("test2", "BASE TABLE")
	mock.ExpectQuery("SHOW FULL TABLES IN.*").WillReturnRows(tablesRows)
	tablesRows = sqlmock.NewRows([]string{"Tables_in_test", "Table_type"}).AddRow("test_t", "BASE TABLE")
	mock.ExpectQuery("SHOW FULL TABLES IN.*").WillReturnRows(tablesRows)

	f, err := filter.Parse([]string{"*.*"})
	require.NoError(t, err)
	mysql, err := NewMySQLSources(ctx, tableDiffs, []*config.DataSource{ds}, 4, f, false)
	require.NoError(t, err)

	// random splitter
	// query 1: SELECT COUNT(1) cnt FROM `source_test`.`test2`
	countRows := sqlmock.NewRows([]string{"Cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	// query 2: SELECT COUNT(1) cnt FROM `source_test_t`.`test_t`
	countRows = sqlmock.NewRows([]string{"Cnt"}).AddRow(0)
	mock.ExpectQuery("SELECT COUNT.*").WillReturnRows(countRows)
	rangeIter, err := mysql.GetRangeIterator(ctx, nil, mysql.GetTableAnalyzer(), 3)
	require.NoError(t, err)
	_, err = rangeIter.Next(ctx)
	require.NoError(t, err)
	rangeIter.Close()

	rangeIter, err = mysql.GetRangeIterator(ctx, tableCases[0].rangeInfo, mysql.GetTableAnalyzer(), 3)
	require.NoError(t, err)
	rangeIter.Close()

	// row Iterator
	dataRows := sqlmock.NewRows(tableCases[0].rowColumns)
	for k := 0; k < 2; k++ {
		dataRows.AddRow(tableCases[0].rows[k]...)
	}
	mock.ExpectQuery(tableCases[0].rowQuery).WillReturnRows(dataRows)

	rowIter, err := mysql.GetRowsIterator(ctx, tableCases[0].rangeInfo)
	require.NoError(t, err)
	firstRow, err := rowIter.Next()
	require.NoError(t, err)
	require.NotNil(t, firstRow)
	secondRow, err := rowIter.Next()
	require.NoError(t, err)
	require.NotNil(t, secondRow)
	require.Equal(t, mysql.GenerateFixSQL(Insert, firstRow, secondRow, 0), "REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")
	require.Equal(t, mysql.GenerateFixSQL(Delete, firstRow, secondRow, 0), "DELETE FROM `source_test`.`test1` WHERE `a` = 2 AND `b` = 'b' AND `c` = 3.4 LIMIT 1;")
	require.Equal(t, mysql.GenerateFixSQL(Replace, firstRow, secondRow, 0),
		"/*\n"+
			"  DIFF COLUMNS ╏ `A` ╏ `B` ╏ `C`  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"  source data  ╏ 1   ╏ 'a' ╏ 1.2  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"  target data  ╏ 2   ╏ 'b' ╏ 3.4  \n"+
			"╍╍╍╍╍╍╍╍╍╍╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╋╍╍╍╍╍╍\n"+
			"*/\n"+
			"REPLACE INTO `source_test`.`test1`(`a`,`b`,`c`) VALUES (1,'a',1.2);")
	rowIter.Close()

	mysql.Close()
}

func TestTiDBRouter(t *testing.T) {
	ctx := context.Background()

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	tableCases := []*tableCaseType{
		{
			schema:         "source_test",
			table:          "test1",
			createTableSQL: "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
			rowQuery:       "SELECT",
			rowColumns:     []string{"a", "b", "c"},
			rows: [][]driver.Value{
				{"1", "a", "1.2"},
				{"2", "b", "3.4"},
				{"3", "c", "5.6"},
				{"4", "d", "6.7"},
			},
		},
		{
			schema:         "source_test",
			table:          "test2",
			createTableSQL: "CREATE TABLE `source_test`.`test2` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))",
			rangeColumns:   []string{"a", "b"},
			rangeLeft:      []string{"3", "b"},
			rangeRight:     []string{"5", "f"},
		},
	}

	tableDiffs := prepareTiDBTables(t, tableCases)

	routeRuleList := []*router.TableRule{
		{
			SchemaPattern: "source_test_t",
			TablePattern:  "test_t",
			TargetSchema:  "source_test",
			TargetTable:   "test1",
		},
	}
	router, err := router.NewTableRouter(false, routeRuleList)
	require.NoError(t, err)
	ds := &config.DataSource{
		Router: router,
		Conn:   conn,
	}

	databasesRows := sqlmock.NewRows([]string{"Database"}).AddRow("source_test_t").AddRow("source_test")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(databasesRows)
	tablesRows := sqlmock.NewRows([]string{"Tables_in_test", "Table_type"}).AddRow("test_t", "BASE TABLE")
	mock.ExpectQuery("SHOW FULL TABLES IN.*").WillReturnRows(tablesRows)
	tablesRows = sqlmock.NewRows([]string{"Tables_in_test", "Table_type"}).AddRow("test2", "BASE TABLE")
	mock.ExpectQuery("SHOW FULL TABLES IN.*").WillReturnRows(tablesRows)
	mock.ExpectQuery("SELECT version()*").WillReturnRows(sqlmock.NewRows([]string{"version()"}).AddRow("5.7.25-TiDB-v4.0.12"))

	f, err := filter.Parse([]string{"*.*"})
	require.NoError(t, err)
	tidb, err := NewTiDBSource(ctx, tableDiffs, ds, utils.NewWorkerPool(1, "bucketIter"), f, false)
	require.NoError(t, err)
	infoRows := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test_t", "CREATE TABLE `source_test`.`test1` (`a` int, `b` varchar(24), `c` float, primary key(`a`, `b`))")
	mock.ExpectQuery("SHOW CREATE TABLE.*").WillReturnRows(infoRows)
	variableRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION")
	mock.ExpectQuery("SHOW VARIABLE.*").WillReturnRows(variableRows)
	info, err := tidb.GetSourceStructInfo(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, info[0].Name.O, "test1")
}

func prepareTiDBTables(t *testing.T, tableCases []*tableCaseType) []*common.TableDiff {
	tableDiffs := make([]*common.TableDiff, 0, len(tableCases))
	for n, tableCase := range tableCases {
		tableInfo, err := dbutiltest.GetTableInfoBySQL(tableCase.createTableSQL, parser.New())
		require.NoError(t, err)
		tableDiffs = append(tableDiffs, &common.TableDiff{
			Schema: "source_test",
			Table:  fmt.Sprintf("test%d", n+1),
			Info:   tableInfo,
		})

		chunkRange := chunk.NewChunkRange()
		for i, column := range tableCase.rangeColumns {
			chunkRange.Update(column, tableCase.rangeLeft[i], tableCase.rangeRight[i], true, true)
		}

		chunk.InitChunk(chunkRange, chunk.Bucket, 0, 0, "", "")
		chunkRange.Index.TableIndex = n
		rangeInfo := &splitter.RangeInfo{
			ChunkRange: chunkRange,
		}
		tableCase.rangeInfo = rangeInfo
	}

	return tableDiffs
}

func TestSource(t *testing.T) {
	host, isExist := os.LookupEnv("MYSQL_HOST")
	if host == "" || !isExist {
		return
	}
	portstr, isExist := os.LookupEnv("MYSQL_PORT")
	if portstr == "" || !isExist {
		return
	}

	port, err := strconv.Atoi(portstr)
	require.NoError(t, err)

	ctx := context.Background()

	router, err := router.NewTableRouter(false, nil)
	require.NoError(t, err)
	cfg := &config.Config{
		LogLevel:         "debug",
		CheckThreadCount: 4,
		ExportFixSQL:     true,
		CheckStructOnly:  false,
		DataSources: map[string]*config.DataSource{
			"mysql1": {
				Host: host,
				Port: port,
				User: "root",
			},
			"tidb": {
				Host: host,
				Port: port,
				User: "root",
			},
		},
		Routes: nil,
		TableConfigs: map[string]*config.TableConfig{
			"config1": {
				Schema:          "schama1",
				Table:           "tbl",
				IgnoreColumns:   []string{"", ""},
				Fields:          []string{""},
				Range:           "a > 10 AND a < 20",
				TargetTableInfo: nil,
				Collation:       "",
			},
		},
		Task: config.TaskConfig{
			Source:       []string{"mysql1"},
			Routes:       nil,
			Target:       "tidb",
			CheckTables:  []string{"schema*.tbl"},
			TableConfigs: []string{"config1"},
			OutputDir:    "./output",
			SourceInstances: []*config.DataSource{
				{
					Host:   host,
					Port:   port,
					User:   "root",
					Router: router,
				},
			},
			TargetInstance: &config.DataSource{
				Host: host,
				Port: port,
				User: "root",
			},
			TargetTableConfigs: []*config.TableConfig{
				{
					Schema:          "schema1",
					Table:           "tbl",
					IgnoreColumns:   []string{"", ""},
					Fields:          []string{""},
					Range:           "a > 10 AND a < 20",
					TargetTableInfo: nil,
					Collation:       "",
				},
			},
			TargetCheckTables: nil,
			FixDir:            "output/fix-on-tidb0",
			CheckpointDir:     "output/checkpoint",
			HashFile:          "",
		},
		ConfigFile:   "config.toml",
		PrintVersion: false,
	}
	cfg.Task.TargetCheckTables, err = filter.Parse([]string{"schema*.tbl"})
	require.NoError(t, err)

	// create table
	conn, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(%s:%d)/?charset=utf8mb4", host, port))
	require.NoError(t, err)

	conn.Exec("CREATE DATABASE IF NOT EXISTS schema1")
	conn.Exec("CREATE TABLE IF NOT EXISTS `schema1`.`tbl` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))")
	// create db connections refused.
	// TODO unit_test covers source.go
	_, _, err = NewSources(ctx, cfg)
	require.NoError(t, err)
}

func TestRouterRules(t *testing.T) {
	host, isExist := os.LookupEnv("MYSQL_HOST")
	if host == "" || !isExist {
		return
	}
	portStr, isExist := os.LookupEnv("MYSQL_PORT")
	if portStr == "" || !isExist {
		// return
	}
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	ctx := context.Background()

	r, _ := router.NewTableRouter(
		false,
		[]*router.TableRule{
			// make sure this rule works
			{
				SchemaPattern: "schema1",
				TablePattern:  "tbl",
				TargetSchema:  "schema2",
				TargetTable:   "tbl",
			},
		})
	cfg := &config.Config{
		LogLevel:         "debug",
		CheckThreadCount: 4,
		ExportFixSQL:     true,
		CheckStructOnly:  false,
		DataSources: map[string]*config.DataSource{
			"mysql1": {
				Host: host,
				Port: port,
				User: "root",
			},
			"tidb": {
				Host: host,
				Port: port,
				User: "root",
			},
		},
		Routes: nil,
		Task: config.TaskConfig{
			Source:      []string{"mysql1"},
			Routes:      nil,
			Target:      "tidb",
			CheckTables: []string{"schema2.tbl"},
			OutputDir:   "./output",
			SourceInstances: []*config.DataSource{
				{
					Host:           host,
					Port:           port,
					User:           "root",
					Router:         r,
					RouteTargetSet: make(map[string]struct{}),
				},
			},
			TargetInstance: &config.DataSource{
				Host: host,
				Port: port,
				User: "root",
			},
			TargetCheckTables: nil,
			FixDir:            "output/fix-on-tidb0",
			CheckpointDir:     "output/checkpoint",
			HashFile:          "",
		},
		ConfigFile:   "config.toml",
		PrintVersion: false,
	}
	cfg.Task.TargetCheckTables, err = filter.Parse([]string{"schema2.tbl", "schema_test.tbl"})
	require.NoError(t, err)
	cfg.Task.SourceInstances[0].RouteTargetSet[dbutil.TableName("schema2", "tbl")] = struct{}{}

	// create table
	conn, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(%s:%d)/?charset=utf8mb4", host, port))
	require.NoError(t, err)

	conn.Exec("CREATE DATABASE IF NOT EXISTS schema1")
	conn.Exec("CREATE TABLE IF NOT EXISTS `schema1`.`tbl` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))")
	conn.Exec("CREATE DATABASE IF NOT EXISTS schema2")
	conn.Exec("CREATE TABLE IF NOT EXISTS `schema2`.`tbl` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))")
	conn.Exec("CREATE DATABASE IF NOT EXISTS schema_test")
	conn.Exec("CREATE TABLE IF NOT EXISTS `schema_test`.`tbl` (`a` int, `b` varchar(24), `c` float, `d` datetime, primary key(`a`, `b`))")

	_, _, err = NewSources(ctx, cfg)
	require.NoError(t, err)

	require.Equal(t, 1, len(cfg.Task.SourceInstances))
	targetSchema, targetTable, err := cfg.Task.SourceInstances[0].Router.Route("schema1", "tbl")
	require.NoError(t, err)
	require.Equal(t, "schema2", targetSchema)
	require.Equal(t, "tbl", targetTable)
	targetSchema, targetTable, err = cfg.Task.SourceInstances[0].Router.Route("schema2", "tbl")
	require.NoError(t, err)
	require.Equal(t, shieldDBName, targetSchema)
	require.Equal(t, shieldTableName, targetTable)
	targetSchema, targetTable, err = cfg.Task.SourceInstances[0].Router.Route("schema_test", "tbl")
	require.NoError(t, err)
	require.Equal(t, "schema_test", targetSchema)
	require.Equal(t, "tbl", targetTable)
	_, tableRules := cfg.Task.SourceInstances[0].Router.AllRules()
	require.Equal(t, 1, len(tableRules["schema1"]))
	require.Equal(t, 1, len(tableRules["schema2"]))
	require.Equal(t, 1, len(tableRules["schema_test"]))
}

func TestInitTables(t *testing.T) {
	ctx := context.Background()
	cfg := config.NewConfig()
	// Test case 1: test2.t2 will parse after filter.
	require.NoError(t, cfg.Parse([]string{"--config", "../config/config.toml"}))
	require.NoError(t, cfg.Init())

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	cfg.Task.TargetInstance.Conn = conn

	rows := sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("test2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("t1", "t1").AddRow("t2", "t2")
	mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("t2", "CREATE TABLE `t2` (\n\t\t\t`id` int(11) DEFAULT NULL,\n\t\t  \t`name` varchar(24) DEFAULT NULL\n\t\t\t) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
	mock.ExpectQuery("SHOW CREATE TABLE *").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("", "")
	mock.ExpectQuery("SHOW VARIABLES LIKE*").WillReturnRows(rows)

	tablesToBeCheck, err := initTables(ctx, cfg)
	require.NoError(t, err)

	require.Len(t, tablesToBeCheck, 1)
	require.Equal(t, tablesToBeCheck[0].Schema, "test2")
	require.Equal(t, tablesToBeCheck[0].Table, "t2")
	// Range can be replaced during initTables
	require.Equal(t, tablesToBeCheck[0].Range, "age > 10 AND age < 20")

	require.NoError(t, mock.ExpectationsWereMet())

	// Test case 2: init failed due to conflict table config point to one table.
	cfg = config.NewConfig()
	require.NoError(t, cfg.Parse([]string{"--config", "../config/config_conflict.toml"}))
	require.NoError(t, cfg.Init())
	cfg.Task.TargetInstance.Conn = conn

	rows = sqlmock.NewRows([]string{"Database"}).AddRow("mysql").AddRow("test2")
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("t1", "t1").AddRow("t2", "t2")
	mock.ExpectQuery("SHOW FULL TABLES*").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("t2", "CREATE TABLE `t2` (\n\t\t\t`id` int(11) DEFAULT NULL,\n\t\t  \t`name` varchar(24) DEFAULT NULL\n\t\t\t) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
	mock.ExpectQuery("SHOW CREATE TABLE *").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"col1", "col2"}).AddRow("", "")
	mock.ExpectQuery("SHOW VARIABLES LIKE*").WillReturnRows(rows)

	_, err = initTables(ctx, cfg)
	require.Contains(t, err.Error(), "different config matched to same target table")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckTableMatched(t *testing.T) {
	var tableDiffs []*common.TableDiff
	tableDiffs = append(tableDiffs, &common.TableDiff{
		Schema: "test",
		Table:  "t1",
	})
	tableDiffs = append(tableDiffs, &common.TableDiff{
		Schema: "test",
		Table:  "t2",
	})

	tmap := make(map[string]struct{})
	smap := make(map[string]struct{})

	smap["`test`.`t1`"] = struct{}{}
	smap["`test`.`t2`"] = struct{}{}

	tmap["`test`.`t1`"] = struct{}{}
	tmap["`test`.`t2`"] = struct{}{}

	_, err := checkTableMatched(tableDiffs, tmap, smap, false)
	require.NoError(t, err)

	smap["`test`.`t3`"] = struct{}{}
	_, err = checkTableMatched(tableDiffs, tmap, smap, false)
	require.Contains(t, err.Error(), "the target has no table to be compared. source-table is ``test`.`t3``")

	delete(smap, "`test`.`t2`")
	_, err = checkTableMatched(tableDiffs, tmap, smap, false)
	require.Contains(t, err.Error(), "the source has no table to be compared. target-table is ``test`.`t2``")

	tables, err := checkTableMatched(tableDiffs, tmap, smap, true)
	require.NoError(t, err)
	require.Equal(t, 0, tables[0].TableLack)
	require.Equal(t, 1, tables[1].TableLack)
	require.Equal(t, -1, tables[2].TableLack)
}
