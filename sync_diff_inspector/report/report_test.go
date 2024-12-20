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

package report

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/stretchr/testify/require"
)

var task *config.TaskConfig = &config.TaskConfig{
	OutputDir:     "output_dir",
	FixDir:        "output_dir/123456/fix-on-tidb1",
	CheckpointDir: "output_dir/123456/checkpoint",
}

func TestReport(t *testing.T) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	report := NewReport(task)
	createTableSQL1 := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo1, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)
	createTableSQL2 := "create table `atest`.`atbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo1,
			Collation: "[123]",
		},
		{
			Schema:    "atest",
			Table:     "atbl",
			Info:      tableInfo2,
			Collation: "[123]",
		},
		{
			Schema:    "ctest",
			Table:     "atbl",
			Info:      tableInfo2,
			Collation: "[123]",
		},
		{
			Schema:    "dtest",
			Table:     "atbl",
			Info:      tableInfo2,
			Collation: "[123]",
		},
	}
	configs := []*Config{
		{
			Host: "127.0.0.1",
			Port: 3306,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 3307,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 4000,
			User: "root",
		},
	}

	configsBytes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		buf := new(bytes.Buffer)
		err := toml.NewEncoder(buf).Encode(configs[i])
		require.NoError(t, err)
		configsBytes[i] = buf.Bytes()
	}
	report.Init(tableDiffs, configsBytes[:2], configsBytes[2])

	// Test CalculateTotal
	mock.ExpectQuery("select sum.*").WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow("123"))
	mock.ExpectQuery("select sum.*where table_schema=.*").WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow("456"))
	report.CalculateTotalSize(ctx, db)

	// Test Table Report
	report.SetTableStructCheckResult("test", "tbl", true, false, common.AllTableExistFlag)
	report.SetTableDataCheckResult("test", "tbl", true, 100, 200, 222, 222, &chunk.CID{1, 1, 1, 1, 2})
	report.SetTableMeetError("test", "tbl", errors.New("eeee"))

	newReport := NewReport(task)
	newReport.LoadReport(report)

	require.Equal(t, newReport.TotalSize, int64(579))
	result, ok := newReport.TableResults["test"]["tbl"]
	require.True(t, ok)
	require.Equal(t, result.MeetError.Error(), "eeee")
	require.True(t, result.DataEqual)
	require.True(t, result.StructEqual)

	require.Equal(t, newReport.getSortedTables(), [][]string{{"`atest`.`atbl`", "0", "0"}, {"`ctest`.`atbl`", "0", "0"}, {"`dtest`.`atbl`", "0", "0"}, {"`test`.`tbl`", "222", "222"}})
	require.Equal(t, newReport.getDiffRows(), [][]string{})

	newReport.SetTableStructCheckResult("atest", "atbl", true, false, common.AllTableExistFlag)
	newReport.SetTableDataCheckResult("atest", "atbl", false, 111, 222, 333, 333, &chunk.CID{1, 1, 1, 1, 2})
	require.Equal(t, newReport.getSortedTables(), [][]string{{"`ctest`.`atbl`", "0", "0"}, {"`dtest`.`atbl`", "0", "0"}, {"`test`.`tbl`", "222", "222"}})
	require.Equal(t, newReport.getDiffRows(), [][]string{{"`atest`.`atbl`", "succeed", "true", "+111/-222", "333", "333"}})

	newReport.SetTableStructCheckResult("atest", "atbl", false, false, common.AllTableExistFlag)
	require.Equal(t, newReport.getSortedTables(), [][]string{{"`ctest`.`atbl`", "0", "0"}, {"`dtest`.`atbl`", "0", "0"}, {"`test`.`tbl`", "222", "222"}})
	require.Equal(t, newReport.getDiffRows(), [][]string{{"`atest`.`atbl`", "succeed", "false", "+111/-222", "333", "333"}})

	newReport.SetTableStructCheckResult("ctest", "atbl", false, true, common.AllTableExistFlag)

	newReport.SetTableStructCheckResult("dtest", "atbl", false, true, common.DownstreamTableLackFlag)

	buf := new(bytes.Buffer)
	newReport.Print(buf)
	info := buf.String()
	require.Contains(t, info, "The structure of `atest`.`atbl` is not equal\n")
	require.Contains(t, info, "The data of `atest`.`atbl` is not equal\n")
	require.Contains(t, info, "The structure of `ctest`.`atbl` is not equal, and data-check is skipped\n")
	require.Contains(t, info, "The data of `dtest`.`atbl` does not exist in downstream database\n")
	require.Contains(t, info, "\n"+
		"The rest of tables are all equal.\n\n"+
		"A total of 0 tables have been compared, 0 tables finished, 0 tables failed, 0 tables skipped.\n"+
		"The patch file has been generated in \n\t'output_dir/123456/fix-on-tidb1/'\n"+
		"You can view the comparison details through 'output_dir/sync_diff.log'\n")
}

func TestCalculateTotal(t *testing.T) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	report := NewReport(task)
	createTableSQL := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo,
			Collation: "[123]",
		},
	}
	configs := []*Config{
		{
			Host: "127.0.0.1",
			Port: 3306,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 3307,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 4000,
			User: "root",
		},
	}

	configsBytes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		buf := new(bytes.Buffer)
		err := toml.NewEncoder(buf).Encode(configs[i])
		require.NoError(t, err)
		configsBytes[i] = buf.Bytes()
	}
	report.Init(tableDiffs, configsBytes[:2], configsBytes[2])

	// Normal
	mock.ExpectQuery("select sum.*").WillReturnRows(sqlmock.NewRows([]string{"data"}).AddRow("123"))
	report.CalculateTotalSize(ctx, db)
	require.Equal(t, report.TotalSize, int64(123))
}

func TestPrint(t *testing.T) {
	report := NewReport(task)
	createTableSQL := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser.New())
	require.NoError(t, err)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo,
			Collation: "[123]",
		},
		{
			Schema:    "test",
			Table:     "tbl1",
			Info:      tableInfo,
			Collation: "[123]",
		},
	}
	configs := []*Config{
		{
			Host: "127.0.0.1",
			Port: 3306,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 3307,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 4000,
			User: "root",
		},
	}

	configsBytes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		buf := new(bytes.Buffer)
		err := toml.NewEncoder(buf).Encode(configs[i])
		require.NoError(t, err)
		configsBytes[i] = buf.Bytes()
	}
	report.Init(tableDiffs, configsBytes[:2], configsBytes[2])

	var buf *bytes.Buffer
	// All Pass
	report.SetTableStructCheckResult("test", "tbl", true, false, common.AllTableExistFlag)
	report.SetTableDataCheckResult("test", "tbl", true, 0, 0, 22, 22, &chunk.CID{0, 0, 0, 0, 1})
	buf = new(bytes.Buffer)
	report.Print(buf)
	require.Equal(t, buf.String(), "A total of 0 table have been compared and all are equal.\n"+
		"You can view the comparison details through 'output_dir/sync_diff.log'\n")

	// Error
	report.SetTableMeetError("test", "tbl1", errors.New("123"))
	report.SetTableStructCheckResult("test", "tbl1", false, false, common.AllTableExistFlag)
	buf = new(bytes.Buffer)
	report.Print(buf)
	require.Equal(t, buf.String(), "Error in comparison process:\n"+
		"123 error occurred in `test`.`tbl1`\n"+
		"You can view the comparison details through 'output_dir/sync_diff.log'\n")
}

func TestGetSnapshot(t *testing.T) {
	report := NewReport(task)
	createTableSQL1 := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo1, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)
	createTableSQL2 := "create table `atest`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)
	createTableSQL3 := "create table `xtest`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo3, err := dbutiltest.GetTableInfoBySQL(createTableSQL3, parser.New())
	require.NoError(t, err)

	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo1,
			Collation: "[123]",
		}, {
			Schema:    "atest",
			Table:     "tbl",
			Info:      tableInfo2,
			Collation: "[123]",
		}, {
			Schema:    "xtest",
			Table:     "tbl",
			Info:      tableInfo3,
			Collation: "[123]",
		},
	}
	configs := []*Config{
		{
			Host: "127.0.0.1",
			Port: 3306,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 3307,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 4000,
			User: "root",
		},
	}

	configsBytes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		buf := new(bytes.Buffer)
		err := toml.NewEncoder(buf).Encode(configs[i])
		require.NoError(t, err)
		configsBytes[i] = buf.Bytes()
	}
	report.Init(tableDiffs, configsBytes[:2], configsBytes[2])

	report.SetTableStructCheckResult("test", "tbl", true, false, common.AllTableExistFlag)
	report.SetTableDataCheckResult("test", "tbl", false, 100, 100, 200, 300, &chunk.CID{0, 0, 0, 1, 10})
	report.SetTableDataCheckResult("test", "tbl", true, 0, 0, 300, 300, &chunk.CID{0, 0, 0, 3, 10})
	report.SetTableDataCheckResult("test", "tbl", false, 200, 200, 400, 500, &chunk.CID{0, 0, 0, 3, 10})

	report.SetTableStructCheckResult("atest", "tbl", true, false, common.AllTableExistFlag)
	report.SetTableDataCheckResult("atest", "tbl", false, 100, 100, 500, 600, &chunk.CID{0, 0, 0, 0, 10})
	report.SetTableDataCheckResult("atest", "tbl", true, 0, 0, 600, 600, &chunk.CID{0, 0, 0, 3, 10})
	report.SetTableDataCheckResult("atest", "tbl", false, 200, 200, 700, 800, &chunk.CID{0, 0, 0, 3, 10})

	report.SetTableStructCheckResult("xtest", "tbl", true, false, common.AllTableExistFlag)
	report.SetTableDataCheckResult("xtest", "tbl", false, 100, 100, 800, 900, &chunk.CID{0, 0, 0, 0, 10})
	report.SetTableDataCheckResult("xtest", "tbl", true, 0, 0, 900, 900, &chunk.CID{0, 0, 0, 1, 10})
	report.SetTableDataCheckResult("xtest", "tbl", false, 200, 200, 1000, 1100, &chunk.CID{0, 0, 0, 3, 10})

	reportSnap, err := report.GetSnapshot(&chunk.CID{0, 0, 0, 1, 10}, "test", "tbl")
	require.NoError(t, err)
	require.Equal(t, reportSnap.TotalSize, report.TotalSize)
	require.Equal(t, reportSnap.Result, report.Result)
	for key, value := range report.TableResults {
		if _, ok := reportSnap.TableResults[key]; !ok {
			v, ok := value["tbl"]
			require.True(t, ok)
			require.Equal(t, v.Schema, "atest")
			continue
		}

		if _, ok := reportSnap.TableResults[key]["tbl"]; !ok {
			require.Equal(t, key, "atest")
			continue
		}

		v1 := value["tbl"]
		v2 := reportSnap.TableResults[key]["tbl"]
		require.Equal(t, v1.Schema, v2.Schema)
		require.Equal(t, v1.Table, v2.Table)
		require.Equal(t, v1.StructEqual, v2.StructEqual)
		require.Equal(t, v1.DataEqual, v2.DataEqual)
		require.Equal(t, v1.MeetError, v2.MeetError)

		chunkMap1 := v1.ChunkMap
		chunkMap2 := v2.ChunkMap
		for id, r1 := range chunkMap1 {
			sid := new(chunk.CID)
			if _, ok := chunkMap2[id]; !ok {
				require.NoError(t, sid.FromString(id))
				require.Equal(t, sid.Compare(&chunk.CID{0, 0, 0, 3, 10}), 0)
				continue
			}
			require.NoError(t, sid.FromString(id))
			require.True(t, sid.Compare(&chunk.CID{0, 0, 0, 1, 10}) <= 0)
			r2 := chunkMap2[id]
			require.Equal(t, r1.RowsAdd, r2.RowsAdd)
			require.Equal(t, r1.RowsDelete, r2.RowsDelete)
		}

	}
}

func TestCommitSummary(t *testing.T) {
	outputDir := "./"
	report := NewReport(&config.TaskConfig{OutputDir: outputDir, FixDir: task.FixDir})
	createTableSQL1 := "create table `test`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo1, err := dbutiltest.GetTableInfoBySQL(createTableSQL1, parser.New())
	require.NoError(t, err)
	createTableSQL2 := "create table `atest`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo2, err := dbutiltest.GetTableInfoBySQL(createTableSQL2, parser.New())
	require.NoError(t, err)
	createTableSQL3 := "create table `xtest`.`tbl`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo3, err := dbutiltest.GetTableInfoBySQL(createTableSQL3, parser.New())
	require.NoError(t, err)
	createTableSQL4 := "create table `xtest`.`tb1`(`a` int, `b` varchar(10), `c` float, `d` datetime, primary key(`a`, `b`))"
	tableInfo4, err := dbutiltest.GetTableInfoBySQL(createTableSQL4, parser.New())
	require.NoError(t, err)
	tableDiffs := []*common.TableDiff{
		{
			Schema:    "test",
			Table:     "tbl",
			Info:      tableInfo1,
			Collation: "[123]",
		}, {
			Schema:    "atest",
			Table:     "tbl",
			Info:      tableInfo2,
			Collation: "[123]",
		}, {
			Schema:    "xtest",
			Table:     "tbl",
			Info:      tableInfo3,
			Collation: "[123]",
		}, {
			Schema:    "ytest",
			Table:     "tbl",
			Info:      tableInfo3,
			Collation: "[123]",
		}, {
			Schema:    "xtest",
			Table:     "tb1",
			Info:      tableInfo4,
			Collation: "[123]",
		}, {
			Schema:    "xtest",
			Table:     "tb2",
			Info:      tableInfo4,
			Collation: "[123]",
		},
	}
	configs := []*Config{
		{
			Host: "127.0.0.1",
			Port: 3306,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 3307,
			User: "root",
		},
		{
			Host: "127.0.0.1",
			Port: 4000,
			User: "root",
		},
	}

	configsBytes := make([][]byte, 3)
	for i := 0; i < 3; i++ {
		buf := new(bytes.Buffer)
		err := toml.NewEncoder(buf).Encode(configs[i])
		require.NoError(t, err)
		configsBytes[i] = buf.Bytes()
	}
	report.Init(tableDiffs, configsBytes[:2], configsBytes[2])

	report.SetTableStructCheckResult("test", "tbl", true, false, common.AllTableExistFlag)
	report.SetTableDataCheckResult("test", "tbl", true, 100, 200, 400, 400, &chunk.CID{0, 0, 0, 1, 10})

	report.SetTableStructCheckResult("atest", "tbl", true, false, common.AllTableExistFlag)
	report.SetTableDataCheckResult("atest", "tbl", false, 100, 200, 500, 600, &chunk.CID{0, 0, 0, 2, 10})

	report.SetTableStructCheckResult("xtest", "tbl", false, false, common.AllTableExistFlag)
	report.SetTableDataCheckResult("xtest", "tbl", false, 100, 200, 600, 700, &chunk.CID{0, 0, 0, 3, 10})

	report.SetTableStructCheckResult("xtest", "tb1", false, true, common.UpstreamTableLackFlag)
	report.SetTableDataCheckResult("xtest", "tb1", false, 0, 200, 0, 200, &chunk.CID{0, 0, 0, 4, 10})

	report.SetTableStructCheckResult("xtest", "tb2", false, true, common.DownstreamTableLackFlag)
	report.SetTableDataCheckResult("xtest", "tb2", false, 100, 0, 100, 0, &chunk.CID{0, 0, 0, 5, 10})

	err = report.CommitSummary()
	require.NoError(t, err)
	filename := path.Join(outputDir, "summary.txt")
	file, err := os.Open(filename)
	require.NoError(t, err)

	p := make([]byte, 2048)
	file.Read(p)
	str := string(p)
	require.Contains(t, str, "Summary\n\n\n\n"+
		"Source Database\n\n\n\n"+
		"host = \"127.0.0.1\"\n"+
		"port = 3306\n"+
		"user = \"root\"\n\n"+
		"host = \"127.0.0.1\"\n"+
		"port = 3307\n"+
		"user = \"root\"\n\n"+
		"Target Databases\n\n\n\n"+
		"host = \"127.0.0.1\"\n"+
		"port = 4000\n"+
		"user = \"root\"\n\n"+
		"Comparison Result\n\n\n\n"+
		"The table structure and data in following tables are equivalent\n\n"+
		"+---------------+---------+-----------+\n"+
		"|     TABLE     | UPCOUNT | DOWNCOUNT |\n"+
		"+---------------+---------+-----------+\n"+
		"| `test`.`tbl`  |     400 |       400 |\n"+
		"| `ytest`.`tbl` |       0 |         0 |\n"+
		"+---------------+---------+-----------+\n\n\n"+
		"The following tables contains inconsistent data\n\n"+
		"+---------------+---------+--------------------+----------------+---------+-----------+\n"+
		"|     TABLE     | RESULT  | STRUCTURE EQUALITY | DATA DIFF ROWS | UPCOUNT | DOWNCOUNT |\n"+
		"+---------------+---------+--------------------+----------------+---------+-----------+\n")
	require.Contains(t, str,
		"| `atest`.`tbl` | succeed | true               | +100/-200      |     500 |       600 |\n")
	require.Contains(t, str,
		"| `xtest`.`tbl` | succeed | false              | +100/-200      |     600 |       700 |\n")
	require.Contains(t, str,
		"| `xtest`.`tb1` | skipped | false              | +0/-200        |       0 |       200 |\n")
	require.Contains(t, str,
		"| `xtest`.`tb2` | skipped | false              | +100/-0        |     100 |         0 |\n")

	file.Close()
	err = os.Remove(filename)
	require.NoError(t, err)
}
