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

package checker

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/util/filter"
	"github.com/stretchr/testify/require"
)

var errNoSuchTable = &mysql.MySQLError{Number: 1146, Message: "Table 'xxx' doesn't exist"}

func TestShardingTablesChecker(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()

	// 1. test a success check
	mock = initShardingMock(mock)
	createTableRow2 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)

	checker := NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		false,
		1)
	result := checker.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.NoError(t, mock.ExpectationsWereMet())

	// 2. check different column number
	checker = NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		false,
		1)
	mock = initShardingMock(mock)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" int(11) NOT NULL,
  "d" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)

	result = checker.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Len(t, result.Errors, 1)
	require.NoError(t, mock.ExpectationsWereMet())

	// 3. check different column def
	checker = NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		false,
		1)
	mock = initShardingMock(mock)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" varchar(20) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)

	result = checker.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Len(t, result.Errors, 1)
	require.NoError(t, mock.ExpectationsWereMet())

	// 4. test tiflow#5759
	checker = NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
			{Schema: "test-db", Name: "test-table-3"},
			{Schema: "test-db", Name: "test-table-4"},
		}},
		false,
		1)
	mock = initShardingMock(mock)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" varchar(20) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)
	createTableRow3 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-3", `CREATE TABLE "test-table-3" (
  "c" varchar(20) NOT NULL,
  "c2" INT,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-3`").WillReturnRows(createTableRow3)
	createTableRow4 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-4", `CREATE TABLE "test-table-4" (
  "c" varchar(20) NOT NULL,
  "c2" INT,
  "c3" INT,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-4`").WillReturnRows(createTableRow4)

	// in tiflow#5759, this function will enter deadlock
	result = checker.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Len(t, result.Errors, 3)
}

func TestTablesChecker(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	downDB, downMock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()

	commonMock := func() {
		maxConnectionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("max_connections", "2")
		mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow)
		sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "ANSI_QUOTES")
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
		sqlModeRow2 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "ANSI_QUOTES")
		downMock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow2)
	}

	// 1. test a success check

	commonMock()
	createTableRowUp := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  PRIMARY KEY ("c")
		) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnError(errNoSuchTable)

	checker := NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result := checker.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 2. check many errors

	commonMock()
	createTableRowUp = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  CONSTRAINT "fk" FOREIGN KEY ("c") REFERENCES "t" ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	createTableRow2 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  CONSTRAINT "fk" FOREIGN KEY ("c") REFERENCES "t" ("c")
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow2)

	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Len(t, result.Errors, 2)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 3. test #5759

	commonMock()
	createTableRow1 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL
) ENGINE=InnoDB`)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" int(11) NOT NULL
) ENGINE=InnoDB`)
	createTableRow3 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-3", `CREATE TABLE "test-table-3" (
  "c" int(11) NOT NULL
) ENGINE=InnoDB`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow1)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table`").WillReturnError(errNoSuchTable)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-3`").WillReturnRows(createTableRow3)

	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table"}: {
					{Schema: "test-db", Name: "test-table-1"},
					{Schema: "test-db", Name: "test-table-2"},
					{Schema: "test-db", Name: "test-table-3"},
				},
			},
		},
		nil,
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Len(t, result.Errors, 3)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 4. check warning from mismatching of upstream/downstream
	commonMock()
	createTableRowUp = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  "d" int(11) NOT NULL,
		  "e" int(11) NOT NULL,
		  PRIMARY KEY ("c"),
		  UNIQUE KEY "idx_d" ("d")
		) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table", `CREATE TABLE "test-table" (
  		  "c" int(11) NOT NULL,
  		  "d" int(11) NOT NULL,
  		  "f" int(11) DEFAULT NULL,
  		  "g" int(11) NOT NULL,
		  PRIMARY KEY ("c")
		) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci`)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table`").WillReturnRows(createTableRow2)

	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Len(t, result.Errors, 5)
	require.Equal(t,
		"table `test-db`.`test-table-1` charset is not same, upstream: (test-table-1 latin1), downstream: (test-table gbk)",
		result.Errors[0].ShortErr)
	require.Equal(t,
		"table `test-db`.`test-table-1` collation is not same, upstream: (test-table-1 latin1_bin), downstream: (test-table gbk_chinese_ci)",
		result.Errors[1].ShortErr)
	require.Equal(t,
		"table `test-db`.`test-table-1` upstream has more PK or NOT NULL UK than downstream, index name: idx_d, columns: [d]",
		result.Errors[2].ShortErr)
	require.Equal(t,
		"table `test-db`.`test-table-1` upstream has more columns than downstream, columns: [e]",
		result.Errors[3].ShortErr)
	require.Equal(t,
		"table `test-db`.`test-table-1` downstream has more columns than upstream that require values to insert records, table name: test-table, columns: [g]",
		result.Errors[4].ShortErr)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 5. check extended columns
	commonMock()
	createTableRowUp = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  "ext1" int(11) NOT NULL,
		  PRIMARY KEY ("c")
		) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	createTableRowDown := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  "ext3" int(11) NOT NULL,
		  PRIMARY KEY ("c")
		) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin`)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowDown)

	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		map[filter.Table][]string{
			{Schema: "test-db", Name: "test-table-1"}: {"ext1", "ext2", "ext3"},
		},
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Len(t, result.Errors, 2)
	require.Equal(t,
		"table `test-db`.`test-table-1` upstream table must not contain extended column [ext1]",
		result.Errors[0].ShortErr)
	require.Equal(t,
		"table `test-db`.`test-table-1` downstream table must contain extended columns [ext1 ext2]",
		result.Errors[1].ShortErr)
	require.Contains(t, result.Instruction, "DM automatically fills the values of extended columns. You need to remove these columns or change configuration.")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())
}

func TestCombineInstruction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	downDB, downMock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()
	commonMock := func() {
		maxConnectionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("max_connections", "2")
		mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow)
		sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "ANSI_QUOTES")
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
		sqlModeRow2 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("sql_mode", "ANSI_QUOTES")
		downMock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow2)
	}

	// 1. table with foreign key & no primary key
	commonMock()
	createTableRowUp := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
			"b" int(11) NOT NULL,
			FOREIGN KEY ("b") REFERENCES "test-table-2" ("c")
		) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnError(errNoSuchTable)
	checker := NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result := checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Contains(t, result.Instruction, "TiDB does not support foreign key constraints. See the document: https://docs.pingcap.com/tidb/stable/mysql-compatibility#unsupported-features")
	require.Contains(t, result.Instruction, "You need to set primary/unique keys for the table. Otherwise replication efficiency might become very low and exactly-once replication cannot be guaranteed.")
	require.Contains(t, result.Errors[0].ShortErr, "is parsed but ignored by TiDB.")
	require.Contains(t, result.Errors[1].ShortErr, "primary/unique key does not exist")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 2. mismatched index columns
	commonMock()
	createTableRowUp = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
	    "c" int(11) NOT NULL,
			"b" int(11) NOT NULL,
			"d" int(11) NOT NULL,
			PRIMARY KEY("c", "b", "d")
		) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	createTableDown := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		"c" int(11) NOT NULL,
		"b" int(11) NOT NULL,
		"d" int(11) NOT NULL,
		PRIMARY KEY("c")
	) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableDown)
	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Equal(t, result.State, StateWarning)
	require.Contains(t, result.Errors[0].ShortErr, "upstream has more PK or NOT NULL UK than downstream")
	require.Contains(t, result.Instruction, "Ensure that you use the same index columns for both upstream and downstream databases. Otherwise the migration job might fail or data inconsistency might occur.")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 3. charset not same or collation not same
	commonMock()
	createTableRowUp = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
	    "c" int(11) NOT NULL,
			"b" int(11) NOT NULL,
			"d" int(11) NOT NULL,
			PRIMARY KEY("c")
		) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin`)
	createTableDown = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		"c" int(11) NOT NULL,
		"b" int(11) NOT NULL,
		"d" int(11) NOT NULL,
		PRIMARY KEY("c")
	) ENGINE=InnoDB DEFAULT CHARSET=binary COLLATE=binary`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableDown)
	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result = checker.Check(ctx)
	require.Equal(t, result.State, StateWarning)
	require.Contains(t, result.Errors[0].ShortErr, "charset is not same")
	require.Contains(t, result.Errors[1].ShortErr, "collation is not same")
	require.Contains(t, result.Instruction, "Ensure that you use the same charsets for both upstream and downstream databases. Different charsets might cause data inconsistency.")
	require.Contains(t, result.Instruction, "Ensure that you use the same collations for both upstream and downstream databases. Otherwise the query results from the two databases might be inconsistent.")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 4. different column number
	commonMock()
	createTableRowUp = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
	    "c" int(11) NOT NULL,
			"b" int(11) NOT NULL,
			"d" int(11) NOT NULL,
			PRIMARY KEY("c")
		) ENGINE=InnoDB`)
	createTableDown = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		"c" int(11) NOT NULL,
		"b" int(11) NOT NULL,
		PRIMARY KEY("c")
	) ENGINE=InnoDB`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableDown)
	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Contains(t, result.Errors[0].ShortErr, "upstream has more columns than downstream")
	require.Contains(t, result.Instruction, "Ensure that the column numbers are the same between upstream and downstream databases. Otherwise the migration job may fail.")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 5. upstream has extended column & downstream doesn't have extended column
	commonMock()
	createTableRowUp = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
      "c" int(11) NOT NULL,
			"b" int(11) NOT NULL,
			"d" int(11) NOT NULL,
			PRIMARY KEY("c")
		) ENGINE=InnoDB`)
	createTableDown = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		"c" int(11) NOT NULL,
		"b" int(11) NOT NULL,
		"e" int(11) NOT NULL,
		PRIMARY KEY("c")
	) ENGINE=InnoDB`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableDown)
	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		map[filter.Table][]string{
			{Schema: "test-db", Name: "test-table-1"}: {"d"},
		},
		1) // extended column "d"
	result = checker.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Contains(t, result.Errors[0].ShortErr, "upstream table must not contain extended column")
	require.Contains(t, result.Errors[1].ShortErr, "downstream table must contain extended columns")
	require.Contains(t, result.Instruction, "DM automatically fills the values of extended columns. You need to remove these columns or change configuration.")
	require.Contains(t, result.Instruction, "You need to manually add extended columns to the downstream table.")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 6. no downstream table with extended column
	commonMock()
	createTableRowUp = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
      "c" int(11) NOT NULL,
			"b" int(11) NOT NULL,
			PRIMARY KEY("c")
		) ENGINE=InnoDB`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRowUp)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnError(errNoSuchTable)
	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		map[filter.Table][]string{
			{Schema: "test-db", Name: "test-table-1"}: {"d"},
		},
		1) // extended column "d"
	result = checker.Check(ctx)
	require.Equal(t, StateFailure, result.State)
	require.Contains(t, result.Errors[0].ShortErr, "does not exist in downstream table")
	require.Contains(t, result.Instruction, "You need to create a table with extended columns before replication.")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())
}

func TestOptimisticShardingTablesChecker(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()

	cases := []struct {
		createTable1SQL string
		createTable2SQL string
		expectState     State
		errLen          int
	}{
		// optimistic check different column number
		{
			createTable1SQL: `CREATE TABLE "test-table-1" (
				"c" int(11) NOT NULL,
				PRIMARY KEY ("c")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			createTable2SQL: `CREATE TABLE "test-table-2" (
				"c" int(11) NOT NULL,
				"d" int(11) NOT NULL,
				PRIMARY KEY ("c")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			expectState: StateSuccess,
		},
		// optimistic check auto_increment conflict
		{
			createTable1SQL: `CREATE TABLE "test-table-1" (
				"c" int(11) NOT NULL AUTO_INCREMENT,
				PRIMARY KEY ("c")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			createTable2SQL: `CREATE TABLE "test-table-2" (
				"c" int(11) NOT NULL AUTO_INCREMENT,
				"d" int(11) NOT NULL,
				PRIMARY KEY ("c")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			expectState: StateWarning,
			errLen:      2, // 2 auto_increment warning
		},
		{
			createTable1SQL: `CREATE TABLE "test-table-1" (
				"c" int(11) NOT NULL AUTO_INCREMENT,
				PRIMARY KEY ("c")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			createTable2SQL: `CREATE TABLE "test-table-2" (
				"c" int(11) NOT NULL,
				"d" int(11) NOT NULL,
				PRIMARY KEY ("c")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			expectState: StateWarning,
			errLen:      1, // 1 auto_increment warning
		},
		// must set auto_increment with key(failure)
		{
			createTable1SQL: `CREATE TABLE "test-table-1" (
				"c" int(11) NOT NULL AUTO_INCREMENT
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			createTable2SQL: `CREATE TABLE "test-table-2" (
				"c" int(11) NOT NULL,
				"d" int(11) NOT NULL
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			expectState: StateFailure,
			errLen:      2, // 1 auto_increment warning
		},
		{
			createTable1SQL: `CREATE TABLE "test-table-1" (
				"c" int(11) NOT NULL AUTO_INCREMENT,
				PRIMARY KEY ("c")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			createTable2SQL: `CREATE TABLE "test-table-2" (
				"c" int(11) NOT NULL,
				"d" int(11) NOT NULL
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			expectState: StateFailure,
			errLen:      2, // 1 auto_increment warning
		},
		// different auto_increment
		{
			createTable1SQL: `CREATE TABLE "test-table-1" (
				"c" int(11) NOT NULL AUTO_INCREMENT,
				PRIMARY KEY ("c")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			createTable2SQL: `CREATE TABLE "test-table-2" (
				"c" int(11) NOT NULL,
				"d" int(11) NOT NULL AUTO_INCREMENT,
				PRIMARY KEY ("d")
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
			expectState: StateFailure,
			errLen:      3, // 2 auto_increment warning
		},
	}

	for _, cs := range cases {
		maxConnecionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2")
		mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnecionsRow)
		sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ANSI_QUOTES")
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
		createTableRow1 := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test-table-1", cs.createTable1SQL)
		mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow1)
		createTableRow2 := sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test-table-2", cs.createTable2SQL)
		mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)
		checker := NewOptimisticShardingTablesChecker(
			"test-name",
			map[string]*sql.DB{"test-source": db},
			map[string][]filter.Table{"test-source": {
				{Schema: "test-db", Name: "test-table-1"},
				{Schema: "test-db", Name: "test-table-2"},
			}},
			0)
		result := checker.Check(ctx)
		require.Equal(t, cs.expectState, result.State)
		require.Len(t, result.Errors, cs.errLen)
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

func TestUnknownCharsetCollation(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	downDB, downMock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()

	// 1. test TablesChecker

	maxConnectionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow)
	sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  PRIMARY KEY ("c")
		) ENGINE=InnoDB DEFAULT CHARSET=utf32`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	sqlModeRow2 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	downMock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow2)

	checker := NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result := checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Len(t, result.Errors, 1)
	require.Contains(t, result.Errors[0].ShortErr, "Unknown character set: 'utf32'")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// 2. test ShardingTablesChecker
	// 2.1 the first table has unknown charset

	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
"c" int(11) NOT NULL,
PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=utf16`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)

	checker = NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		false,
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Len(t, result.Errors, 1)
	require.Contains(t, result.Errors[0].ShortErr, "Unknown character set: 'utf16'")
	require.NoError(t, mock.ExpectationsWereMet())

	// 2.2 not the first table has unknown charset

	mock = initShardingMock(mock)
	createTableRow2 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=utf16`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)

	checker = NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		false,
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Len(t, result.Errors, 1)
	require.Contains(t, result.Errors[0].ShortErr, "Unknown character set: 'utf16'")
	require.NoError(t, mock.ExpectationsWereMet())

	// 2.3 not the first table has unknown collation

	mock = initShardingMock(mock)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_german1_ci`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)

	checker = NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		false,
		1)
	result = checker.Check(ctx)
	// unknown collation will not raise error during parsing
	require.Equal(t, StateSuccess, result.State)
	require.NoError(t, mock.ExpectationsWereMet())

	// 3. test OptimisticShardingTablesChecker

	maxConnecionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnecionsRow)
	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test-table-1", `
CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=utf16`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("test-table-2", `
CREATE TABLE "test-table-2" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=utf16`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)
	checker = NewOptimisticShardingTablesChecker(
		"test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		0)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Len(t, result.Errors, 2)
	require.Contains(t, result.Errors[0].ShortErr, "Unknown character set: 'utf16'")
	require.Contains(t, result.Errors[1].ShortErr, "Unknown character set: 'utf16'")
	require.NoError(t, mock.ExpectationsWereMet())
}

func initShardingMock(mock sqlmock.Sqlmock) sqlmock.Sqlmock {
	sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
"c" int(11) NOT NULL,
PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)

	maxConnecionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnecionsRow)
	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
"c" int(11) NOT NULL,
PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	return mock
}

func TestExpressionUK(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	downDB, downMock, err := sqlmock.New()
	require.NoError(t, err)
	ctx := context.Background()

	// test same table structure

	maxConnectionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow)
	sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  "c2" int(11) NOT NULL,
		  PRIMARY KEY ("c"),
		  UNIQUE KEY "uk" (("c2"+1), "c")
		) ENGINE=InnoDB`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	sqlModeRow2 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	downMock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow2)
	createTableRow2 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  "c2" int(11) NOT NULL,
		  PRIMARY KEY ("c"),
		  UNIQUE KEY "uk" (("c2"+1), "c")
		) ENGINE=InnoDB`)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow2)
	checker := NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result := checker.Check(ctx)
	require.Equal(t, StateSuccess, result.State)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())

	// test different table structure

	maxConnectionsRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow)
	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  "c2" int(11) NOT NULL,
		  PRIMARY KEY ("c"),
		  UNIQUE KEY "uk" (("c2"+1), "c")
		) ENGINE=InnoDB`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	sqlModeRow2 = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	downMock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow2)
	createTableRow2 = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
		  "c" int(11) NOT NULL,
		  "c2" int(11) NOT NULL,
		  PRIMARY KEY ("c"),
		  UNIQUE KEY "uk" (("c2"+3), "c")
		) ENGINE=InnoDB`)
	downMock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow2)

	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		downDB,
		map[string]map[filter.Table][]filter.Table{
			"test-source": {
				{Schema: "test-db", Name: "test-table-1"}: {
					{Schema: "test-db", Name: "test-table-1"},
				},
			},
		},
		nil,
		1)
	result = checker.Check(ctx)
	require.Equal(t, StateWarning, result.State)
	require.Len(t, result.Errors, 2)
	// maybe [`c2`+1 c] or [c `c2`+1]
	require.Contains(t, result.Errors[0].ShortErr, "upstream has more PK or NOT NULL UK than downstream")
	require.Contains(t, result.Errors[0].ShortErr, "`c2`+1")
	require.Contains(t, result.Errors[1].ShortErr, "downstream has more PK or NOT NULL UK than upstream")
	require.Contains(t, result.Errors[1].ShortErr, "`c2`+3")
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, downMock.ExpectationsWereMet())
}
