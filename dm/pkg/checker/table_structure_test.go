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
	"encoding/json"
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	tc "github.com/pingcap/check"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

func (t *testCheckSuite) TestShardingTablesChecker(c *tc.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, tc.IsNil)
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
		map[string][]*filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		nil,
		false,
		1)
	result := checker.Check(ctx)
	c.Assert(result.State, tc.Equals, StateSuccess)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)

	// 2. check different column number
	checker = NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]*filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		nil,
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
	c.Assert(result.State, tc.Equals, StateFailure)
	c.Assert(result.Errors, tc.HasLen, 1)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)

	// 3. check different column def
	checker = NewShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]*filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
			{Schema: "test-db", Name: "test-table-2"},
		}},
		nil,
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
	c.Assert(result.State, tc.Equals, StateFailure)
	c.Assert(result.Errors, tc.HasLen, 1)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
}

func (t *testCheckSuite) TestTablesChecker(c *tc.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, tc.IsNil)
	ctx := context.Background()

	// 1. test a success check
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
		) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)

	checker := NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		map[string][]*filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
		}},
		1)
	result := checker.Check(ctx)
	c.Assert(result.State, tc.Equals, StateSuccess)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)

	// 2. check many errors
	maxConnectionsRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow)
	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  CONSTRAINT "fk" FOREIGN KEY ("c") REFERENCES "t" ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)

	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		map[string][]*filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
		}},
		1)
	result = checker.Check(ctx)
	c.Assert(result.State, tc.Equals, StateFailure)
	c.Assert(result.Errors, tc.HasLen, 2) // no PK/UK + has FK
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)

	// 3. unsupported charset
	maxConnectionsRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnectionsRow)
	sqlModeRow = sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow = sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
  "c" int(11) NOT NULL,
  PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=ucs2`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)

	checker = NewTablesChecker(
		map[string]*sql.DB{"test-source": db},
		map[string][]*filter.Table{"test-source": {
			{Schema: "test-db", Name: "test-table-1"},
		}},
		1)
	result = checker.Check(ctx)
	c.Assert(result.State, tc.Equals, StateFailure)
	c.Assert(result.Errors, tc.HasLen, 1)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
}

func (t *testCheckSuite) TestOptimisticShardingTablesChecker(c *tc.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, tc.IsNil)
	ctx := context.Background()

	printJSON := func(r *Result) {
		rawResult, _ := json.MarshalIndent(r, "", "\t")
		fmt.Println("\n" + string(rawResult))
	}

	// optimistic check different column number
	maxConnecionsRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("max_connections", "2")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(maxConnecionsRow)
	sqlModeRow := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlModeRow)
	createTableRow := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-1", `CREATE TABLE "test-table-1" (
"c" int(11) NOT NULL,
PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-1`").WillReturnRows(createTableRow)
	createTableRow2 := sqlmock.NewRows([]string{"Table", "Create Table"}).
		AddRow("test-table-2", `CREATE TABLE "test-table-2" (
"c" int(11) NOT NULL,
"d" int(11) NOT NULL,
PRIMARY KEY ("c")
) ENGINE=InnoDB DEFAULT CHARSET=latin1`)
	mock.ExpectQuery("SHOW CREATE TABLE `test-db`.`test-table-2`").WillReturnRows(createTableRow2)
	checker := NewOptimisticShardingTablesChecker("test-name",
		map[string]*sql.DB{"test-source": db},
		map[string][]*filter.Table{"test-source": {
			&filter.Table{Schema: "test-db", Name: "test-table-1"},
			&filter.Table{Schema: "test-db", Name: "test-table-2"},
		}},
		0)
	result := checker.Check(ctx)
	printJSON(result)
	c.Assert(result.State, tc.Equals, StateSuccess)
	c.Assert(mock.ExpectationsWereMet(), tc.IsNil)
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
