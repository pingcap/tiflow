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
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

// MySQLVersion represents MySQL version number.
type MySQLVersion [3]uint

// MinVersion define a mininum version.
var MinVersion = MySQLVersion{0, 0, 0}

// MaxVersion define a maximum version.
var MaxVersion = MySQLVersion{math.MaxUint8, math.MaxUint8, math.MaxUint8}

// version format:
// mysql        5.7.18-log
// mariadb      5.5.50-MariaDB-1~wheezy
// percona      5.7.19-17-log
// aliyun rds   5.7.18-log
// aws rds      5.7.16-log
// ref: https://dev.mysql.com/doc/refman/5.7/en/which-version.html

// v is mysql version in string format.
func toMySQLVersion(v string) (MySQLVersion, error) {
	version := MySQLVersion{0, 0, 0}
	tmp := strings.Split(v, "-")
	if len(tmp) == 0 {
		return version, errors.NotValidf("MySQL version %s", v)
	}

	tmp = strings.Split(tmp[0], ".")
	if len(tmp) != 3 {
		return version, errors.NotValidf("MySQL version %s", v)
	}

	for i := range tmp {
		val, err := strconv.ParseUint(tmp[i], 10, 64)
		if err != nil {
			return version, errors.NotValidf("MySQL version %s", v)
		}
		version[i] = uint(val)
	}
	return version, nil
}

// Ge means v >= min.
func (v MySQLVersion) Ge(min MySQLVersion) bool {
	for i := range v {
		if v[i] > min[i] {
			return true
		} else if v[i] < min[i] {
			return false
		}
	}
	return true
}

// Gt means v > min.
func (v MySQLVersion) Gt(min MySQLVersion) bool {
	for i := range v {
		if v[i] > min[i] {
			return true
		} else if v[i] < min[i] {
			return false
		}
	}
	return false
}

// Lt means v < min.
func (v MySQLVersion) Lt(max MySQLVersion) bool {
	for i := range v {
		if v[i] < max[i] {
			return true
		} else if v[i] > max[i] {
			return false
		}
	}
	return false
}

// Le means v <= min.
func (v MySQLVersion) Le(max MySQLVersion) bool {
	for i := range v {
		if v[i] < max[i] {
			return true
		} else if v[i] > max[i] {
			return false
		}
	}
	return true
}

// String implements the Stringer interface.
func (v MySQLVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])
}

// IsMariaDB tells whether the version is from mariadb.
func IsMariaDB(version string) bool {
	return strings.Contains(strings.ToUpper(version), "MARIADB")
}

func markCheckError(result *Result, err error) {
	if err != nil {
		var state State
		if utils.OriginError(err) == context.Canceled {
			state = StateWarning
		} else {
			state = StateFailure
		}
		if result.State != StateFailure {
			result.State = state
		}
		result.Errors = append(result.Errors, &Error{Severity: state, ShortErr: err.Error()})
	}
}

func isMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

const tablesWithOneThread = 10000 / 16

var cunrrencies = []int{4, 8, 16, 32}

func getConcurrency(tableNum int) int {
	// if tableNum < 100, one thread is enough
	if tableNum < 100 {
		return 1
	}
	threads := tableNum/tablesWithOneThread + 1
	for _, num := range cunrrencies {
		if num >= threads {
			return num
		}
	}
	return cunrrencies[len(cunrrencies)-1]
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

	return mock
}
