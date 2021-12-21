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
	"github.com/DATA-DOG/go-sqlmock"
)

// MustAdjustSQLMode adjusts SQL mode for compatibility, like what cdc mysql sink does.
func MustAdjustSQLMode(mock sqlmock.Sqlmock) {
	// sql mode is adjust for compatibility.
	mock.ExpectQuery("SELECT @@SESSION.sql_mode;").
		WillReturnRows(sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
			AddRow("STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"))
	mock.ExpectExec("SET sql_mode = 'ALLOW_INVALID_DATES,IGNORE_SPACE,NO_AUTO_VALUE_ON_ZERO';").
		WillReturnResult(sqlmock.NewResult(0, 0))
}
