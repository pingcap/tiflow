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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/stretchr/testify/require"
)

func TestMysqlVersion(t *testing.T) {
	versionChecker := &MySQLVersionChecker{}

	cases := []struct {
		rawVersion string
		pass       bool
	}{
		{"5.5.0-log", false},
		{"5.6.0-log", true},
		{"5.7.0-log", true},
		{"5.8.0-log", true}, // although it does not exist
		{"8.0.1-log", false},
		{"8.0.20", false},
		{"5.5.50-MariaDB-1~wheezy", false},
		{"10.1.1-MariaDB-1~wheezy", false},
		{"10.1.2-MariaDB-1~wheezy", false},
		{"10.13.1-MariaDB-1~wheezy", false},
	}

	for _, cs := range cases {
		result := &Result{
			State: StateWarning,
		}
		versionChecker.checkVersion(cs.rawVersion, result)
		require.Equal(t, result.State == StateSuccess, cs.pass)
	}
}

func TestVersionInstruction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	versionChecker := &MySQLVersionChecker{
		db:     db,
		dbinfo: &dbutil.DBConfig{},
	}
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version';").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "8.0.20"))
	result := versionChecker.Check(context.Background())
	require.Equal(t, result.State, StateWarning)
	require.Equal(t, result.Instruction, "It is recommended that you select a database version that meets the requirements before performing data migration. Otherwise data inconsistency or task exceptions might occur.")
}
