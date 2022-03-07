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

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateOptions(t *testing.T) {
	testCases := []struct {
		o           *options
		expectedErr string
	}{
		{
			&options{
				rowBytes: 0,
			},
			".*rowBytes must be greater than zero.*",
		},
		{
			&options{
				rowBytes: 1024 * 1024,
				rowCount: 0,
			},
			".*rowCount must be greater than zero.*",
		},
		{
			&options{
				rowBytes:    1024 * 1024,
				rowCount:    1,
				sqlFilePath: "",
			},
			".*please specify the correct file path.*",
		},
		{
			&options{
				rowBytes:     1024 * 1024,
				rowCount:     1,
				sqlFilePath:  "./test.sql",
				databaseName: "",
			},
			".*please specify the database name.*",
		},
		{
			&options{
				rowBytes:     1024 * 1024,
				rowCount:     1,
				sqlFilePath:  "./test.sql",
				databaseName: "kafka-big-messages",
				tableName:    "",
			},
			".*please specify the table name.*",
		},
		{
			&options{
				rowBytes:     1024 * 1024,
				rowCount:     10,
				sqlFilePath:  "./test.sql",
				databaseName: "kafka-big-messages",
				tableName:    "test",
			},
			"",
		},
	}

	for _, tc := range testCases {
		err := tc.o.validate()
		if tc.expectedErr != "" {
			require.Error(t, err)
			require.Regexp(t, tc.expectedErr, tc.o.validate().Error())
		} else {
			require.Nil(t, err)
		}
	}
}

func TestGenDatabaseSql(t *testing.T) {
	database := "test"

	sql := genDatabaseSql(database)

	require.Equal(t, "DROP DATABASE IF EXISTS test;\nCREATE DATABASE test;\nUSE test;\n\n", sql)
}

func TestGenCreateTableSql(t *testing.T) {
	rawBytes := varcharColumnMaxLen
	tableName := "test"

	sql := genCreateTableSql(rawBytes, tableName)
	require.Equal(t, "CREATE TABLE test(id int primary key , a0 VARCHAR(16383));\n", sql)
}

func TestGenInsertSql(t *testing.T) {
	// Override the col value to test.
	oldColValue := colValue
	colValue = "a"
	defer func() {
		colValue = oldColValue
	}()

	rawBytes := varcharColumnMaxLen
	tableName := "test"
	id := 1

	sql := genInsertSql(rawBytes, tableName, id)
	println(sql)
	require.Equal(t, "INSERT INTO test VALUES (1, 'a');\n", sql)
}
