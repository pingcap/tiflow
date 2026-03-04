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

package source

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

func TestGetChecksumSplitFields(t *testing.T) {
	testCases := []struct {
		name           string
		createTableSQL string
		pkIsHandle     bool
		isCommonHandle bool
		expectedFields string
	}{
		{
			name:           "pk is handle",
			createTableSQL: "CREATE TABLE `t` (`id` BIGINT PRIMARY KEY, `v` INT)",
			pkIsHandle:     true,
			isCommonHandle: false,
			expectedFields: "id",
		},
		{
			name:           "common handle",
			createTableSQL: "CREATE TABLE `t` (`a` VARCHAR(10), `b` VARCHAR(10), PRIMARY KEY(`a`,`b`))",
			pkIsHandle:     false,
			isCommonHandle: true,
			expectedFields: "a,b",
		},
		{
			name:           "tidb row id fallback",
			createTableSQL: "CREATE TABLE `t` (`a` INT, `b` INT, KEY `idx_a`(`a`))",
			pkIsHandle:     false,
			isCommonHandle: false,
			expectedFields: "_tidb_rowid",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableInfo, err := utils.GetTableInfoBySQL(tc.createTableSQL, parser.New())
			require.NoError(t, err)
			tableInfo.PKIsHandle = tc.pkIsHandle
			tableInfo.IsCommonHandle = tc.isCommonHandle
			require.Equal(t, tc.expectedFields, getChecksumSplitFields(tableInfo))
		})
	}
}
