// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

func TestPrepareChecksumSplitFields(t *testing.T) {
	testCases := []struct {
		name           string
		createTableSQL string
		pkIsHandle     bool
		isCommonHandle bool
		expectedFields string
		expectErr      bool
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
		{
			name:           "pk is handle but pk col removed by ignore columns",
			createTableSQL: "CREATE TABLE `t` (`a` INT, `b` INT)",
			pkIsHandle:     true,
			isCommonHandle: false,
			expectErr:      true,
		},
		{
			name:           "common handle but pk index removed by ignore columns",
			createTableSQL: "CREATE TABLE `t` (`a` INT, `b` INT)",
			pkIsHandle:     false,
			isCommonHandle: true,
			expectErr:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableInfo, err := utils.GetTableInfoBySQL(tc.createTableSQL, parser.New())
			require.NoError(t, err)
			tableInfo.PKIsHandle = tc.pkIsHandle
			tableInfo.IsCommonHandle = tc.isCommonHandle
			fields, err := prepareChecksumSplitFields(tableInfo)
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedFields, fields)
			}
		})
	}
}

func TestPrepareChecksumSplitFieldsOnRowID(t *testing.T) {
	original, err := utils.GetTableInfoBySQL("CREATE TABLE `t` (`a` INT, `b` INT)", parser.New())
	require.NoError(t, err)

	cloned := original.Clone()
	fields, err := prepareChecksumSplitFields(cloned)
	require.NoError(t, err)
	require.Equal(t, "_tidb_rowid", fields)

	require.Nil(t, dbutil.FindColumnByName(original.Columns, "_tidb_rowid"))
	rowIDCol := dbutil.FindColumnByName(cloned.Columns, "_tidb_rowid")
	require.NotNil(t, rowIDCol)

	originalIndices := dbutil.FindAllIndex(original)
	require.Empty(t, originalIndices)

	clonedIndices := dbutil.FindAllIndex(cloned)
	require.Len(t, clonedIndices, 1)
	require.Len(t, clonedIndices[0].Columns, 1)
	require.Equal(t, "_tidb_rowid", clonedIndices[0].Columns[0].Name.O)
}

func TestGetGlobalChecksumIteratorFallsBackToRegularFields(t *testing.T) {
	tableInfo, err := utils.GetTableInfoBySQL(
		"CREATE TABLE `t` (`id` BIGINT PRIMARY KEY, `v` INT, KEY `idx_v`(`v`))",
		parser.New(),
	)
	require.NoError(t, err)

	filteredInfo, _ := utils.ResetColumns(tableInfo.Clone(), []string{"id"})
	src := &TiDBSource{
		tableDiffs: []*common.TableDiff{{
			Schema: "test",
			Table:  "t",
			Info:   filteredInfo,
		}},
		sourceTableMap: map[string]*common.TableSource{
			dbutil.TableName("test", "t"): {
				OriginSchema: "test",
				OriginTable:  "t",
			},
		},
	}

	startRange := &splitter.RangeInfo{
		ChunkRange: &chunk.Range{
			Index: &chunk.CID{
				TableIndex: 0,
				ChunkIndex: 0,
				ChunkCnt:   1,
			},
		},
	}
	iter, chunkCount, err := src.GetGlobalChecksumIterator(context.Background(), 0, startRange)
	require.NoError(t, err)
	require.NotNil(t, iter)
	require.GreaterOrEqual(t, chunkCount, 0)
}

func TestGetGlobalChecksumIteratorReturnsErrorForInvalidConfiguredFallbackFields(t *testing.T) {
	tableInfo, err := utils.GetTableInfoBySQL(
		"CREATE TABLE `t` (`id` BIGINT PRIMARY KEY, `v` INT, KEY `idx_v`(`v`))",
		parser.New(),
	)
	require.NoError(t, err)

	filteredInfo, _ := utils.ResetColumns(tableInfo.Clone(), []string{"id"})
	src := &TiDBSource{
		tableDiffs: []*common.TableDiff{{
			Schema: "test",
			Table:  "t",
			Info:   filteredInfo,
			Fields: "id",
		}},
		sourceTableMap: map[string]*common.TableSource{
			dbutil.TableName("test", "t"): {
				OriginSchema: "test",
				OriginTable:  "t",
			},
		},
	}

	startRange := &splitter.RangeInfo{
		ChunkRange: &chunk.Range{
			Index: &chunk.CID{
				TableIndex: 0,
				ChunkIndex: 0,
				ChunkCnt:   1,
			},
		},
	}
	iter, _, err := src.GetGlobalChecksumIterator(context.Background(), 0, startRange)
	require.Error(t, err)
	require.Nil(t, iter)
	require.Contains(t, err.Error(), "column id")
}
