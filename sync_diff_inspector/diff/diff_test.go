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

package diff

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/pingcap/tiflow/sync_diff_inspector/splitter"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type metadataLogSource struct {
	source.Source
	db       *sql.DB
	table    *common.TableDiff
	checksum uint64
}

func (s *metadataLogSource) GetDB() *sql.DB {
	return s.db
}

func (s *metadataLogSource) GetTables() []*common.TableDiff {
	return []*common.TableDiff{s.table}
}

func (s *metadataLogSource) GetCountAndMD5(context.Context, *splitter.RangeInfo) *source.ChecksumInfo {
	return &source.ChecksumInfo{Count: splitter.SplitThreshold, Checksum: s.checksum}
}

func TestBinGenerateColumnMetadataRedaction(t *testing.T) {
	original := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() { dmlog.SetRedactLog(original) })

	tableInfo, err := utils.GetTableInfoBySQL("CREATE TABLE `test`.`test` ("+
		"`a` varchar(100) DEFAULT 'private-default', "+
		"`b` varchar(200) AS (concat(`a`, 'private-generated')) STORED, "+
		"`c` enum('private-enum', 'public'), UNIQUE KEY (`a`, `b`, `c`))", parser.New())
	require.NoError(t, err)
	columns, err := json.Marshal(tableInfo.Columns)
	require.NoError(t, err)
	for _, literal := range []string{"private-default", "private-generated", "private-enum"} {
		require.Contains(t, string(columns), literal)
	}
	table := &common.TableDiff{Schema: "test_schema", Table: "test_table", Info: tableInfo, Range: "TRUE"}

	for _, redact := range []bool{false, true} {
		t.Run(fmt.Sprintf("redact=%t", redact), func(t *testing.T) {
			dmlog.SetRedactLog(redact)
			core, observed := observer.New(zap.DebugLevel)
			restore := log.ReplaceGlobals(zap.New(core), nil)
			t.Cleanup(restore)

			conn, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer conn.Close()
			mock.ExpectQuery("SELECT `a`, `b`, `c` FROM `test_schema`.`test_table` WHERE (TRUE) AND (TRUE) ORDER BY `a`, `b`, `c` LIMIT 1 OFFSET 1000").
				WillReturnRows(sqlmock.NewRows([]string{"a", "b", "c"}).AddRow("mid-value", "mid-generated", "private-enum"))

			upstream := &metadataLogSource{db: conn, table: table, checksum: 1}
			downstream := &metadataLogSource{table: table, checksum: 2}
			df := &Diff{upstream: upstream, downstream: downstream, workSource: upstream}
			tableRange := &splitter.RangeInfo{ChunkRange: chunk.NewChunkRange(tableInfo), IndexID: tableInfo.Indices[0].ID}
			result, err := df.BinGenerate(context.Background(), upstream, tableRange, 2*splitter.SplitThreshold)
			require.NoError(t, err)
			require.Same(t, tableRange, result)
			require.NoError(t, mock.ExpectationsWereMet())
			unchangedColumns, err := json.Marshal(tableInfo.Columns)
			require.NoError(t, err)
			require.Equal(t, columns, unchangedColumns)

			entries := observed.FilterMessage("mid values").All()
			require.Len(t, entries, 1)
			if redact {
				require.Equal(t, "?", entries[0].ContextMap()["indices"])
				encodedLogs, err := json.Marshal(observed.All())
				require.NoError(t, err)
				for _, literal := range []string{"private-default", "private-generated", "private-enum"} {
					require.NotContains(t, string(encodedLogs), literal)
				}
			} else {
				require.Equal(t, zap.Reflect("indices", tableInfo.Columns), entries[0].Context[1])
				encodedColumns, err := json.Marshal(entries[0].ContextMap()["indices"])
				require.NoError(t, err)
				require.Equal(t, columns, encodedColumns)
			}
		})
	}
}

func TestGetSnapshot(t *testing.T) {
	cases := []struct {
		latestSnapshot []string
		snapshot       string
		expected       string
		snapshotRows   string
	}{
		{
			latestSnapshot: []string{},
			snapshot:       "1",
			expected:       "1",
		},
		{
			latestSnapshot: []string{"2"},
			snapshot:       "",
			expected:       "2",
		},
		{
			latestSnapshot: []string{"0"},
			snapshot:       "3",
			expected:       "3",
		},
		{
			latestSnapshot: []string{"4"},
			snapshot:       "0",
			expected:       "0",
		},
		{
			latestSnapshot: []string{"5"},
			snapshot:       "6",
			expected:       "5",
		},
		{
			latestSnapshot: []string{"7"},
			snapshot:       "6",
			expected:       "6",
		},
		{
			// 2017-10-07 16:45:26
			latestSnapshot: []string{"395146933305344000"},
			snapshot:       "2017-10-08 16:45:26",
			expected:       "395146933305344000",
			snapshotRows:   "1507452326",
		},
		{
			// 2017-10-07 16:45:26
			latestSnapshot: []string{"395146933305344000"},
			snapshot:       "2017-10-06 16:45:26",
			expected:       "2017-10-06 16:45:26",
			snapshotRows:   "1507279526",
		},
		{
			latestSnapshot: []string{"1"},
			snapshot:       "2017-10-06 16:45:26",
			expected:       "1",
			snapshotRows:   "1507279526",
		},
		{
			latestSnapshot: []string{"395146933305344000"},
			snapshot:       "1",
			expected:       "1",
		},
		{
			// 2090-11-19 22:07:45
			latestSnapshot: []string{"1000022649077760000"},
			snapshot:       "2090-11-18 22:07:45",
			expected:       "2090-11-18 22:07:45",
			snapshotRows:   "3814697265",
		},
	}

	conn, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer conn.Close()

	for i, cs := range cases {
		if len(cs.snapshotRows) > 0 {
			dataRows := sqlmock.NewRows([]string{""}).AddRow(cs.snapshotRows)
			mock.ExpectQuery("SELECT unix_timestamp(?)").WillReturnRows(dataRows)
		}
		val := GetSnapshot(cs.latestSnapshot, cs.snapshot, conn)
		require.Equal(t, cs.expected, val, "case %d", i)
	}
}
