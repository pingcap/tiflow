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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/source"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/stretchr/testify/require"
)

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

func TestSQLCommentKeepsMetadataInert(t *testing.T) {
	sql := sqlComment("table: test\r\n; DROP DATABASE prod;\n/*! SELECT 1 */") + "SELECT 1;"
	stmts, _, err := parser.New().ParseSQL(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	_, ok := stmts[0].(*ast.SelectStmt)
	require.True(t, ok)
	require.True(t, strings.HasPrefix(sql, "-- table:"))
}

type sqlFileTestSource struct {
	source.Source
	tables []*common.TableDiff
}

func (s *sqlFileTestSource) GetTables() []*common.TableDiff { return s.tables }

func TestWriteSQLsEscapesPathSeparators(t *testing.T) {
	fixDir := filepath.Join(t.TempDir(), "fix-sql")
	require.NoError(t, os.Mkdir(fixDir, 0o700))
	cp := new(checkpoints.Checkpoint)
	cp.Init()
	df := &Diff{
		FixSQLDir: fixDir,
		downstream: &sqlFileTestSource{tables: []*common.TableDiff{
			{Schema: "../%2Fschema", Table: `..\table`},
		}},
		sqlCh: make(chan *ChunkDML, 1),
		cp:    cp,
	}
	df.sqlCh <- &ChunkDML{
		node: &checkpoints.Node{ChunkRange: &chunk.Range{Index: &chunk.CID{}}},
		sqls: []string{"REPLACE INTO t VALUES (1);"},
	}
	close(df.sqlCh)
	df.sqlWg.Add(1)
	df.writeSQLs(context.Background())

	entries, err := os.ReadDir(fixDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "..%2F%252Fschema:..%5Ctable:0:0-0:0.sql", entries[0].Name())
}
