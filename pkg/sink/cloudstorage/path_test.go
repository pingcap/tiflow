// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func testFilePathGenerator(ctx context.Context, t *testing.T, dir string) *FilePathGenerator {
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", dir)
	storage, err := util.GetExternalStorageFromURI(ctx, uri)
	require.NoError(t, err)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = util.AddressOf(config.DateSeparatorNone.String())
	replicaConfig.Sink.Protocol = util.AddressOf(config.ProtocolOpen.String())
	replicaConfig.Sink.FileIndexWidth = util.AddressOf(6)
	cfg := NewConfig()
	err = cfg.Apply(ctx, sinkURI, replicaConfig)
	require.NoError(t, err)

	f := NewFilePathGenerator(cfg, storage, ".json", clock.New())
	return f
}

func TestGenerateDataFilePath(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	table := VersionedTableName{
		TableNameWithPhysicTableID: model.TableName{
			Schema: "test",
			Table:  "table1",
		},
		TableInfoVersion: 5,
	}

	dir := t.TempDir()
	f := testFilePathGenerator(ctx, t, dir)
	f.versionMap[table] = table.TableInfoVersion
	date := f.GenerateDateStr()
	// date-separator: none
	path, err := f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/CDC000001.json", path)
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/CDC000002.json", path)

	// date-separator: year
	mockClock := clock.NewMock()
	f = testFilePathGenerator(ctx, t, dir)
	f.versionMap[table] = table.TableInfoVersion
	f.config.DateSeparator = config.DateSeparatorYear.String()
	f.clock = mockClock
	mockClock.Set(time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC))
	date = f.GenerateDateStr()
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2022/CDC000001.json", path)
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2022/CDC000002.json", path)
	// year changed
	mockClock.Set(time.Date(2023, 1, 1, 0, 0, 20, 0, time.UTC))
	date = f.GenerateDateStr()
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023/CDC000001.json", path)
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023/CDC000002.json", path)

	// date-separator: month
	mockClock = clock.NewMock()
	f = testFilePathGenerator(ctx, t, dir)
	f.versionMap[table] = table.TableInfoVersion
	f.config.DateSeparator = config.DateSeparatorMonth.String()
	f.clock = mockClock
	mockClock.Set(time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC))
	date = f.GenerateDateStr()
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2022-12/CDC000001.json", path)
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2022-12/CDC000002.json", path)
	// month changed
	mockClock.Set(time.Date(2023, 1, 1, 0, 0, 20, 0, time.UTC))
	date = f.GenerateDateStr()
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023-01/CDC000001.json", path)
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023-01/CDC000002.json", path)

	// date-separator: day
	mockClock = clock.NewMock()
	f = testFilePathGenerator(ctx, t, dir)
	f.versionMap[table] = table.TableInfoVersion
	f.config.DateSeparator = config.DateSeparatorDay.String()
	f.clock = mockClock
	mockClock.Set(time.Date(2022, 12, 31, 23, 59, 59, 0, time.UTC))
	date = f.GenerateDateStr()
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2022-12-31/CDC000001.json", path)
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2022-12-31/CDC000002.json", path)
	// day changed
	mockClock.Set(time.Date(2023, 1, 1, 0, 0, 20, 0, time.UTC))
	date = f.GenerateDateStr()
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023-01-01/CDC000001.json", path)
	path, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023-01-01/CDC000002.json", path)
}

func TestFetchIndexFromFileName(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	dir := t.TempDir()
	f := testFilePathGenerator(ctx, t, dir)
	testCases := []struct {
		fileName string
		wantErr  string
	}{
		{
			fileName: "CDC000011.json",
			wantErr:  "",
		},
		{
			fileName: "CDC1000000.json",
			wantErr:  "",
		},
		{
			fileName: "CDC1.json",
			wantErr:  "filename in storage sink is invalid",
		},
		{
			fileName: "cdc000001.json",
			wantErr:  "filename in storage sink is invalid",
		},
		{
			fileName: "CDC000005.xxx",
			wantErr:  "filename in storage sink is invalid",
		},
		{
			fileName: "CDChello.json",
			wantErr:  "filename in storage sink is invalid",
		},
	}

	for _, tc := range testCases {
		_, err := f.fetchIndexFromFileName(tc.fileName)
		if len(tc.wantErr) != 0 {
			require.Contains(t, err.Error(), tc.wantErr)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestGenerateDataFilePathWithIndexFile(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	dir := t.TempDir()
	f := testFilePathGenerator(ctx, t, dir)
	mockClock := clock.NewMock()
	f.config.DateSeparator = config.DateSeparatorDay.String()
	f.clock = mockClock
	mockClock.Set(time.Date(2023, 3, 9, 23, 59, 59, 0, time.UTC))
	table := VersionedTableName{
		TableNameWithPhysicTableID: model.TableName{
			Schema: "test",
			Table:  "table1",
		},
		TableInfoVersion: 5,
	}
	f.versionMap[table] = table.TableInfoVersion
	date := f.GenerateDateStr()
	indexFilePath := f.GenerateIndexFilePath(table, date)
	err := f.storage.WriteFile(ctx, indexFilePath, []byte("CDC000005.json\n"))
	require.NoError(t, err)

	// index file exists, but the file is not exist
	dataFilePath, err := f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023-03-09/CDC000005.json", dataFilePath)

	// cleanup cached file index
	delete(f.fileIndex, table)
	// index file exists, and the file is empty
	err = f.storage.WriteFile(ctx, dataFilePath, []byte(""))
	require.NoError(t, err)
	dataFilePath, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023-03-09/CDC000005.json", dataFilePath)

	// cleanup cached file index
	delete(f.fileIndex, table)
	// index file exists, and the file is not empty
	err = f.storage.WriteFile(ctx, dataFilePath, []byte("test"))
	require.NoError(t, err)
	dataFilePath, err = f.GenerateDataFilePath(ctx, table, date)
	require.NoError(t, err)
	require.Equal(t, "test/table1/5/2023-03-09/CDC000006.json", dataFilePath)
}

func TestIsSchemaFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		expect bool
	}{
		{
			"valid database schema <schema>/meta/",
			"schema2/meta/schema_123_0123456789.json", true,
		},
		{
			"valid table schema <schema>/<table>/meta/",
			"schema1/table1/meta/schema_123_0123456789.json", true,
		},
		{"valid special prefix", "meta/meta/schema_123_0123456789.json", true},
		{"valid schema1", "meta/schema_123_0123456789.json", true},
		{"missing field1", "meta/schema_012345678_.json", false},
		{"missing field2", "meta/schema_012345678.json", false},
		{"invalid checksum1", "meta/schema_123_012345678.json", false},
		{"invalid checksum2", "meta/schema_123_012a4567c9.json", false},
		{"invalid table version", "meta/schema_abc_0123456789.json", false},
		{"invalid extension1", "meta/schema_123_0123456789.txt", false},
		{"invalid extension2", "meta/schema_123_0123456789.json ", false},
		{"invalid path", "meta/schema1/schema_123_0123456789.json", false},
	}

	for _, tt := range tests {
		require.Equal(t, tt.expect, IsSchemaFile(tt.path),
			"testCase: %s, path: %v", tt.name, tt.path)
	}
}
