// Copyright 2022 PingCAP, Inc.
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

package verification

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCompareCheckSum(t *testing.T) {
	tests := []struct {
		name          string
		wantSource    map[string]string
		wantSink      map[string]string
		wantDBErr     error
		wantSourceErr error
		wantSinkErr   error
		wantRet       bool
	}{
		{
			name:       "happy",
			wantSource: map[string]string{"t1": "1", "t2": "2"},
			wantSink:   map[string]string{"t1": "1", "t2": "2"},
			wantRet:    true,
		},
		{
			name:       "getAllDBs err",
			wantSource: map[string]string{"t1": "1", "t2": "2"},
			wantSink:   map[string]string{"t1": "1", "t2": "2"},
			wantDBErr:  errors.New("db err"),
		},
		{
			name:          "upstreamChecker err",
			wantSource:    map[string]string{"t1": "1", "t2": "2"},
			wantSink:      map[string]string{"t1": "1", "t2": "2"},
			wantSourceErr: errors.New("upstreamChecker err"),
		},
		{
			name:        "downstreamChecker err",
			wantSource:  map[string]string{"t1": "1", "t2": "2"},
			wantSink:    map[string]string{"t1": "1", "t2": "2"},
			wantSinkErr: errors.New("upstreamChecker err"),
		},
		{
			name:       "not match",
			wantSource: map[string]string{"t1": "1", "t2": "2"},
			wantSink:   map[string]string{"t1": "11", "t2": "21"},
		},
	}
	for _, tt := range tests {
		mockUpChecker := &mockCheckSumChecker{}
		mockUpChecker.On("getAllDBs", mock.Anything).Return([]string{"1", "2"}, tt.wantDBErr)
		mockUpChecker.On("getCheckSum", mock.Anything, mock.Anything, mock.Anything).Return(tt.wantSource, tt.wantSourceErr)
		mockDownChecker := &mockCheckSumChecker{}
		mockDownChecker.On("getCheckSum", mock.Anything, mock.Anything, mock.Anything).Return(tt.wantSink, tt.wantSinkErr)
		f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
		require.Nil(t, err)
		ret, err := compareCheckSum(context.Background(), mockUpChecker, mockDownChecker, f)
		require.Equal(t, tt.wantRet, ret, tt.name)
		if tt.wantDBErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantDBErr), tt.name)
		} else if tt.wantSinkErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantSinkErr), tt.name)
		} else if tt.wantSourceErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantSourceErr), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}

func TestGetAllDBs(t *testing.T) {
	type args struct {
		dbs []string
	}
	tests := []struct {
		name    string
		args    args
		wanRet  []string
		wantErr error
	}{
		{
			name:   "happy",
			wanRet: []string{"d1", "d2"},
			args:   args{dbs: []string{"d1", "d2"}},
		},
		{
			name:    "err",
			args:    args{dbs: []string{"d1", "d2"}},
			wantErr: errors.New("err"),
		},
	}
	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mockDB.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"Database"}).AddRow(tt.args.dbs[0]).AddRow(tt.args.dbs[1])).WillReturnError(tt.wantErr)
		c := &checker{
			db: db,
		}
		ret, err := c.getAllDBs(context.Background())
		require.Equal(t, tt.wanRet, ret, tt.name)
		require.True(t, errors.ErrorEqual(err, tt.wantErr), tt.name)
	}
}

func TestGetColumns(t *testing.T) {
	type args struct {
		columns []columnInfo
	}
	tests := []struct {
		name    string
		args    args
		wanRet  []columnInfo
		wantErr error
	}{
		{
			name:   "happy",
			wanRet: []columnInfo{{Field: "f"}},
			args:   args{columns: []columnInfo{{Field: "f"}}},
		},
		{
			name:    "err",
			args:    args{columns: []columnInfo{{Field: "f"}}},
			wantErr: errors.New("err"),
		},
	}
	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mockDB.ExpectQuery(fmt.Sprintf("SHOW COLUMNS FROM %s", "t")).WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).AddRow(tt.args.columns[0].Field, tt.args.columns[0].Type, tt.args.columns[0].Null, tt.args.columns[0].Key, tt.args.columns[0].Default, tt.args.columns[0].Extra)).WillReturnError(tt.wantErr)
		c := &checker{
			db: db,
		}
		ret, err := c.getColumns(context.Background(), "t")
		require.Equal(t, tt.wanRet, ret, tt.name)
		require.True(t, errors.ErrorEqual(err, tt.wantErr), tt.name)
	}
}

func TestDoChecksum(t *testing.T) {
	type args struct {
		columns []columnInfo
	}
	tests := []struct {
		name    string
		args    args
		wanRet  string
		wantErr error
	}{
		{
			name:   "happy",
			wanRet: "123",
			args:   args{columns: []columnInfo{{Field: "f"}}},
		},
		{
			name:    "err",
			args:    args{columns: []columnInfo{{Field: "f"}}},
			wantErr: errors.New("err"),
		},
	}
	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)

		concat := fmt.Sprintf("CONCAT_WS(',', %s, %s)", "f", "ISNULL(f)")
		query := fmt.Sprintf("SELECT BIT_XOR(CAST(crc32(%s) AS UNSIGNED)) AS checksum FROM %s", concat, "t")

		mockDB.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"checksum"}).AddRow(tt.wanRet)).WillReturnError(tt.wantErr)
		c := &checker{
			db: db,
		}
		ret, err := c.doChecksum(context.Background(), tt.args.columns, "d", "t")
		require.True(t, errors.ErrorEqual(err, tt.wantErr), tt.name)
		require.Equal(t, tt.wanRet, ret, tt.name)
	}
}

func TestGetAllTables(t *testing.T) {
	type args struct {
		tables       []string
		filterConfig *config.ReplicaConfig
	}
	tests := []struct {
		name    string
		args    args
		wanRet  []string
		wantErr error
	}{
		{
			name:   "happy",
			wanRet: []string{"t1", "t2"},
			args:   args{tables: []string{"t1", "t2"}, filterConfig: config.GetDefaultReplicaConfig()},
		},
		{
			name:   "happy filter enable",
			wanRet: []string{"t2"},
			args: args{
				tables: []string{"t1", "t2"},
				filterConfig: &config.ReplicaConfig{
					Filter: &config.FilterConfig{
						Rules: []string{"*.*", "!d.t1"},
					},
				},
			},
		},
		{
			name:    "err",
			args:    args{tables: []string{"t1", "t2"}, filterConfig: config.GetDefaultReplicaConfig()},
			wantErr: errors.New("err"),
		},
	}
	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mockDB.ExpectQuery("SHOW TABLES").WillReturnRows(sqlmock.NewRows([]string{"TABLES"}).AddRow(tt.args.tables[0]).AddRow(tt.args.tables[1])).WillReturnError(tt.wantErr)
		c := &checker{
			db: db,
		}
		f, err := filter.NewFilter(tt.args.filterConfig, "")
		require.Nil(t, err, tt.name)
		ret, err := c.getAllTables(context.Background(), "d", f)
		require.Equal(t, tt.wanRet, ret, tt.name)
		require.True(t, errors.ErrorEqual(err, tt.wantErr), tt.name)
	}
}

func TestGetCheckSum(t *testing.T) {
	type args struct {
		tables []string
	}
	tests := []struct {
		name            string
		args            args
		wantRet         map[string]string
		wantDBErr       error
		wantTableErr    error
		wantColumnErr   error
		wantCheckSumErr error
	}{
		{
			name: "happy",
			args: args{
				tables: []string{"t1"},
			},
			wantRet: map[string]string{"t1": "11"},
		},
		{
			name: "db err",
			args: args{
				tables: []string{"t1"},
			},
			wantDBErr: errors.New("db"),
		},
		{
			name: "table err",
			args: args{
				tables: []string{"t1"},
			},
			wantTableErr: errors.New("table"),
		},
		{
			name: "column err",
			args: args{
				tables: []string{"t1"},
			},
			wantColumnErr: errors.New("column"),
			wantRet:       map[string]string{},
		},
		{
			name: "check err",
			args: args{
				tables: []string{"t1"},
			},
			wantRet:         map[string]string{},
			wantCheckSumErr: errors.New("check"),
		},
	}

	for _, tt := range tests {
		db, mockDB, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mockDB.ExpectExec("USE db").WillReturnResult(sqlmock.NewResult(1, 1)).WillReturnError(tt.wantDBErr)
		mockDB.ExpectQuery("SHOW TABLES").WillReturnRows(sqlmock.NewRows([]string{"TABLES"}).AddRow(tt.args.tables[0])).WillReturnError(tt.wantTableErr)
		mockDB.ExpectQuery(fmt.Sprintf("SHOW COLUMNS FROM %s", tt.args.tables[0])).WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).AddRow("c", "t", "n", "k", "d", "e")).WillReturnError(tt.wantColumnErr)
		concat := fmt.Sprintf("CONCAT_WS(',', %s, %s)", "c", "ISNULL(c)")
		query := fmt.Sprintf("SELECT BIT_XOR(CAST(crc32(%s) AS UNSIGNED)) AS checksum FROM %s", concat, tt.args.tables[0])
		mockDB.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"checksum"}).AddRow(tt.wantRet[tt.args.tables[0]])).WillReturnError(tt.wantCheckSumErr)
		c := &checker{
			db: db,
		}
		f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
		require.Nil(t, err)
		ret, err := c.getCheckSum(context.Background(), "db", f)
		require.Equal(t, tt.wantRet, ret, tt.name)
		if tt.wantDBErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantDBErr), tt.name)
		} else if tt.wantTableErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantTableErr), tt.name)
		} else if tt.wantColumnErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantColumnErr), tt.name)
		} else if tt.wantCheckSumErr != nil {
			require.True(t, errors.ErrorEqual(err, tt.wantCheckSumErr), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}
