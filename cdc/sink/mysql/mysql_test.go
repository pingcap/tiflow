// Copyright 2020 PingCAP, Inc.
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

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/charset"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func newMySQLSink4Test(ctx context.Context) *mysqlSink {
	params := defaultParams.Clone()
	params.batchReplaceEnabled = false

	return &mysqlSink{
		txnCache:   newUnresolvedTxnCache(),
		statistics: metrics.NewStatistics(ctx, "", metrics.SinkTypeDB),
		params:     params,
	}
}

func TestPrepareDML(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		input    []*model.RowChangedEvent
		expected *preparedDMLs
	}{
		{
			input: []*model.RowChangedEvent{},
			expected: &preparedDMLs{
				startTs: []model.Ts{},
				sqls:    []string{},
				values:  [][]interface{}{},
			},
		}, {
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813514,
					CommitTs: 418658114257813515,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs:  []model.Ts{418658114257813514},
				sqls:     []string{"DELETE FROM `common_1`.`uk_without_pk` WHERE `a1` = ? AND `a3` = ? LIMIT 1;"},
				values:   [][]interface{}{{1, 1}},
				rowCount: 1,
			},
		}, {
			input: []*model.RowChangedEvent{
				{
					StartTs:  418658114257813516,
					CommitTs: 418658114257813517,
					Table:    &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					IndexColumns: [][]int{{1, 2}},
				},
			},
			expected: &preparedDMLs{
				startTs:  []model.Ts{418658114257813516},
				sqls:     []string{"REPLACE INTO `common_1`.`uk_without_pk`(`a1`,`a3`) VALUES (?,?);"},
				values:   [][]interface{}{{2, 2}},
				rowCount: 1,
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLSink4Test(ctx)
	for _, tc := range testCases {
		dmls := ms.prepareDMLs(tc.input)
		require.Equal(t, tc.expected, dmls)
	}
}

func TestPrepareUpdate(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		quoteTable   string
		preCols      []*model.Column
		cols         []*model.Column
		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			quoteTable:   "`test`.`t1`",
			preCols:      []*model.Column{},
			cols:         []*model.Column{},
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{Name: "b", Type: mysql.TypeVarchar, Flag: 0, Value: "test2"},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? LIMIT 1;",
			expectedArgs: []interface{}{1, "test2", 1},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: "test2",
				},
				{
					Name: "c",
					Type: mysql.TypeLong, Flag: model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? AND `b`=? LIMIT 1;",
			expectedArgs: []interface{}{2, "test2", 1, "test"},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name: "b", Type: mysql.TypeVarchar,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: []byte("世界"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? AND `b`=? LIMIT 1;",
			expectedArgs: []interface{}{2, []byte("世界"), 1, []byte("你好")},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    model.MultipleKeyFlag | model.HandleKeyFlag,
					Charset: charset.CharsetBin,
					Value:   []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 2,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    model.MultipleKeyFlag | model.HandleKeyFlag,
					Charset: charset.CharsetBin,
					Value:   []byte("世界"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? AND `b`=? LIMIT 1;",
			expectedArgs: []interface{}{2, []byte("世界"), 1, []byte("你好")},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    model.MultipleKeyFlag | model.HandleKeyFlag,
					Charset: charset.CharsetGBK,
					Value:   "你好",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 2,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    model.MultipleKeyFlag | model.HandleKeyFlag,
					Charset: charset.CharsetGBK,
					Value:   "世界",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "UPDATE `test`.`t1` SET `a`=?,`b`=? WHERE `a`=? AND `b`=? LIMIT 1;",
			expectedArgs: []interface{}{2, "世界", 1, "你好"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareUpdate(tc.quoteTable, tc.preCols, tc.cols, false)
		fmt.Println(query)
		require.Equal(t, tc.expectedSQL, query)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestPrepareDelete(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		quoteTable   string
		preCols      []*model.Column
		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			quoteTable:   "`test`.`t1`",
			preCols:      []*model.Column{},
			expectedSQL:  "",
			expectedArgs: nil,
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1;",
			expectedArgs: []interface{}{1},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1;",
			expectedArgs: []interface{}{1, "test"},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name: "b", Type: mysql.TypeVarchar,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1;",
			expectedArgs: []interface{}{1, []byte("你好")},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    model.MultipleKeyFlag | model.HandleKeyFlag,
					Charset: charset.CharsetBin,
					Value:   []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1;",
			expectedArgs: []interface{}{1, []byte("你好")},
		},
		{
			quoteTable: "`test`.`t1`",
			preCols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    model.MultipleKeyFlag | model.HandleKeyFlag,
					Charset: charset.CharsetGBK,
					Value:   "你好",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1;",
			expectedArgs: []interface{}{1, "你好"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareDelete(tc.quoteTable, tc.preCols, false)
		require.Equal(t, tc.expectedSQL, query)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestWhereSlice(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		cols             []*model.Column
		forceReplicate   bool
		expectedColNames []string
		expectedArgs     []interface{}
	}{
		{
			cols:             []*model.Column{},
			forceReplicate:   false,
			expectedColNames: nil,
			expectedArgs:     nil,
		},
		{
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			forceReplicate:   false,
			expectedColNames: []string{"a"},
			expectedArgs:     []interface{}{1},
		},
		{
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name: "b", Type: mysql.TypeVarString,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			forceReplicate:   false,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols:             []*model.Column{},
			forceReplicate:   true,
			expectedColNames: []string{},
			expectedArgs:     []interface{}{},
		},
		{
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a"},
			expectedArgs:     []interface{}{1},
		},
		{
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.UniqueKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, "test"},
		},
		{
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarString,
					Flag:  model.MultipleKeyFlag,
					Value: "test",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b", "c"},
			expectedArgs:     []interface{}{1, "test", 100},
		},
		{
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeTinyBlob,
					Flag:  model.MultipleKeyFlag | model.HandleKeyFlag,
					Value: []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			forceReplicate:   false,
			expectedColNames: []string{"a", "b"},
			expectedArgs:     []interface{}{1, []byte("你好")},
		},
		{
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.MultipleKeyFlag,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeTinyBlob,
					Flag:    model.MultipleKeyFlag,
					Charset: charset.CharsetGBK,
					Value:   []byte("你好"),
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Flag:  model.GeneratedColumnFlag,
					Value: 100,
				},
			},
			forceReplicate:   true,
			expectedColNames: []string{"a", "b", "c"},
			expectedArgs:     []interface{}{1, "你好", 100},
		},
	}
	for _, tc := range testCases {
		colNames, args := whereSlice(tc.cols, tc.forceReplicate)
		require.Equal(t, tc.expectedColNames, colNames)
		require.Equal(t, tc.expectedArgs, args)
	}
}

func TestMapReplace(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		quoteTable    string
		cols          []*model.Column
		expectedQuery string
		expectedArgs  []interface{}
	}{
		{
			quoteTable: "`test`.`t1`",
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Value: "varchar",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Value: 1,
					Flag:  model.GeneratedColumnFlag,
				},
				{
					Name:  "d",
					Type:  mysql.TypeTiny,
					Value: uint8(255),
				},
			},
			expectedQuery: "REPLACE INTO `test`.`t1`(`a`,`b`,`d`) VALUES ",
			expectedArgs:  []interface{}{1, "varchar", uint8(255)},
		},
		{
			quoteTable: "`test`.`t1`",
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Value: "varchar",
				},
				{
					Name:  "c",
					Type:  mysql.TypeLong,
					Value: 1,
				},
				{
					Name:  "d",
					Type:  mysql.TypeTiny,
					Value: uint8(255),
				},
			},
			expectedQuery: "REPLACE INTO `test`.`t1`(`a`,`b`,`c`,`d`) VALUES ",
			expectedArgs:  []interface{}{1, "varchar", 1, uint8(255)},
		},
		{
			quoteTable: "`test`.`t1`",
			cols: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Value: 1,
				},
				{
					Name:    "b",
					Type:    mysql.TypeVarchar,
					Charset: charset.CharsetGBK,
					Value:   []byte("你好"),
				},
				{
					Name:    "c",
					Type:    mysql.TypeTinyBlob,
					Charset: charset.CharsetUTF8MB4,
					Value:   []byte("世界"),
				},
				{
					Name:    "d",
					Type:    mysql.TypeMediumBlob,
					Charset: charset.CharsetBin,
					Value:   []byte("你好,世界"),
				},
				{
					Name:  "e",
					Type:  mysql.TypeBlob,
					Value: []byte("你好,世界"),
				},
			},
			expectedQuery: "REPLACE INTO `test`.`t1`(`a`,`b`,`c`,`d`,`e`) VALUES ",
			expectedArgs: []interface{}{
				1, "你好", "世界", []byte("你好,世界"),
				[]byte("你好,世界"),
			},
		},
	}
	for _, tc := range testCases {
		// multiple times to verify the stability of column sequence in query string
		for i := 0; i < 10; i++ {
			query, args := prepareReplace(tc.quoteTable, tc.cols, false, false)
			require.Equal(t, tc.expectedQuery, query)
			require.Equal(t, tc.expectedArgs, args)
		}
	}
}

type sqlArgs [][]interface{}

func (a sqlArgs) Len() int           { return len(a) }
func (a sqlArgs) Less(i, j int) bool { return fmt.Sprintf("%s", a[i]) < fmt.Sprintf("%s", a[j]) }
func (a sqlArgs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func TestReduceReplace(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		replaces   map[string][][]interface{}
		batchSize  int
		sort       bool
		expectSQLs []string
		expectArgs [][]interface{}
	}{
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
				},
			},
			batchSize: 1,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1"},
				{2, "2"},
				{3, "3"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
				},
			},
			batchSize: 3,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3"},
				{4, "3", 5, "5"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
				},
			},
			batchSize: 10,
			sort:      false,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?),(?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3", 4, "3", 5, "5"},
			},
		},
		{
			replaces: map[string][][]interface{}{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES ": {
					[]interface{}{1, "1"},
					[]interface{}{2, "2"},
					[]interface{}{3, "3"},
					[]interface{}{4, "3"},
					[]interface{}{5, "5"},
					[]interface{}{6, "6"},
				},
				"REPLACE INTO `test`.`t2`(`a`,`b`) VALUES ": {
					[]interface{}{7, ""},
					[]interface{}{8, ""},
					[]interface{}{9, ""},
				},
			},
			batchSize: 3,
			sort:      true,
			expectSQLs: []string{
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t1`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
				"REPLACE INTO `test`.`t2`(`a`,`b`) VALUES (?,?),(?,?),(?,?)",
			},
			expectArgs: [][]interface{}{
				{1, "1", 2, "2", 3, "3"},
				{4, "3", 5, "5", 6, "6"},
				{7, "", 8, "", 9, ""},
			},
		},
	}
	for _, tc := range testCases {
		sqls, args := reduceReplace(tc.replaces, tc.batchSize)
		if tc.sort {
			sort.Strings(sqls)
			sort.Sort(sqlArgs(args))
		}
		require.Equal(t, tc.expectSQLs, sqls)
		require.Equal(t, tc.expectArgs, args)
	}
}

func mockTestDB(adjustSQLMode bool) (*sql.DB, error) {
	return mockTestDBWithSQLMode(adjustSQLMode, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE")
}

func mockTestDBWithSQLMode(adjustSQLMode bool, sqlMode interface{}) (*sql.DB, error) {
	// mock for test db, which is used querying TiDB session variable
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, err
	}
	if adjustSQLMode {
		mock.ExpectQuery("SELECT @@SESSION.sql_mode;").
			WillReturnRows(sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
				AddRow(sqlMode))
	}

	columns := []string{"Variable_name", "Value"}
	mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
	)
	mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
	)
	mock.ExpectQuery("show session variables like 'transaction_isolation';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("transaction_isolation", "REPEATED-READ"),
	)
	mock.ExpectQuery("show session variables like 'tidb_placement_mode';").
		WillReturnRows(
			sqlmock.NewRows(columns).
				AddRow("tidb_placement_mode", "IGNORE"),
		)
	mock.ExpectQuery("show session variables like 'tidb_enable_external_ts_read';").
		WillReturnRows(
			sqlmock.NewRows(columns).
				AddRow("tidb_enable_external_ts_read", "OFF"),
		)
	mock.ExpectQuery("select character_set_name from information_schema.character_sets " +
		"where character_set_name = 'gbk';").WillReturnRows(
		sqlmock.NewRows([]string{"character_set_name"}).AddRow("gbk"),
	)

	mock.ExpectClose()
	return db, nil
}

func TestAdjustSQLMode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDBWithSQLMode(true, nil)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	require.Nil(t, err)
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)
	require.Nil(t, err)

	err = sink.Close(ctx)
	require.Nil(t, err)
}

type mockUnavailableMySQL struct {
	listener net.Listener
	quit     chan interface{}
	wg       sync.WaitGroup
}

func newMockUnavailableMySQL(addr string, t *testing.T) *mockUnavailableMySQL {
	s := &mockUnavailableMySQL{
		quit: make(chan interface{}),
	}
	l, err := net.Listen("tcp", addr)
	require.Nil(t, err)
	s.listener = l
	s.wg.Add(1)
	go s.serve(t)
	return s
}

func (s *mockUnavailableMySQL) serve(t *testing.T) {
	defer s.wg.Done()

	for {
		_, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				require.Error(t, err)
			}
		} else {
			s.wg.Add(1)
			go func() {
				// don't read from TCP connection, to simulate database service unavailable
				<-s.quit
				s.wg.Done()
			}()
		}
	}
}

func (s *mockUnavailableMySQL) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func TestNewMySQLTimeout(t *testing.T) {
	addr := "127.0.0.1:33333"
	mockMySQL := newMockUnavailableMySQL(addr, t)
	defer mockMySQL.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(fmt.Sprintf("mysql://%s/?read-timeout=1s&timeout=1s", addr))
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	require.Nil(t, err)
	_, err = NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)
	require.Equal(t, driver.ErrBadConn, errors.Cause(err))
}

func TestNewMySQLSinkExecDML(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t2`(`a`,`b`) VALUES (?,?),(?,?)").
			WithArgs(1, "test", 2, "test").
			WillReturnResult(sqlmock.NewResult(2, 2))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)

	require.Nil(t, err)

	rows := []*model.RowChangedEvent{
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  5,
			CommitTs: 6,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 3,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
	}

	err = sink.EmitRowChangedEvents(ctx, rows...)
	require.Nil(t, err)

	// retry to make sure event is flushed
	err = retry.Do(context.Background(), func() error {
		ts, err := sink.FlushRowChangedEvents(ctx, 1, model.NewResolvedTs(uint64(2)))
		require.Nil(t, err)
		if ts.Ts < uint64(2) {
			return errors.Errorf("checkpoint ts %d less than resolved ts %d", ts.Ts, 2)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))

	require.Nil(t, err)

	err = retry.Do(context.Background(), func() error {
		ts, err := sink.FlushRowChangedEvents(ctx, 2, model.NewResolvedTs(uint64(4)))
		require.Nil(t, err)
		if ts.Ts < uint64(4) {
			return errors.Errorf("checkpoint ts %d less than resolved ts %d", ts.Ts, 4)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))
	require.Nil(t, err)

	err = sink.RemoveTable(ctx, 2)
	require.Nil(t, err)
	v, ok := sink.tableMaxResolvedTs.Load(2)
	require.False(t, ok)
	require.Nil(t, v)
	v, ok = sink.tableCheckpointTs.Load(2)
	require.False(t, ok)
	require.Nil(t, v)

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestExecDMLRollbackErrDatabaseNotExists(t *testing.T) {
	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
			},
		},
	}

	errDatabaseNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrDatabaseNotExists.Code()),
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errDatabaseNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)

	require.Nil(t, err)

	err = sink.execDMLs(ctx, rows, 1 /* bucket */)
	require.Equal(t, errDatabaseNotExists, errors.Cause(err))

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestExecDMLRollbackErrTableNotExists(t *testing.T) {
	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
			},
		},
	}

	errTableNotExists := &dmysql.MySQLError{
		Number: uint16(infoschema.ErrTableNotExists.Code()),
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
			WithArgs(1, 2).
			WillReturnError(errTableNotExists)
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)

	require.Nil(t, err)

	err = sink.execDMLs(ctx, rows, 1 /* bucket */)
	require.Equal(t, errTableNotExists, errors.Cause(err))

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestExecDMLRollbackErrRetryable(t *testing.T) {
	rows := []*model.RowChangedEvent{
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
		{
			Table: &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
			},
		},
	}

	errLockDeadlock := &dmysql.MySQLError{
		Number: mysql.ErrLockDeadlock,
	}

	dbIndex := 0
	mockGetDBConnErrDatabaseNotExists := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		for i := 0; i < int(defaultDMLMaxRetry); i++ {
			mock.ExpectBegin()
			mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?),(?)").
				WithArgs(1, 2).
				WillReturnError(errLockDeadlock)
			mock.ExpectRollback()
		}
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConnErrDatabaseNotExists
	backupMaxRetry := defaultDMLMaxRetry
	defaultDMLMaxRetry = 2
	defer func() {
		GetDBConnImpl = backupGetDBConn
		defaultDMLMaxRetry = backupMaxRetry
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)

	require.Nil(t, err)

	err = sink.execDMLs(ctx, rows, 1 /* bucket */)
	require.Equal(t, errLockDeadlock, errors.Cause(err))

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestMysqlSinkNotRetryErrDupEntry(t *testing.T) {
	errDup := mysql.NewErr(mysql.ErrDupEntry)
	rows := []*model.RowChangedEvent{
		{
			StartTs:       2,
			CommitTs:      3,
			ReplicatingTs: 1,
			Table:         &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
	}

	dbIndex := 0
	mockDBInsertDupEntry := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO `s1`.`t1`(`a`) VALUES (?)").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit().
			WillReturnError(errDup)
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockDBInsertDupEntry
	backupMaxRetry := defaultDMLMaxRetry
	defaultDMLMaxRetry = 2
	defer func() {
		GetDBConnImpl = backupGetDBConn
		defaultDMLMaxRetry = backupMaxRetry
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse(
		"mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&safe-mode=false")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx, model.DefaultChangeFeedID(changefeed), sinkURI, rc)
	require.Nil(t, err)

	err = sink.execDMLs(ctx, rows, 1 /* bucket */)
	require.Equal(t, errDup, errors.Cause(err))

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestNewMySQLSinkExecDDL(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("ALTER TABLE test.t1 ADD COLUMN a int").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("ALTER TABLE test.t1 ADD COLUMN a int").
			WillReturnError(&dmysql.MySQLError{
				Number: uint16(infoschema.ErrColumnExists.Code()),
			})
		mock.ExpectRollback()
		mock.ExpectClose()
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)

	require.Nil(t, err)

	ddl1 := &model.DDLEvent{
		StartTs:  1000,
		CommitTs: 1010,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "test",
				Table:  "t1",
			},
		},
		Type:  timodel.ActionAddColumn,
		Query: "ALTER TABLE test.t1 ADD COLUMN a int",
	}
	err = sink.EmitDDLEvent(ctx, ddl1)
	require.Nil(t, err)
	err = sink.EmitDDLEvent(ctx, ddl1)
	require.Nil(t, err)

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestNeedSwitchDB(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		ddl        *model.DDLEvent
		needSwitch bool
	}{
		{
			&model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: ""},
				},
				Type: timodel.ActionCreateTable,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: "golang"},
				},
				Type: timodel.ActionCreateSchema,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: "golang"},
				},
				Type: timodel.ActionDropSchema,
			},
			false,
		},
		{
			&model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{Schema: "golang"},
				},
				Type: timodel.ActionCreateTable,
			},
			true,
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.needSwitch, needSwitchDB(tc.ddl))
	}
}

func TestNewMySQLSink(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)

	require.Nil(t, err)
	err = sink.Close(ctx)
	require.Nil(t, err)
	// Test idempotency of `Close` interface
	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestNewMySQLSinkWithIPv6Address(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		require.Contains(t, dsnStr, "root@tcp([::1]:3306)")
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := model.DefaultChangeFeedID("test-changefeed")
	// See https://www.ietf.org/rfc/rfc2732.txt, we have to use brackets to wrap IPv6 address.
	sinkURI, err := url.Parse("mysql://[::1]:3306/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		changefeed,
		sinkURI, rc)
	require.Nil(t, err)
	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestMySQLSinkClose(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx := context.Background()

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	// test sink.Close will work correctly even if the ctx pass in has not been cancel
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)

	require.Nil(t, err)
	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestMySQLSinkFlushResolvedTs(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`) VALUES (?)").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t2`(`a`) VALUES (?)").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())

	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()

	// test sink.Close will work correctly even if the ctx pass in has not been cancel
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)
	require.Nil(t, err)
	checkpoint, err := sink.FlushRowChangedEvents(ctx, model.TableID(1), model.NewResolvedTs(1))
	require.Nil(t, err)
	require.True(t, checkpoint.Ts <= 1)
	rows := []*model.RowChangedEvent{
		{
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			CommitTs: 5,
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
	}
	err = sink.EmitRowChangedEvents(ctx, rows...)
	require.Nil(t, err)
	checkpoint, err = sink.FlushRowChangedEvents(ctx, model.TableID(1), model.NewResolvedTs(6))
	require.True(t, checkpoint.Ts <= 6)
	require.Nil(t, err)
	require.True(t, sink.getTableCheckpointTs(model.TableID(1)).Ts <= 6)
	rows = []*model.RowChangedEvent{
		{
			Table:    &model.TableName{Schema: "s1", Table: "t2", TableID: 2},
			CommitTs: 4,
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
			},
		},
	}
	err = sink.EmitRowChangedEvents(ctx, rows...)
	require.Nil(t, err)
	checkpoint, err = sink.FlushRowChangedEvents(ctx, model.TableID(2), model.NewResolvedTs(5))
	require.True(t, checkpoint.Ts <= 5)
	require.Nil(t, err)
	require.True(t, sink.getTableCheckpointTs(model.TableID(2)).Ts <= 5)
	_ = sink.Close(ctx)
	_, err = sink.FlushRowChangedEvents(ctx, model.TableID(2), model.NewResolvedTs(6))
	require.Nil(t, err)

	cancel()
	_, err = sink.FlushRowChangedEvents(ctx, model.TableID(2), model.NewResolvedTs(6))
	require.Regexp(t, ".*context canceled.*", err)
}

func TestGBKSupported(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		mock.ExpectClose()
		require.Nil(t, err)
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	zapcore, logs := observer.New(zap.WarnLevel)
	conf := &log.Config{Level: "warn", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	logger := zap.New(zapcore)
	restoreFn := log.ReplaceGlobals(logger, r)
	defer restoreFn()

	ctx := context.Background()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=4")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)
	require.Nil(t, err)

	// no gbk-related warning log will be output because GBK charset is supported
	require.Equal(t, logs.FilterMessage("gbk charset is not supported").Len(), 0)

	err = sink.Close(ctx)
	require.Nil(t, err)
}

func TestCleanTableResource(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	tblID := model.TableID(1)
	s := &mysqlSink{
		txnCache:   newUnresolvedTxnCache(),
		statistics: metrics.NewStatistics(ctx, "", metrics.SinkTypeDB),
	}
	require.Nil(t, s.EmitRowChangedEvents(ctx, &model.RowChangedEvent{
		Table: &model.TableName{TableID: tblID, Schema: "test", Table: "t1"},
	}))
	s.tableCheckpointTs.Store(tblID, model.NewResolvedTs(uint64(1)))
	s.tableMaxResolvedTs.Store(tblID, model.NewResolvedTs(uint64(2)))
	_, ok := s.txnCache.unresolvedTxns[tblID]
	require.True(t, ok)
	require.Nil(t, s.AddTable(tblID))
	_, ok = s.txnCache.unresolvedTxns[tblID]
	require.False(t, ok)
	_, ok = s.tableCheckpointTs.Load(tblID)
	require.False(t, ok)
	_, ok = s.tableMaxResolvedTs.Load(tblID)
	require.False(t, ok)
}

func TestHolderString(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		count    int
		expected string
	}{
		{1, "?"},
		{2, "?,?"},
		{10, "?,?,?,?,?,?,?,?,?,?"},
	}
	for _, tc := range testCases {
		s := placeHolder(tc.count)
		require.Equal(t, tc.expected, s)
	}
	// test invalid input
	require.Panics(t, func() { placeHolder(0) }, "strings.Builder.Grow: negative count")
	require.Panics(t, func() { placeHolder(-1) }, "strings.Builder.Grow: negative count")
}

func TestMySQLSinkExecDMLError(t *testing.T) {
	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// test db
			db, err := mockTestDB(true)
			require.Nil(t, err)
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `s1`.`t1`(`a`,`b`) VALUES (?,?)").WillDelayFor(1 * time.Second).
			WillReturnError(&dmysql.MySQLError{Number: mysql.ErrNoSuchTable})
		return db, nil
	}
	backupGetDBConn := GetDBConnImpl
	GetDBConnImpl = mockGetDBConn
	defer func() {
		GetDBConnImpl = backupGetDBConn
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeed := "test-changefeed"
	sinkURI, err := url.Parse("mysql://127.0.0.1:4000/?time-zone=UTC&worker-count=1&batch-replace-size=1")
	require.Nil(t, err)
	rc := config.GetDefaultReplicaConfig()
	sink, err := NewMySQLSink(ctx,
		model.DefaultChangeFeedID(changefeed),
		sinkURI, rc)
	require.Nil(t, err)

	rows := []*model.RowChangedEvent{
		{
			StartTs:  1,
			CommitTs: 2,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  2,
			CommitTs: 3,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  3,
			CommitTs: 4,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 3,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  4,
			CommitTs: 5,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 1,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
		{
			StartTs:  5,
			CommitTs: 6,
			Table:    &model.TableName{Schema: "s1", Table: "t1", TableID: 1},
			Columns: []*model.Column{
				{
					Name:  "a",
					Type:  mysql.TypeLong,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
					Value: 2,
				},
				{
					Name:  "b",
					Type:  mysql.TypeVarchar,
					Flag:  0,
					Value: "test",
				},
			},
		},
	}

	for _, row := range rows {
		err = sink.EmitRowChangedEvents(ctx, row)
		require.NoError(t, err)
	}

	i := 0
	for {
		// Keep flushing so that appendFinishTxn is called enough times.
		ts, err := sink.FlushRowChangedEvents(ctx, 1, model.NewResolvedTs(uint64(i)))
		if err != nil {
			break
		}
		require.Less(t, ts.Ts, uint64(2))
		i++
	}

	err = sink.RemoveTable(ctx, 1)
	require.Error(t, err)
	require.Regexp(t, ".*ErrMySQLTxnError.*", err)

	_ = sink.Close(ctx)
}

func TestMysqlSinkSafeModeOff(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    []*model.RowChangedEvent
		expected *preparedDMLs
	}{
		{
			name:  "empty",
			input: []*model.RowChangedEvent{},
			expected: &preparedDMLs{
				startTs: []model.Ts{},
				sqls:    []string{},
				values:  [][]interface{}{},
			},
		}, {
			name: "insert without PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813514,
					CommitTs:      418658114257813515,
					ReplicatingTs: 418658114257813513,
					Table:         &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813514},
				sqls: []string{
					"INSERT INTO `common_1`.`uk_without_pk`(`a1`,`a3`) VALUES (?,?);",
				},
				values:   [][]interface{}{{1, 1}},
				rowCount: 1,
			},
		}, {
			name: "insert with PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813514,
					CommitTs:      418658114257813515,
					ReplicatingTs: 418658114257813513,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 1,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 1,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs:  []model.Ts{418658114257813514},
				sqls:     []string{"INSERT INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);"},
				values:   [][]interface{}{{1, 1}},
				rowCount: 1,
			},
		}, {
			name: "update without PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					Table:         &model.TableName{Schema: "common_1", Table: "uk_without_pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"UPDATE `common_1`.`uk_without_pk` SET `a1`=?,`a3`=? " +
						"WHERE `a1`=? AND `a3`=? LIMIT 1;",
				},
				values:   [][]interface{}{{3, 3, 2, 2}},
				rowCount: 1,
			},
		}, {
			name: "update with PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					PreColumns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 2,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 2,
					}},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{"UPDATE `common_1`.`pk` SET `a1`=?,`a3`=? " +
					"WHERE `a1`=? AND `a3`=? LIMIT 1;"},
				values:   [][]interface{}{{3, 3, 2, 2}},
				rowCount: 1,
			},
		}, {
			name: "batch insert with PK",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813515,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 5,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 5,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"INSERT INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
					"INSERT INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
				},
				values:   [][]interface{}{{3, 3}, {5, 5}},
				rowCount: 2,
			},
		}, {
			name: "safe mode on commit ts < replicating ts",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813518,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516},
				sqls: []string{
					"REPLACE INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
				},
				values:   [][]interface{}{{3, 3}},
				rowCount: 1,
			},
		}, {
			name: "safe mode on one row commit ts < replicating ts",
			input: []*model.RowChangedEvent{
				{
					StartTs:       418658114257813516,
					CommitTs:      418658114257813517,
					ReplicatingTs: 418658114257813518,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 3,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 3,
					}},
				},
				{
					StartTs:       418658114257813506,
					CommitTs:      418658114257813507,
					ReplicatingTs: 418658114257813505,
					Table:         &model.TableName{Schema: "common_1", Table: "pk"},
					Columns: []*model.Column{nil, {
						Name:  "a1",
						Type:  mysql.TypeLong,
						Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
						Value: 5,
					}, {
						Name:  "a3",
						Type:  mysql.TypeLong,
						Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
						Value: 5,
					}},
				},
			},
			expected: &preparedDMLs{
				startTs: []model.Ts{418658114257813516, 418658114257813506},
				sqls: []string{
					"REPLACE INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
					"REPLACE INTO `common_1`.`pk`(`a1`,`a3`) VALUES (?,?);",
				},
				values:   [][]interface{}{{3, 3}, {5, 5}},
				rowCount: 2,
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ms := newMySQLSink4Test(ctx)
	ms.params.safeMode = false
	ms.params.enableOldValue = true
	for _, tc := range testCases {
		dmls := ms.prepareDMLs(tc.input)
		require.Equal(t, tc.expected, dmls, tc.name)
	}
}
