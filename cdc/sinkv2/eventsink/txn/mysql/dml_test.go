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

package mysql

import (
	"fmt"
	"sort"
	"testing"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

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
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? LIMIT 1",
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
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? AND `b` = ? LIMIT 1",
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
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? AND `b` = ? LIMIT 1",
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
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? AND `b` = ? LIMIT 1",
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
			expectedSQL:  "UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? AND `b` = ? LIMIT 1",
			expectedArgs: []interface{}{2, "世界", 1, "你好"},
		},
	}
	for _, tc := range testCases {
		query, args := prepareUpdate(tc.quoteTable, tc.preCols, tc.cols, false)
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
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? LIMIT 1",
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
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1",
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
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1",
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
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1",
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
			expectedSQL:  "DELETE FROM `test`.`t1` WHERE `a` = ? AND `b` = ? LIMIT 1",
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
			expectedQuery: "REPLACE INTO `test`.`t1` (`a`,`b`,`d`) VALUES ",
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
			expectedQuery: "REPLACE INTO `test`.`t1` (`a`,`b`,`c`,`d`) VALUES ",
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
			expectedQuery: "REPLACE INTO `test`.`t1` (`a`,`b`,`c`,`d`,`e`) VALUES ",
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
