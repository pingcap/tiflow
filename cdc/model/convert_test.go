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

package model

import (
	"testing"

	"github.com/pingcap/tidb/parser/charset"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestRowRedoConvert(t *testing.T) {
	t.Parallel()
	row := &RowChangedEvent{
		StartTs:  100,
		CommitTs: 120,
		Table:    &TableName{Schema: "test", Table: "table1", TableID: 57},
		PreColumns: []*Column{{
			Name:  "a1",
			Type:  mysql.TypeLong,
			Flag:  BinaryFlag | MultipleKeyFlag | HandleKeyFlag,
			Value: int64(1),
		}, {
			Name:  "a2",
			Type:  mysql.TypeVarchar,
			Value: []byte("char"),
		}, {
			Name:  "a3",
			Type:  mysql.TypeLong,
			Flag:  BinaryFlag | MultipleKeyFlag | HandleKeyFlag,
			Value: int64(1),
		}, {
			Name:    "a4",
			Type:    mysql.TypeTinyBlob,
			Charset: charset.CharsetGBK,
			Value:   []byte("你好"),
		}, nil},
		Columns: []*Column{{
			Name:  "a1",
			Type:  mysql.TypeLong,
			Flag:  BinaryFlag | MultipleKeyFlag | HandleKeyFlag,
			Value: int64(2),
		}, {
			Name:  "a2",
			Type:  mysql.TypeVarchar,
			Value: []byte("char-updated"),
		}, {
			Name:  "a3",
			Type:  mysql.TypeLong,
			Flag:  BinaryFlag | MultipleKeyFlag | HandleKeyFlag,
			Value: int64(2),
		}, {
			Name:    "a4",
			Type:    mysql.TypeTinyBlob,
			Charset: charset.CharsetGBK,
			Value:   []byte("世界"),
		}, nil},
		IndexColumns: [][]int{{1, 3}},
	}
	rowRedo := RowToRedo(row)
	require.Equal(t, 5, len(rowRedo.PreColumns))
	require.Equal(t, 5, len(rowRedo.Columns))

	redoLog := &RedoLog{
		RedoRow: rowRedo,
		Type:    RedoLogTypeRow,
	}
	data, err := redoLog.MarshalMsg(nil)
	require.Nil(t, err)
	redoLog2 := &RedoLog{}
	_, err = redoLog2.UnmarshalMsg(data)
	require.Nil(t, err)
	require.Equal(t, row, LogToRow(redoLog2.RedoRow))
}

func TestRowRedoConvertWithEmptySlice(t *testing.T) {
	t.Parallel()
	row := &RowChangedEvent{
		StartTs:  100,
		CommitTs: 120,
		Table:    &TableName{Schema: "test", Table: "table1", TableID: 57},
		PreColumns: []*Column{{
			Name:  "a1",
			Type:  mysql.TypeLong,
			Flag:  BinaryFlag | MultipleKeyFlag | HandleKeyFlag,
			Value: int64(1),
		}, {
			Name:  "a2",
			Type:  mysql.TypeVarchar,
			Value: []byte(""), // empty slice should be marshal and unmarshal safely
		}},
		Columns: []*Column{{
			Name:  "a1",
			Type:  mysql.TypeLong,
			Flag:  BinaryFlag | MultipleKeyFlag | HandleKeyFlag,
			Value: int64(2),
		}, {
			Name:  "a2",
			Type:  mysql.TypeVarchar,
			Value: []byte(""),
		}},
		IndexColumns: [][]int{{1}},
	}
	rowRedo := RowToRedo(row)
	redoLog := &RedoLog{
		RedoRow: rowRedo,
		Type:    RedoLogTypeRow,
	}
	data, err := redoLog.MarshalMsg(nil)
	require.Nil(t, err)

	redoLog2 := &RedoLog{}
	_, err = redoLog2.UnmarshalMsg(data)
	require.Nil(t, err)
	require.Equal(t, row, LogToRow(redoLog2.RedoRow))
}

func TestDDLRedoConvert(t *testing.T) {
	t.Parallel()
	ddl := &DDLEvent{
		StartTs:   1020,
		CommitTs:  1030,
		Type:      timodel.ActionAddColumn,
		Query:     "ALTER TABLE test.t1 ADD COLUMN a int",
		TableInfo: &TableInfo{},
	}
	redoDDL := DDLToRedo(ddl)

	redoLog := &RedoLog{
		RedoDDL: redoDDL,
		Type:    RedoLogTypeDDL,
	}
	data, err := redoLog.MarshalMsg(nil)
	require.Nil(t, err)
	redoLog2 := &RedoLog{}
	_, err = redoLog2.UnmarshalMsg(data)
	require.Nil(t, err)
	require.Equal(t, ddl, LogToDDL(redoLog2.RedoDDL))
}
