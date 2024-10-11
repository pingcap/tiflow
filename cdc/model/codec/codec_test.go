// Copyright 2023 PingCAP, Inc.
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

package codec

import (
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	codecv1 "github.com/pingcap/tiflow/cdc/model/codec/v1"
	"github.com/stretchr/testify/require"
)

func TestV1toV2(t *testing.T) {
	var msg1 []byte
	var rv1 *codecv1.RedoLog
	var rv2, rv2Gen *model.RedoLog
	var err error

	rv1 = &codecv1.RedoLog{
		RedoRow: &codecv1.RedoRowChangedEvent{
			Row: &codecv1.RowChangedEvent{
				StartTs:  1,
				CommitTs: 2,
				Table: &codecv1.TableName{
					Schema:      "schema",
					Table:       "table",
					TableID:     1,
					IsPartition: false,
				},
				TableInfo: nil,
				Columns: []*codecv1.Column{
					{
						Name: "column",
						Flag: model.BinaryFlag,
					},
				},
				PreColumns: []*codecv1.Column{
					{
						Name: "column",
						Flag: model.BinaryFlag,
					},
				},
				IndexColumns: [][]int{{1}},
			},
		},
		RedoDDL: &codecv1.RedoDDLEvent{
			DDL: &codecv1.DDLEvent{
				StartTs:  1,
				CommitTs: 2,
				Type:     timodel.ActionCreateTable,
			},
		},
	}

	rv2 = &model.RedoLog{
		RedoRow: model.RedoRowChangedEvent{
			Row: &model.RowChangedEventInRedoLog{
				StartTs:  1,
				CommitTs: 2,
				Table: &model.TableName{
					Schema:      "schema",
					Table:       "table",
					TableID:     1,
					IsPartition: false,
				},
				Columns: []*model.Column{
					{
						Name: "column",
						Flag: model.BinaryFlag,
					},
				},
				PreColumns: []*model.Column{
					{
						Name: "column",
						Flag: model.BinaryFlag,
					},
				},
				IndexColumns: [][]int{{1}},
			},
		},
		RedoDDL: model.RedoDDLEvent{
			DDL: &model.DDLEvent{
				StartTs:  1,
				CommitTs: 2,
				Type:     timodel.ActionCreateTable,
			},
		},
	}

	// Unmarshal from v1, []byte{} will be transformed into "".
	rv1.RedoRow.Row.Columns[0].Value = []byte{}
	rv1.RedoRow.Row.PreColumns[0].Value = []byte{}
	rv2.RedoRow.Row.Columns[0].Value = ""
	rv2.RedoRow.Row.PreColumns[0].Value = ""

	// Marshal v1 into bytes.
	codecv1.PreMarshal(rv1)
	msg1, err = rv1.MarshalMsg(nil)
	require.Nil(t, err)

	// Unmarshal v2 from v1 bytes.
	rv2Gen, msg1, err = UnmarshalRedoLog(msg1)
	require.Nil(t, err)
	require.Zero(t, len(msg1))
	require.Equal(t, rv2.RedoRow.Row, rv2Gen.RedoRow.Row)

	// For v2, []byte{} will be kept same in marshal and unmarshal.
	rv2.RedoRow.Row.Columns[0].Value = []byte{}
	rv2.RedoRow.Row.PreColumns[0].Value = []byte{}
	rv2Gen.RedoRow.Row.Columns[0].Value = []byte{}
	rv2Gen.RedoRow.Row.PreColumns[0].Value = []byte{}

	msg1, err = MarshalRedoLog(rv2Gen, nil)
	require.Nil(t, err)
	rv2Gen, msg1, err = UnmarshalRedoLog(msg1)
	require.Nil(t, err)
	require.Zero(t, len(msg1))
	require.Equal(t, rv2.RedoRow.Row, rv2Gen.RedoRow.Row)
}

func TestRowRedoConvert(t *testing.T) {
	t.Parallel()

	row := &model.RowChangedEventInRedoLog{
		StartTs:  100,
		CommitTs: 120,
		Table:    &model.TableName{Schema: "test", Table: "table1", TableID: 57},
		PreColumns: []*model.Column{{
			Name:  "a1",
			Type:  mysql.TypeLong,
			Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
			Value: int64(1),
		}, {
			Name:  "a2",
			Type:  mysql.TypeVarchar,
			Value: []byte("char"),
		}, {
			Name:  "a3",
			Type:  mysql.TypeLong,
			Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
			Value: int64(1),
		}, {
			Name:    "a4",
			Type:    mysql.TypeTinyBlob,
			Charset: charset.CharsetGBK,
			Value:   []byte("你好"),
		}, nil},
		Columns: []*model.Column{{
			Name:  "a1",
			Type:  mysql.TypeLong,
			Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
			Value: int64(2),
		}, {
			Name:  "a2",
			Type:  mysql.TypeVarchar,
			Value: []byte("char-updated"),
		}, {
			Name:  "a3",
			Type:  mysql.TypeLong,
			Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
			Value: int64(2),
		}, {
			Name:    "a4",
			Type:    mysql.TypeTinyBlob,
			Charset: charset.CharsetGBK,
			Value:   []byte("世界"),
		}, nil},
		IndexColumns: [][]int{{1, 3}},
	}

	redoLog := &model.RedoLog{RedoRow: model.RedoRowChangedEvent{Row: row}}
	data, err := MarshalRedoLog(redoLog, nil)
	require.Nil(t, err)

	redoLog2, data, err := UnmarshalRedoLog(data)
	require.Nil(t, err)
	require.Zero(t, len(data))
	require.Equal(t, row, redoLog2.RedoRow.Row)
}

func TestRowRedoConvertWithEmptySlice(t *testing.T) {
	t.Parallel()

	row := &model.RowChangedEventInRedoLog{
		StartTs:  100,
		CommitTs: 120,
		Table:    &model.TableName{Schema: "test", Table: "table1", TableID: 57},
		PreColumns: []*model.Column{{
			Name:  "a1",
			Type:  mysql.TypeLong,
			Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
			Value: int64(1),
		}, {
			Name:  "a2",
			Type:  mysql.TypeVarchar,
			Value: []byte(""), // empty slice should be marshal and unmarshal safely
		}},
		Columns: []*model.Column{{
			Name:  "a1",
			Type:  mysql.TypeLong,
			Flag:  model.BinaryFlag | model.MultipleKeyFlag | model.HandleKeyFlag,
			Value: int64(2),
		}, {
			Name:  "a2",
			Type:  mysql.TypeVarchar,
			Value: []byte(""),
		}},
		IndexColumns: [][]int{{1}},
	}

	redoLog := &model.RedoLog{RedoRow: model.RedoRowChangedEvent{Row: row}}
	data, err := MarshalRedoLog(redoLog, nil)
	require.Nil(t, err)

	redoLog2, data, err := UnmarshalRedoLog(data)
	require.Nil(t, err)
	require.Zero(t, len(data))
	require.Equal(t, row, redoLog2.RedoRow.Row)
}

func TestDDLRedoConvert(t *testing.T) {
	t.Parallel()

	ddl := &model.DDLEvent{
		StartTs:   1020,
		CommitTs:  1030,
		TableInfo: &model.TableInfo{},
		Type:      timodel.ActionAddColumn,
		Query:     "ALTER TABLE test.t1 ADD COLUMN a int",
	}

	redoLog := &model.RedoLog{RedoDDL: model.RedoDDLEvent{DDL: ddl}}
	data, err := MarshalRedoLog(redoLog, nil)
	require.Nil(t, err)

	redoLog2, data, err := UnmarshalRedoLog(data)
	require.Nil(t, err)
	require.Zero(t, len(data))
	require.Equal(t, ddl, redoLog2.RedoDDL.DDL)
}
