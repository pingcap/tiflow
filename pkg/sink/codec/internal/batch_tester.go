// Copyright 2022 PingCAP, Inc.
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

package internal

import (
	"context"
	"sort"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/stretchr/testify/require"
)

// RowTestData contains the data to construct RowChangedEvent
type RowTestData struct {
	CommitTs   uint64
	Schema     string
	Table      string
	TableInfo  *model.TableInfo
	PreColumns []*model.Column
	Columns    []*model.Column
}

// CreateRowChangedEventFromTestData create a RowChangedEvent from RowTestData.
func CreateRowChangedEventFromTestData(rowData *RowTestData) *model.RowChangedEvent {
	var tableInfo *model.TableInfo
	if len(rowData.PreColumns) > 0 {
		tableInfo = model.BuildTableInfo(rowData.Schema, rowData.Table, rowData.PreColumns, nil)
	} else {
		tableInfo = model.BuildTableInfo(rowData.Schema, rowData.Table, rowData.Columns, nil)
	}
	return &model.RowChangedEvent{
		CommitTs:   rowData.CommitTs,
		TableInfo:  tableInfo,
		Columns:    model.Columns2ColumnDatas(rowData.Columns, tableInfo),
		PreColumns: model.Columns2ColumnDatas(rowData.PreColumns, tableInfo),
	}
}

func createRowChangedEventsFromTestDatas(rowDataArrays [][]*RowTestData) [][]*model.RowChangedEvent {
	result := make([][]*model.RowChangedEvent, 0, len(rowDataArrays))
	for _, rowDatas := range rowDataArrays {
		events := make([]*model.RowChangedEvent, 0, len(rowDatas))
		for _, data := range rowDatas {
			events = append(events, CreateRowChangedEventFromTestData(data))
		}
		result = append(result, events)
	}
	return result
}

var (
	// CodecRowTestData defines test cases data for RowChangedEvent.
	CodecRowTestData = [][]*RowTestData{
		{{
			CommitTs: 424316552636792833,
			Schema:   "a",
			Table:    "b",
			PreColumns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar0")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string0")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/01"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(1.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(1000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
			Columns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar1")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string1")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/02"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/02 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/02 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(2.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(2000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
		}},
		{{
			CommitTs: 424316553934667777,
			Schema:   "a",
			Table:    "b",
			PreColumns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar0")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string0")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/01"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(1.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(1000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
			Columns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar1")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string1")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/02"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/02 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/02 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(2.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(2000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
		}, {
			CommitTs: 424316554327097345,
			Schema:   "a",
			Table:    "b",
			PreColumns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar0")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string0")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/01"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(1.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(1000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
			Columns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar1")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string1")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/02"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/02 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/02 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(2.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(2000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
		}, {
			CommitTs: 424316554746789889,
			Schema:   "a",
			Table:    "b",
			PreColumns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar0")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string0")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/01"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(1.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(1000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
			Columns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar1")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string1")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/02"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/02 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/02 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(2.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(2000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
		}, {
			CommitTs: 424316555073945601,
			Schema:   "a",
			Table:    "b",
			PreColumns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar0")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string0")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/01"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(1.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(1000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
			Columns: []*model.Column{
				{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar1")},
				{Name: "string", Type: mysql.TypeString, Value: []byte("string1")},
				{Name: "date", Type: mysql.TypeDate, Value: "2021/01/02"},
				{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/02 00:00:00"},
				{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/02 00:00:00"},
				{Name: "float", Type: mysql.TypeFloat, Value: float64(2.0)},
				{Name: "long", Type: mysql.TypeLong, Value: int64(2000)},
				{Name: "null", Type: mysql.TypeNull, Value: nil},
			},
		}},
		{},
	}

	// CodecRowCases defines test cases for RowChangedEvent.
	CodecRowCases = createRowChangedEventsFromTestDatas(CodecRowTestData)

	tableInfoForDDLCase = &model.TableInfo{
		TableName: model.TableName{
			Schema: "a", Table: "b",
		},
	}
	// CodecDDLCases defines test cases for DDLEvent.
	CodecDDLCases = [][]*model.DDLEvent{{{
		CommitTs:  424316555979653121,
		TableInfo: tableInfoForDDLCase,
		Query:     "create table a",
		Type:      1,
	}}, {{
		CommitTs:  424316583965360129,
		TableInfo: tableInfoForDDLCase,
		Query:     "create table a",
		Type:      1,
	}, {
		CommitTs:  424316586087940097,
		TableInfo: tableInfoForDDLCase,
		Query:     "create table b",
		Type:      2,
	}, {
		CommitTs:  424316588736118785,
		TableInfo: tableInfoForDDLCase,
		Query:     "create table c",
		Type:      3,
	}}, {}}

	// CodecResolvedTSCases defines test cases for resolved ts events.
	CodecResolvedTSCases = [][]uint64{{424316592563683329}, {424316594097225729, 424316594214141953, 424316594345213953}, {}}
)

type columnsArray []*model.Column

func (a columnsArray) Len() int {
	return len(a)
}

func (a columnsArray) Less(i, j int) bool {
	return a[i].Name < a[j].Name
}

func (a columnsArray) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// SortColumnArrays sort column arrays by name
func SortColumnArrays(arrays ...[]*model.Column) {
	for _, array := range arrays {
		if array != nil {
			sort.Sort(columnsArray(array))
		}
	}
}

// BatchTester is a tester for batch encoders.
type BatchTester struct {
	RowCases        [][]*RowTestData
	DDLCases        [][]*model.DDLEvent
	ResolvedTsCases [][]uint64
}

// NewDefaultBatchTester creates a default BatchTester.
func NewDefaultBatchTester() *BatchTester {
	return &BatchTester{
		RowCases:        CodecRowTestData,
		DDLCases:        CodecDDLCases,
		ResolvedTsCases: CodecResolvedTSCases,
	}
}

// TestBatchCodec tests bunch of cases for RowEventDecoder.
func (s *BatchTester) TestBatchCodec(
	t *testing.T,
	encoderBuilder codec.RowEventEncoderBuilder,
	newDecoder func(key []byte, value []byte) (codec.RowEventDecoder, error),
) {
	checkRowDecoder := func(decoder codec.RowEventDecoder, cs []*RowTestData) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeRow, tp)
			row, err := decoder.NextRowChangedEvent()
			require.NoError(t, err)
			srcRow := CreateRowChangedEventFromTestData(cs[index])

			require.Equal(t, srcRow, row)
			index++
		}
	}
	checkDDLDecoder := func(decoder codec.RowEventDecoder, cs []*model.DDLEvent) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeDDL, tp)
			ddl, err := decoder.NextDDLEvent()
			require.Nil(t, err)
			require.Equal(t, cs[index], ddl)
			index++
		}
	}
	checkTSDecoder := func(decoder codec.RowEventDecoder, cs []uint64) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeResolved, tp)
			ts, err := decoder.NextResolvedEvent()
			require.Nil(t, err)
			require.Equal(t, cs[index], ts)
			index++
		}
	}

	for _, cs := range s.RowCases {
		encoder := encoderBuilder.Build()

		for _, rowData := range cs {
			SortColumnArrays(rowData.Columns, rowData.PreColumns)
			row := CreateRowChangedEventFromTestData(rowData)
			err := encoder.AppendRowChangedEvent(context.Background(), "", row, nil)
			require.Nil(t, err)
		}

		if len(cs) > 0 {
			res := encoder.Build()
			require.Len(t, res, 1)
			require.Equal(t, len(cs), res[0].GetRowsCount())

			decoder, err := newDecoder(res[0].Key, res[0].Value)
			require.NoError(t, err)

			checkRowDecoder(decoder, cs)
		}
	}
	for _, cs := range s.DDLCases {
		encoder := encoderBuilder.Build()
		for i, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(t, err)
			require.NotNil(t, msg)

			decoder, err := newDecoder(msg.Key, msg.Value)
			require.NoError(t, err)

			checkDDLDecoder(decoder, cs[i:i+1])

		}
	}

	for _, cs := range s.ResolvedTsCases {
		encoder := encoderBuilder.Build()
		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			require.Nil(t, err)
			require.NotNil(t, msg)

			decoder, err := newDecoder(msg.Key, msg.Value)
			require.NoError(t, err)

			checkTSDecoder(decoder, cs[i:i+1])
		}
	}
}
