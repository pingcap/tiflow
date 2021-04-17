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

package codec

import (
	"bytes"
	"compress/zlib"
	"fmt"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

var (
	codecRowCases = [][]*model.RowChangedEvent{{{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{
			{Name: "varchar", Type: mysql.TypeVarchar, Value: []byte("varchar")},
			{Name: "string", Type: mysql.TypeString, Value: []byte("string")},
			{Name: "date", Type: mysql.TypeDate, Value: "2021/01/01"},
			{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
			{Name: "datetime", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
			{Name: "float", Type: mysql.TypeFloat, Value: float64(1.0)},
			{Name: "long", Type: mysql.TypeLong, Value: int64(1000)},
			{Name: "null", Type: mysql.TypeNull, Value: nil},
		},
	}}, {{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{
			{Name: "varchar1", Type: mysql.TypeVarchar, Value: []byte("varchar")},
			{Name: "string1", Type: mysql.TypeString, Value: []byte("string")},
			{Name: "date1", Type: mysql.TypeDate, Value: "2021/01/01"},
			{Name: "timestamp1", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
			{Name: "datetime1", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
			{Name: "float1", Type: mysql.TypeFloat, Value: float64(1.0)},
			{Name: "long1", Type: mysql.TypeLong, Value: int64(1000)},
			{Name: "null1", Type: mysql.TypeNull, Value: nil},
		},
	}, {
		CommitTs: 2,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{
			{Name: "varchar2", Type: mysql.TypeVarchar, Value: []byte("varchar")},
			{Name: "string2", Type: mysql.TypeString, Value: []byte("string")},
			{Name: "date2", Type: mysql.TypeDate, Value: "2021/01/01"},
			{Name: "timestamp2", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
			{Name: "datetime2", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
			{Name: "float2", Type: mysql.TypeFloat, Value: float64(1.0)},
			{Name: "long2", Type: mysql.TypeLong, Value: int64(1000)},
			{Name: "null2", Type: mysql.TypeNull, Value: nil},
		},
	}, {
		CommitTs: 3,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{
			{Name: "varchar3", Type: mysql.TypeVarchar, Value: []byte("varchar")},
			{Name: "string3", Type: mysql.TypeString, Value: []byte("string")},
			{Name: "date3", Type: mysql.TypeDate, Value: "2021/01/01"},
			{Name: "timestamp3", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
			{Name: "datetime3", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
			{Name: "float3", Type: mysql.TypeFloat, Value: float64(1.0)},
			{Name: "long3", Type: mysql.TypeLong, Value: int64(1000)},
			{Name: "null3", Type: mysql.TypeNull, Value: nil},
		},
	}, {
		CommitTs: 4,
		Table:    &model.TableName{Schema: "a", Table: "c", TableID: 6, IsPartition: true},
		Columns: []*model.Column{
			{Name: "varchar4", Type: mysql.TypeVarchar, Value: []byte("varchar")},
			{Name: "string4", Type: mysql.TypeString, Value: []byte("string")},
			{Name: "date4", Type: mysql.TypeDate, Value: "2021/01/01"},
			{Name: "timestamp4", Type: mysql.TypeTimestamp, Value: "2021/01/01 00:00:00"},
			{Name: "datetime4", Type: mysql.TypeDatetime, Value: "2021/01/01 00:00:00"},
			{Name: "float4", Type: mysql.TypeFloat, Value: float64(1.0)},
			{Name: "long4", Type: mysql.TypeLong, Value: int64(1000)},
			{Name: "null4", Type: mysql.TypeNull, Value: nil},
		},
	}}, {}}

	codecDDLCases = [][]*model.DDLEvent{{{
		CommitTs: 1,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table a",
		Type:  1,
	}}, {{
		CommitTs: 1,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table a",
		Type:  1,
	}, {
		CommitTs: 2,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table b",
		Type:  2,
	}, {
		CommitTs: 3,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table c",
		Type:  3,
	}}, {}}

	codecResolvedTSCases = [][]uint64{{1}, {1, 2, 3}, {}}
)

var _ = check.Suite(&codecTestSuite{})

type codecTestSuite struct{}

func (s *codecTestSuite) checkCompressedSize(messages []*MQMessage) (int, int) {
	var buff bytes.Buffer
	writer := zlib.NewWriter(&buff)
	originalSize := 0
	for _, message := range messages {
		originalSize += len(message.Key) + len(message.Value)
		if len(message.Key) > 0 {
			_, _ = writer.Write(message.Key)
		}
		_, _ = writer.Write(message.Value)
	}
	writer.Close()
	return originalSize, buff.Len()
}

func (s *codecTestSuite) encodeRowCase(c *check.C, encoder EventBatchEncoder, events []*model.RowChangedEvent) []*MQMessage {
	err := encoder.SetParams(map[string]string{"max-message-bytes": "8192", "max-batch-size": "64"})
	c.Assert(err, check.IsNil)

	for _, event := range events {
		op, err := encoder.AppendRowChangedEvent(event)
		c.Assert(err, check.IsNil)
		c.Assert(op, check.Equals, EncoderNoOperation)
	}

	if len(events) > 0 {
		return encoder.Build()
	}
	return nil
}

func (s *codecTestSuite) TestJsonVsCraft(c *check.C) {
	defer testleak.AfterTest(c)()
	fmt.Println("| index | craft size | json size | craft compressed | json compressed |")
	fmt.Println("| ----- | ---------- | --------- | ---------------- | --------------- |")
	for i, cs := range codecRowCases {
		craftEncoder := NewCraftEventBatchEncoder()
		jsonEncoder := NewJSONEventBatchEncoder()
		craftMessages := s.encodeRowCase(c, craftEncoder, cs)
		jsonMessages := s.encodeRowCase(c, jsonEncoder, cs)
		craftOriginal, craftCompressed := s.checkCompressedSize(craftMessages)
		jsonOriginal, jsonCompressed := s.checkCompressedSize(jsonMessages)
		fmt.Printf("| %d | %d | %d | %d | %d |\n", i, craftOriginal, jsonOriginal, craftCompressed, jsonCompressed)
	}
}
