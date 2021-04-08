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
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("aa")}},
	}}, {{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("aa")}},
	}, {
		CommitTs: 2,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("bb")}},
	}, {
		CommitTs: 3,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("bb")}},
	}, {
		CommitTs: 4,
		Table:    &model.TableName{Schema: "a", Table: "c", TableID: 6, IsPartition: true},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("cc")}},
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
