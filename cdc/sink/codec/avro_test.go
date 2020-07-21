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

package codec

import (
	"context"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	model2 "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

type avroBatchEncoderSuite struct {
	encoder *AvroEventBatchEncoder
}

var _ = check.Suite(&avroBatchEncoderSuite{})

func (s *avroBatchEncoderSuite) SetUpSuite(c *check.C) {
	startHTTPInterceptForTestingRegistry(c)

	manager, err := NewAvroSchemaManager(context.Background(), "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)

	s.encoder = &AvroEventBatchEncoder{
		valueSchemaManager: manager,
		keyBuf:             nil,
		valueBuf:           nil,
	}
}

func (s *avroBatchEncoderSuite) TearDownSuite(c *check.C) {
	stopHTTPInterceptForTestingRegistry()
}

func (s *avroBatchEncoderSuite) TestAvroEncodeOnly(c *check.C) {
	avroCodec, err := goavro.NewCodec(`
        {
          "type": "record",
          "name": "test1",
          "fields" : [
            {"name": "id", "type": ["null", "int"], "default": null},
			{"name": "myint", "type": ["null", "int"], "default": null},
			{"name": "mybool", "type": ["null", "int"], "default": null},
			{"name": "myfloat", "type": ["null", "float"], "default": null},
			{"name": "mybytes", "type": ["null", "bytes"], "default": null},
			{"name": "ts", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null}
          ]
        }`)

	c.Assert(err, check.IsNil)

	table := model.TableName{
		Schema:    "testdb",
		Table:     "test1",
		Partition: 0,
	}

	err = s.encoder.valueSchemaManager.Register(context.Background(), table, avroCodec)
	c.Assert(err, check.IsNil)

	r, err := s.encoder.avroEncode(&table, 1, map[string]*model.Column{
		"id":      {Value: int32(1), Type: mysql.TypeLong},
		"myint":   {Value: int32(2), Type: mysql.TypeLong},
		"mybool":  {Value: uint8(1), Type: mysql.TypeTiny},
		"myfloat": {Value: float32(3.14), Type: mysql.TypeFloat},
		"mybytes": {Value: []byte("Hello World"), Type: mysql.TypeBlob},
		"ts":      {Value: time.Now().Format(types.TimeFSPFormat), Type: mysql.TypeTimestamp},
	})
	c.Assert(err, check.IsNil)

	res, _, err := avroCodec.NativeFromBinary(r.data)
	c.Check(err, check.IsNil)
	c.Check(res, check.NotNil)

	txt, err := avroCodec.TextualFromNative(nil, res)
	c.Check(err, check.IsNil)
	log.Info("TestAvroEncodeOnly", zap.ByteString("result", txt))
}

func (s *avroBatchEncoderSuite) TestAvroEnvelope(c *check.C) {
	avroCodec, err := goavro.NewCodec(`
        {
          "type": "record",
          "name": "test2",
          "fields" : [
            {"name": "id", "type": "int", "default": 0}
          ]
        }`)

	c.Assert(err, check.IsNil)

	testNativeData := make(map[string]interface{})
	testNativeData["id"] = 7

	bin, err := avroCodec.BinaryFromNative(nil, testNativeData)
	c.Check(err, check.IsNil)

	res := avroEncodeResult{
		data:       bin,
		registryID: 7,
	}

	evlp, err := res.toEnvelope()
	c.Check(err, check.IsNil)

	c.Assert(evlp[0], check.Equals, magicByte)
	c.Assert(evlp[1:5], check.BytesEquals, []byte{0, 0, 0, 7})

	parsed, _, err := avroCodec.NativeFromBinary(evlp[5:])
	c.Assert(err, check.IsNil)
	c.Assert(parsed, check.NotNil)

	id, exists := parsed.(map[string]interface{})["id"]
	c.Assert(exists, check.IsTrue)
	c.Assert(id, check.Equals, int32(7))
}

func (s *avroBatchEncoderSuite) TestAvroEncode(c *check.C) {
	trueVar := true
	testCaseUpdate := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "person",
		},
		Delete: false,
		Columns: map[string]*model.Column{
			"id":      {Type: mysql.TypeLong, WhereHandle: &trueVar, Value: 1},
			"name":    {Type: mysql.TypeVarchar, Value: "Bob"},
			"tiny":    {Type: mysql.TypeTiny, Value: uint8(255)},
			"comment": {Type: mysql.TypeBlob, Value: []byte("测试")},
		},
	}

	testCaseDdl := &model.DDLEvent{
		CommitTs: 417318403368288260,
		Schema:   "test",
		Table:    "person",
		Query:    "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
		Type:     model2.ActionCreateTable,
	}

	ctx := context.Background()

	pm := puller.NewMockPullerManager(c, true)
	pm.MustExec(testCaseDdl.Query)
	ddlPlr := pm.CreatePuller(0, []regionspan.ComparableSpan{regionspan.ToComparableSpan(regionspan.GetDDLSpan())})
	go func() {
		err := ddlPlr.Run(ctx)
		if err != nil && errors.Cause(err) != context.Canceled {
			c.Fail()
		}
	}()

	info := pm.GetTableInfo("test", "person")
	testCaseDdl.TableInfo = new(model.TableInfo)
	testCaseDdl.TableInfo.Schema = "test"
	testCaseDdl.TableInfo.Table = "person"
	testCaseDdl.TableInfo.ColumnInfo = make([]*model.ColumnInfo, len(info.Columns))
	for i, v := range info.Columns {
		testCaseDdl.TableInfo.ColumnInfo[i] = new(model.ColumnInfo)
		testCaseDdl.TableInfo.ColumnInfo[i].FromTiColumnInfo(v)
	}
	testCaseDdl.TableInfo.UpdateTs = 0xbeefbeef

	err := s.encoder.AppendDDLEvent(testCaseDdl)
	c.Check(err, check.IsNil)

	err = s.encoder.AppendRowChangedEvent(testCaseUpdate)
	c.Check(err, check.IsNil)
}
