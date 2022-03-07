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
	model2 "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.uber.org/zap"
)

type avroBatchEncoderSuite struct {
	encoder *AvroEventBatchEncoder
}

var _ = check.Suite(&avroBatchEncoderSuite{})

func (s *avroBatchEncoderSuite) SetUpSuite(c *check.C) {
	startHTTPInterceptForTestingRegistry(c)

	keyManager, err := NewAvroSchemaManager(context.Background(), &security.Credential{}, "http://127.0.0.1:8081", "-key")
	c.Assert(err, check.IsNil)

	valueManager, err := NewAvroSchemaManager(context.Background(), &security.Credential{}, "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)

	s.encoder = &AvroEventBatchEncoder{
		valueSchemaManager: valueManager,
		keySchemaManager:   keyManager,
		resultBuf:          make([]*MQMessage, 0, 4096),
	}
}

func (s *avroBatchEncoderSuite) TearDownSuite(c *check.C) {
	stopHTTPInterceptForTestingRegistry()
}

func setBinChsClnFlag(ft *types.FieldType) *types.FieldType {
	types.SetBinChsClnFlag(ft)
	return ft
}

func setFlag(ft *types.FieldType, flag uint) *types.FieldType {
	types.SetTypeFlag(&ft.Flag, flag, true)
	return ft
}

func setElems(ft *types.FieldType, elems []string) *types.FieldType {
	ft.Elems = elems
	return ft
}

func (s *avroBatchEncoderSuite) TestAvroEncodeOnly(c *check.C) {
	defer testleak.AfterTest(c)()

	table := model.TableName{
		Schema: "testdb",
		Table:  "TestAvroEncodeOnly",
	}

	cols := []*model.Column{
		{Name: "id", Value: int64(1), Type: mysql.TypeLong},
		{Name: "myint", Value: int64(2), Type: mysql.TypeLong},
		{Name: "mybool", Value: int64(1), Type: mysql.TypeTiny},
		{Name: "myfloat", Value: float64(3.14), Type: mysql.TypeFloat},
		{Name: "mybytes1", Value: []byte("Hello World"), Flag: model.BinaryFlag, Type: mysql.TypeBlob},
		{Name: "mybytes2", Value: []byte("Hello World"), Flag: model.BinaryFlag, Type: mysql.TypeMediumBlob},
		{Name: "mybytes3", Value: []byte("Hello World"), Flag: model.BinaryFlag, Type: mysql.TypeTinyBlob},
		{Name: "mybytes4", Value: []byte("Hello World"), Flag: model.BinaryFlag, Type: mysql.TypeLongBlob},
		{Name: "mybytes5", Value: []byte("Hello World"), Flag: model.BinaryFlag, Type: mysql.TypeVarString},
		{Name: "mybytes6", Value: []byte("Hello World"), Flag: model.BinaryFlag, Type: mysql.TypeString},
		{Name: "mybytes7", Value: []byte("Hello World"), Flag: model.BinaryFlag, Type: mysql.TypeVarchar},
		{Name: "mystring1", Value: "Hello World", Type: mysql.TypeBlob},
		{Name: "mystring2", Value: "Hello World", Type: mysql.TypeMediumBlob},
		{Name: "mystring3", Value: "Hello World", Type: mysql.TypeTinyBlob},
		{Name: "mystring4", Value: "Hello World", Type: mysql.TypeLongBlob},
		{Name: "mystring5", Value: "Hello World", Type: mysql.TypeVarString},
		{Name: "mystring6", Value: "Hello World", Type: mysql.TypeString},
		{Name: "mystring7", Value: "Hello World", Type: mysql.TypeVarchar},
		{Name: "myenum", Value: uint64(1), Type: mysql.TypeEnum},
		{Name: "myset", Value: uint64(1), Type: mysql.TypeSet},
		{Name: "ts", Value: time.Now().Format(types.TimeFSPFormat), Type: mysql.TypeTimestamp},
		{Name: "myjson", Value: "{\"foo\": \"bar\"}", Type: mysql.TypeJSON},
	}

	colInfos := []rowcodec.ColInfo{
		{ID: 1, IsPKHandle: true, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)},
		{ID: 2, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)},
		{ID: 3, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTiny)},
		{ID: 4, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeFloat)},
		{ID: 5, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeBlob))},
		{ID: 6, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeMediumBlob))},
		{ID: 7, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeTinyBlob))},
		{ID: 8, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeLongBlob))},
		{ID: 9, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeVarString))},
		{ID: 10, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeString))},
		{ID: 11, IsPKHandle: false, VirtualGenCol: false, Ft: setBinChsClnFlag(types.NewFieldType(mysql.TypeVarchar))},
		{ID: 12, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBlob)},
		{ID: 13, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeMediumBlob)},
		{ID: 14, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTinyBlob)},
		{ID: 15, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLongBlob)},
		{ID: 16, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeVarString)},
		{ID: 17, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeString)},
		{ID: 18, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeVarchar)},
		{ID: 19, IsPKHandle: false, VirtualGenCol: false, Ft: setElems(types.NewFieldType(mysql.TypeEnum), []string{"a", "b"})},
		{ID: 20, IsPKHandle: false, VirtualGenCol: false, Ft: setElems(types.NewFieldType(mysql.TypeSet), []string{"a", "b"})},
		{ID: 21, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTimestamp)},
		{ID: 22, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeJSON)},
	}

	schema, err := ColumnInfoToAvroSchema(table.Table, cols)
	c.Assert(err, check.IsNil)
	avroCodec, err := goavro.NewCodec(schema)
	c.Assert(err, check.IsNil)

	r, err := avroEncode(&table, s.encoder.valueSchemaManager, 1, cols, colInfos, time.Local)
	c.Assert(err, check.IsNil)

	res, _, err := avroCodec.NativeFromBinary(r.data)
	c.Check(err, check.IsNil)
	c.Check(res, check.NotNil)
	for k, v := range res.(map[string]interface{}) {
		if k == "myenum" || k == "myset" {
			if vmap, ok := v.(map[string]interface{}); ok {
				_, exists := vmap["string"]
				c.Check(exists, check.IsTrue)
			}
		}
	}

	txt, err := avroCodec.TextualFromNative(nil, res)
	c.Check(err, check.IsNil)
	log.Info("TestAvroEncodeOnly", zap.ByteString("result", txt))
}

func (s *avroBatchEncoderSuite) TestAvroTimeZone(c *check.C) {
	defer testleak.AfterTest(c)()

	table := model.TableName{
		Schema: "testdb",
		Table:  "TestAvroTimeZone",
	}

	location, err := time.LoadLocation("UTC")
	c.Check(err, check.IsNil)

	timestamp := time.Now()
	cols := []*model.Column{
		{Name: "id", Value: int64(1), Type: mysql.TypeLong},
		{Name: "myint", Value: int64(2), Type: mysql.TypeLong},
		{Name: "mybool", Value: int64(1), Type: mysql.TypeTiny},
		{Name: "myfloat", Value: float64(3.14), Type: mysql.TypeFloat},
		{Name: "mystring", Value: []byte("Hello World"), Type: mysql.TypeBlob},
		{Name: "ts", Value: timestamp.In(location).Format(types.TimeFSPFormat), Type: mysql.TypeTimestamp},
	}

	colInfos := []rowcodec.ColInfo{
		{ID: 1, IsPKHandle: true, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)},
		{ID: 2, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)},
		{ID: 3, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTiny)},
		{ID: 4, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeFloat)},
		{ID: 5, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBlob)},
		{ID: 6, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTimestamp)},
	}

	schema, err := ColumnInfoToAvroSchema(table.Table, cols)
	c.Assert(err, check.IsNil)
	avroCodec, err := goavro.NewCodec(schema)
	c.Assert(err, check.IsNil)

	r, err := avroEncode(&table, s.encoder.valueSchemaManager, 1, cols, colInfos, location)
	c.Assert(err, check.IsNil)

	res, _, err := avroCodec.NativeFromBinary(r.data)
	c.Check(err, check.IsNil)
	c.Check(res, check.NotNil)
	actual := (res.(map[string]interface{}))["ts"].(map[string]interface{})["long.timestamp-millis"].(time.Time)
	c.Check(actual.Local().Sub(timestamp), check.LessEqual, time.Millisecond)
}

func (s *avroBatchEncoderSuite) TestAvroEnvelope(c *check.C) {
	defer testleak.AfterTest(c)()
	avroCodec, err := goavro.NewCodec(`
        {
          "type": "record",
          "name": "TestAvroEnvelope",
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
	defer testleak.AfterTest(c)()
	testCaseUpdate := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "person",
		},
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.HandleKeyFlag, Value: int64(1)},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Bob"},
			{Name: "tiny", Type: mysql.TypeTiny, Value: int64(255)},
			{Name: "utiny", Type: mysql.TypeTiny, Flag: model.UnsignedFlag, Value: uint64(100)},
			{Name: "comment", Type: mysql.TypeBlob, Value: []byte("测试")},
		},
		ColInfos: []rowcodec.ColInfo{
			{ID: 1, IsPKHandle: true, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeLong)},
			{ID: 2, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeVarchar)},
			{ID: 3, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeTiny)},
			{ID: 4, IsPKHandle: false, VirtualGenCol: false, Ft: setFlag(types.NewFieldType(mysql.TypeTiny), uint(model.UnsignedFlag))},
			{ID: 5, IsPKHandle: false, VirtualGenCol: false, Ft: types.NewFieldType(mysql.TypeBlob)},
		},
	}

	testCaseDdl := &model.DDLEvent{
		CommitTs: 417318403368288260,
		TableInfo: &model.SimpleTableInfo{
			Schema: "test", Table: "person",
		},
		Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
		Type:  model2.ActionCreateTable,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pm := puller.NewMockPullerManager(c, true)
	defer pm.TearDown()
	pm.MustExec(testCaseDdl.Query)
	ddlPlr := pm.CreatePuller(0, []regionspan.ComparableSpan{regionspan.ToComparableSpan(regionspan.GetDDLSpan())})
	go func() {
		err := ddlPlr.Run(ctx)
		if err != nil && errors.Cause(err) != context.Canceled {
			c.Fail()
		}
	}()

	info := pm.GetTableInfo("test", "person")
	testCaseDdl.TableInfo = new(model.SimpleTableInfo)
	testCaseDdl.TableInfo.Schema = "test"
	testCaseDdl.TableInfo.Table = "person"
	testCaseDdl.TableInfo.ColumnInfo = make([]*model.ColumnInfo, len(info.Columns))
	for i, v := range info.Columns {
		testCaseDdl.TableInfo.ColumnInfo[i] = new(model.ColumnInfo)
		testCaseDdl.TableInfo.ColumnInfo[i].FromTiColumnInfo(v)
	}

	_, err := s.encoder.EncodeDDLEvent(testCaseDdl)
	c.Check(err, check.IsNil)

	err = s.encoder.AppendRowChangedEvent(testCaseUpdate)
	c.Check(err, check.IsNil)
}
