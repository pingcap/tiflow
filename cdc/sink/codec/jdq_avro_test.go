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
	"golang.org/x/text/encoding/charmap"
	"time"
    "testing"
	"fmt"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	model2 "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

type jdqEventBatchEncoderSuite struct {
	encoder *JdqEventBatchEncoder
}

var _ = check.Suite(&jdqEventBatchEncoderSuite{})

func (s *jdqEventBatchEncoderSuite) SetUpSuite(c *check.C) {
	s.encoder = &JdqEventBatchEncoder{
		resultBuf:          make([]*MQMessage, 0, 4096),
		binaryEncode: charmap.ISO8859_1.NewDecoder(),
	}
}

func (s *jdqEventBatchEncoderSuite) TearDownSuite(c *check.C) {
}

func (s *jdqEventBatchEncoderSuite) TestJdqEncodeOnly(c *check.C) {
	defer testleak.AfterTest(c)()
	jdwSchema := `
                  {"type":"record","name":"JdwData","namespace":"com.jd.bdp.jdw.avro",
                   "fields":[{"name":"mid","type":"long"},
                              {"name":"db","type":"string"},
                              {"name":"sch","type":"string"},
                              {"name":"tab","type":"string"},
                              {"name":"opt","type":"string"},
                              {"name":"ts","type":"long"},
                              {"name":"err","type":["string","null"]},
                              {"name":"src","type":[{"type":"map","values":["string","null"]},"null"]},
                              {"name":"cur","type":[{"type":"map","values":["string","null"]},"null"]},
                              {"name":"cus","type":[{"type":"map","values":["string","null"]},"null"]}]}
               `
	avroCodec, err := goavro.NewCodec(jdwSchema)

	c.Assert(err, check.IsNil)

	table := &model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}
	testjson := `{"vpcId": "vpc-8enbtnk7bc","userpin": "umNsb3VkdGVzdwxy"}`
	cols := []*model.Column{
		{Name: "id", Value: int64(1), Type: mysql.TypeLong, Flag: model.HandleKeyFlag|model.PrimaryKeyFlag,},
		{Name: "myint", Value: int64(2), Type: mysql.TypeLong},
		{Name: "mybool", Value: int64(1), Type: mysql.TypeTiny},
		{Name: "myfloat", Value: float32(3.14), Type: mysql.TypeFloat},
		{Name: "mydouble", Value: float64(3.141592666), Type: mysql.TypeDouble},
		{Name: "ts", Value: time.Now().Format(types.TimeFSPFormat), Type: mysql.TypeTimestamp},
		{Name: "mydatetime", Value: time.Now().Format(types.TimeFSPFormat), Type: mysql.TypeDatetime},
		{Name: "mydate", Value: time.Now().Format(types.DateFormat), Type: mysql.TypeDate},
		{Name: "mytime", Value: "17:51:04.123456", Type: mysql.TypeDuration},
		{Name: "yearr", Value: int64(1901), Type: mysql.TypeYear},
		{Name: "myname", Value: "testing00111", Type: mysql.TypeVarchar},
		{Name: "mydecimal", Value: "3.1415926", Type: mysql.TypeNewDecimal},
		{Name: "myjson", Value: testjson, Type: mysql.TypeJSON},
		{Name: "myblob", Value: []byte("您好！"), Type: mysql.TypeBlob},
		{Name: "mybinary", Value: []byte("二进制数据！"), Type: mysql.TypeVarString, Flag: model.BinaryFlag},
		{Name: "myenum", Value: int64(1), Type: mysql.TypeEnum},
		{Name: "myset", Value: int64(2), Type: mysql.TypeSet},
		{Name: "mybit", Value: int64(8), Type: mysql.TypeBit},

	}

	e := &model.RowChangedEvent{
		StartTs: 123456888,
		CommitTs: 123456999,
		Table: table,
		Columns: cols,
	}

	r, err := s.encoder.jdqEncode(e)
	c.Assert(err, check.IsNil)

	res, _, err := avroCodec.NativeFromBinary(r.data)
	c.Check(err, check.IsNil)
	c.Check(res, check.NotNil)

	txt, err := avroCodec.TextualFromNative(nil, res)
	c.Check(err, check.IsNil)
	log.Info("TestJdqEncodeOnly", zap.ByteString("result", txt))
}

func (s *jdqEventBatchEncoderSuite) TestJdqEnvelope(c *check.C) {
	defer testleak.AfterTest(c)()
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

	res := jdqEncodeResult{
		data:       bin,
	}

	evlp, err := res.toEnvelope()
	c.Check(err, check.IsNil)

	c.Assert(evlp[0], check.Equals, jdqMagicByte)
	//c.Assert(evlp[1:5], check.BytesEquals, []byte{0, 0, 0, 7})

	parsed, _, err := avroCodec.NativeFromBinary(evlp[1:])
	c.Assert(err, check.IsNil)
	c.Assert(parsed, check.NotNil)

	id, exists := parsed.(map[string]interface{})["id"]
	c.Assert(exists, check.IsTrue)
	c.Assert(id, check.Equals, int32(7))
	log.Info("TestJdqEnvelope done!")
}

func (s *jdqEventBatchEncoderSuite) TestJdqEncode(c *check.C) {
	defer testleak.AfterTest(c)()
	myjson := `{"vpcId": "vpc-8enbtnk7bc","values": {"annotations": {"userpin": "umNsb3VkdGVzdwxy"}}}`
	testCaseUpdate := &model.RowChangedEvent{
		StartTs: 417318403368277260,
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "person",
		},
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.HandleKeyFlag|model.PrimaryKeyFlag, Value: int64(9)},
			{Name: "uuid", Type: mysql.TypeVarchar, Flag: model.PrimaryKeyFlag, Value: "wxy009human"},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Bob"},
			{Name: "tiny", Type: mysql.TypeTiny, Value: int64(255)},
			{Name: "comment", Type: mysql.TypeBlob, Value: []byte("测试")},
			{Name: "myint", Value: int64(2), Type: mysql.TypeLong},
			{Name: "mybool", Value: int64(1), Type: mysql.TypeTiny},
			{Name: "myfloat", Value: float32(3.14), Type: mysql.TypeFloat},
			{Name: "mydouble", Value: float64(3.141592666), Type: mysql.TypeDouble},
			{Name: "ts", Value: time.Now().Format(types.TimeFSPFormat), Type: mysql.TypeTimestamp},
			{Name: "mydatetime", Value: time.Now().Format(types.TimeFSPFormat), Type: mysql.TypeDatetime},
			{Name: "mydate", Value: time.Now().Format(types.DateFormat), Type: mysql.TypeDate},
			{Name: "mytime", Value: "17:51:04.123456", Type: mysql.TypeDuration},
			{Name: "yearr", Value: int64(1901), Type: mysql.TypeYear},
			{Name: "myname", Value: "testing00111", Type: mysql.TypeVarchar},
			{Name: "mydecimal", Value: "3.1415926", Type: mysql.TypeNewDecimal},
			{Name: "myjson", Value: myjson, Type: mysql.TypeJSON},
			{Name: "myblob", Value: []byte("您好！"), Type: mysql.TypeBlob},
			{Name: "mybinary", Value: []byte("二进制数据！"), Type: mysql.TypeVarString, Flag: model.BinaryFlag},
			{Name: "myenum", Value: int64(1), Type: mysql.TypeEnum},
			{Name: "myset", Value: int64(2), Type: mysql.TypeSet},
			{Name: "mybit", Value: int64(8), Type: mysql.TypeBit},
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

	_, err = s.encoder.AppendRowChangedEvent(testCaseUpdate)
	c.Check(err, check.IsNil)

	fmt.Printf("result key: %s\n", string(s.encoder.resultBuf[0].Key))
	fmt.Printf("result val: %+v\n", string(s.encoder.resultBuf[0].Value))
	log.Info("TestJdqEncode done!")
}

//func Test(t *testing.T) { check.TestingT(t) }
