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
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
	"time"
)

type AvroBatchEncoderSuite struct {
	encoder *AvroEventBatchEncoder
}

var _ = check.Suite(&AvroBatchEncoderSuite{})

func (s *AvroBatchEncoderSuite) SetUpSuite(c *check.C) {
	manager, err := NewAvroSchemaManager(context.Background(), "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)

	StartHTTPInterceptForTestingRegistry(c)

	s.encoder = &AvroEventBatchEncoder{
		valueSchemaManager: manager,
		keyBuf:             nil,
		valueBuf:           nil,
	}
}

func (s *AvroBatchEncoderSuite) TearDownSuite(c *check.C) {
	StopHttpInterceptForTestingRegistry()
}

func (s *AvroBatchEncoderSuite) TestAvroEncodeOnly(c *check.C) {
	avroCodec, err := goavro.NewCodec(`
        {
          "type": "record",
          "name": "test1",
          "fields" : [
            {"name": "id", "type": ["null", "int"], "default": null},
			{"name": "myint", "type": ["null", "int"], "default": null},
			{"name": "mybool", "type": ["null", "boolean"], "default": null},
			{"name": "myfloat", "type": ["null", "float"], "default": null},
			{"name": "mybytes", "type": ["null", "bytes"], "default": null},
			{"name": "ts", "type": ["null", "long.timestamp-millis"], "default": null}
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
		"id": {Value: int32(1), Type: mysql.TypeLong},
		"myint": {Value: int32(2), Type: mysql.TypeLong},
		"mybool": {Value: true, Type: mysql.TypeTiny},
		"myfloat": {Value: float32(3.14), Type: mysql.TypeFloat},
		"mybytes": {Value: []byte("Hello World"), Type: mysql.TypeBlob},
		"ts": {Value: time.Now().Format(types.TimeFSPFormat), Type: mysql.TypeTimestamp},
	})
	c.Assert(err, check.IsNil)

	res, _, err := avroCodec.NativeFromBinary(r.data)
	c.Check(err, check.IsNil)
	c.Check(res, check.NotNil)

	txt, err := avroCodec.TextualFromNative(nil, res)
	c.Check(err, check.IsNil)
	log.Info("TestAvroEncodeOnly", zap.ByteString("result", txt))
}