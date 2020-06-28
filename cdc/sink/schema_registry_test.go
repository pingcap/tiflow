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

package sink

import (
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type AvroSchemaRegistrySuite struct {
}

var _ = check.Suite(&AvroSchemaRegistrySuite{})

func (s *AvroSchemaRegistrySuite) TestSchemaRegistry(c *check.C) {
	table := model.TableName{
		Schema:    "testdb",
		Table:     "test1",
		Partition: 0,
	}

	manager, err := NewAvroSchemaManager("http://127.0.0.1:8081/")
	c.Assert(err, check.IsNil)

	err = manager.ClearRegistry(table)
	c.Assert(err, check.IsNil)

	_, _, err = manager.Lookup(table, 1)
	c.Assert(err, check.ErrorMatches, `.*not\s+found.*`)

	codec, err := goavro.NewCodec(`{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
           }
          ]
     }`)
	c.Assert(err, check.IsNil)

	err = manager.Register(table, codec)
	c.Assert(err, check.IsNil)

	var id int64
	for i := 0; i < 2; i++ {
		_, id, err = manager.Lookup(table, 1)
		c.Assert(err, check.IsNil)
		c.Assert(id, check.Greater, int64(0))
	}

	codec, err = goavro.NewCodec(`{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
           },
           {
             "type": [
      			"null",
      			"string"
             ],
             "default": null,
             "name": "field2"
           }
          ]
     }`)
	c.Assert(err, check.IsNil)
	err = manager.Register(table, codec)
	c.Assert(err, check.IsNil)

	codec2, id2, err := manager.Lookup(table, 999)
	c.Assert(err, check.IsNil)
	c.Assert(id2, check.Not(check.Equals), id)
	c.Assert(codec.CanonicalSchema(), check.Equals, codec2.CanonicalSchema())
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistryBad(c *check.C) {
	_, err := NewAvroSchemaManager("http://127.0.0.1:808")
	c.Assert(err, check.NotNil)

	_, err = NewAvroSchemaManager("https://127.0.0.1:8080")
	c.Assert(err, check.NotNil)
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistryIdempotent(c *check.C) {
	table := model.TableName{
		Schema:    "testdb",
		Table:     "test1",
		Partition: 0,
	}

	manager, err := NewAvroSchemaManager("http://127.0.0.1:8081/")
	c.Assert(err, check.IsNil)
	for i := 0; i < 20; i++ {
		err = manager.ClearRegistry(table)
		c.Assert(err, check.IsNil)
	}
	codec, err := goavro.NewCodec(`{
       "type": "record",
       "name": "test",
       "fields":
         [
           {
             "type": "string",
             "name": "field1"
           },
           {
             "type": [
      			"null",
      			"string"
             ],
             "default": null,
             "name": "field2"
           }
          ]
     }`)
	c.Assert(err, check.IsNil)

	for i := 0; i < 20; i++ {
		err = manager.Register(table, codec)
		c.Assert(err, check.IsNil)
	}
}
