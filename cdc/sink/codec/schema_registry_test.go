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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type AvroSchemaRegistrySuite struct {
}

var _ = check.Suite(&AvroSchemaRegistrySuite{})

type mockRegistry struct {
	subjects map[string]*mockRegistrySchema
	newID    int
	mu       sync.Mutex
}

type mockRegistrySchema struct {
	content string
	version int
	ID      int
}

func (s *AvroSchemaRegistrySuite) SetUpSuite(c *check.C) {
	httpmock.Activate()

	registry := mockRegistry{
		subjects: make(map[string]*mockRegistrySchema),
		newID:    1,
	}

	httpmock.RegisterResponder("GET", "http://127.0.0.1:8081", httpmock.NewStringResponder(200, "{}"))

	httpmock.RegisterResponder("POST", `=~^http://127.0.0.1:8081/subjects/(.+)/versions`,
		func(req *http.Request) (*http.Response, error) {
			subject, err := httpmock.GetSubmatch(req, 1)
			if err != nil {
				return nil, err
			}
			reqBody, err := ioutil.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			var reqData registerRequest
			err = json.Unmarshal(reqBody, &reqData)
			if err != nil {
				return nil, err
			}

			c.Assert(reqData.SchemaType, check.Equals, "AVRO")

			var respData registerResponse
			registry.mu.Lock()
			item, exists := registry.subjects[subject]
			if !exists {
				item = &mockRegistrySchema{
					content: reqData.Schema,
					version: 0,
					ID:      registry.newID,
				}
				registry.subjects[subject] = item
				respData.ID = registry.newID
			} else {
				if item.content == reqData.Schema {
					respData.ID = item.ID
				} else {
					item.content = reqData.Schema
					item.version++
					item.ID = registry.newID
					respData.ID = registry.newID
				}
			}
			registry.mu.Unlock()
			registry.newID++
			return httpmock.NewJsonResponse(200, &respData)
		})

	httpmock.RegisterResponder("GET", `=~^http://127.0.0.1:8081/subjects/(.+)/versions/latest`,
		func(req *http.Request) (*http.Response, error) {
			subject, err := httpmock.GetSubmatch(req, 1)
			if err != nil {
				return httpmock.NewStringResponse(500, "Internal Server Error"), err
			}

			registry.mu.Lock()
			item, exists := registry.subjects[subject]
			registry.mu.Unlock()
			if !exists {
				return httpmock.NewStringResponse(404, ""), nil
			}

			var respData lookupResponse
			respData.Schema = item.content
			respData.Name = subject
			respData.RegistryID = item.ID

			return httpmock.NewJsonResponse(200, &respData)
		})

	httpmock.RegisterResponder("DELETE", `=~^http://127.0.0.1:8081/subjects/(.+)`,
		func(req *http.Request) (*http.Response, error) {
			subject, err := httpmock.GetSubmatch(req, 1)
			if err != nil {
				return nil, err
			}

			registry.mu.Lock()
			defer registry.mu.Unlock()
			_, exists := registry.subjects[subject]
			if !exists {
				return httpmock.NewStringResponse(404, ""), nil
			}

			delete(registry.subjects, subject)
			return httpmock.NewStringResponse(200, ""), nil
		})

}

func (s *AvroSchemaRegistrySuite) TearDownSuite(c *check.C) {
	httpmock.DeactivateAndReset()
}

func getTestingContext() context.Context {
	// nolint:govet
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	return ctx
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistry(c *check.C) {
	table := model.TableName{
		Schema:    "testdb",
		Table:     "test1",
		Partition: 0,
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)

	err = manager.ClearRegistry(getTestingContext(), table)
	c.Assert(err, check.IsNil)

	_, _, err = manager.Lookup(getTestingContext(), table, 1)
	c.Assert(err, check.ErrorMatches, `.*cancelled.*`)

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

	err = manager.Register(getTestingContext(), table, codec)
	c.Assert(err, check.IsNil)

	var id int
	for i := 0; i < 2; i++ {
		_, id, err = manager.Lookup(getTestingContext(), table, 1)
		c.Assert(err, check.IsNil)
		c.Assert(id, check.Greater, 0)
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
	err = manager.Register(getTestingContext(), table, codec)
	c.Assert(err, check.IsNil)

	codec2, id2, err := manager.Lookup(getTestingContext(), table, 999)
	c.Assert(err, check.IsNil)
	c.Assert(id2, check.Not(check.Equals), id)
	c.Assert(codec.CanonicalSchema(), check.Equals, codec2.CanonicalSchema())
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistryBad(c *check.C) {
	_, err := NewAvroSchemaManager(getTestingContext(), "http://127.0.0.1:808", "-value")
	c.Assert(err, check.NotNil)

	_, err = NewAvroSchemaManager(getTestingContext(), "https://127.0.0.1:8080", "-value")
	c.Assert(err, check.NotNil)
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistryIdempotent(c *check.C) {
	table := model.TableName{
		Schema:    "testdb",
		Table:     "test1",
		Partition: 0,
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)
	for i := 0; i < 20; i++ {
		err = manager.ClearRegistry(getTestingContext(), table)
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
		err = manager.Register(getTestingContext(), table, codec)
		c.Assert(err, check.IsNil)
	}
}
