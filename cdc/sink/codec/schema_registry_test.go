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
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type AvroSchemaRegistrySuite struct {
}

var _ = check.Suite(&AvroSchemaRegistrySuite{})

type mockRegistry struct {
	mu       sync.Mutex
	subjects map[string]*mockRegistrySchema
	newID    int
}

type mockRegistrySchema struct {
	content string
	version int
	ID      int
}

func startHTTPInterceptForTestingRegistry(c *check.C) {
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

			// c.Assert(reqData.SchemaType, check.Equals, "AVRO")

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
			registry.newID++
			registry.mu.Unlock()
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

	failCounter := 0
	httpmock.RegisterResponder("POST", `=~^http://127.0.0.1:8081/may-fail`,
		func(req *http.Request) (*http.Response, error) {
			data, _ := ioutil.ReadAll(req.Body)
			c.Assert(len(data), check.Greater, 0)
			c.Assert(int64(len(data)), check.Equals, req.ContentLength)
			if failCounter < 3 {
				failCounter++
				return httpmock.NewStringResponse(422, ""), nil
			}
			return httpmock.NewStringResponse(200, ""), nil
		})
}

func stopHTTPInterceptForTestingRegistry() {
	httpmock.DeactivateAndReset()
}

func (s *AvroSchemaRegistrySuite) SetUpSuite(c *check.C) {
	startHTTPInterceptForTestingRegistry(c)
}

func (s *AvroSchemaRegistrySuite) TearDownSuite(c *check.C) {
	stopHTTPInterceptForTestingRegistry()
}

func getTestingContext() context.Context {
	// nolint:govet
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	return ctx
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistry(c *check.C) {
	defer testleak.AfterTest(c)()
	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)

	err = manager.ClearRegistry(getTestingContext(), table)
	c.Assert(err, check.IsNil)

	_, _, err = manager.Lookup(getTestingContext(), table, 1)
	c.Assert(err, check.ErrorMatches, `.*not\sfound.*`)

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

	_, err = manager.Register(getTestingContext(), table, codec)
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
	_, err = manager.Register(getTestingContext(), table, codec)
	c.Assert(err, check.IsNil)

	codec2, id2, err := manager.Lookup(getTestingContext(), table, 999)
	c.Assert(err, check.IsNil)
	c.Assert(id2, check.Not(check.Equals), id)
	c.Assert(codec.CanonicalSchema(), check.Equals, codec2.CanonicalSchema())
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistryBad(c *check.C) {
	defer testleak.AfterTest(c)()
	_, err := NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "http://127.0.0.1:808", "-value")
	c.Assert(err, check.NotNil)

	_, err = NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "https://127.0.0.1:8080", "-value")
	c.Assert(err, check.NotNil)
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistryIdempotent(c *check.C) {
	defer testleak.AfterTest(c)()
	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "http://127.0.0.1:8081", "-value")
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

	id := 0
	for i := 0; i < 20; i++ {
		id1, err := manager.Register(getTestingContext(), table, codec)
		c.Assert(err, check.IsNil)
		c.Assert(id == 0 || id == id1, check.IsTrue)
		id = id1
	}
}

func (s *AvroSchemaRegistrySuite) TestGetCachedOrRegister(c *check.C) {
	defer testleak.AfterTest(c)()
	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "http://127.0.0.1:8081", "-value")
	c.Assert(err, check.IsNil)

	called := 0
	// nolint:unparam
	// NOTICE:This is a function parameter definition, so it cannot be modified.
	schemaGen := func() (string, error) {
		called++
		return `{
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
     }`, nil
	}

	codec, id, err := manager.GetCachedOrRegister(getTestingContext(), table, 1, schemaGen)
	c.Assert(err, check.IsNil)
	c.Assert(id, check.Greater, 0)
	c.Assert(codec, check.NotNil)
	c.Assert(called, check.Equals, 1)

	codec1, _, err := manager.GetCachedOrRegister(getTestingContext(), table, 1, schemaGen)
	c.Assert(err, check.IsNil)
	c.Assert(codec1, check.Equals, codec)
	c.Assert(called, check.Equals, 1)

	codec2, _, err := manager.GetCachedOrRegister(getTestingContext(), table, 2, schemaGen)
	c.Assert(err, check.IsNil)
	c.Assert(codec2, check.Not(check.Equals), codec)
	c.Assert(called, check.Equals, 2)

	schemaGen = func() (string, error) {
		return `{
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
     }`, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		finalI := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				codec, id, err := manager.GetCachedOrRegister(getTestingContext(), table, uint64(finalI), schemaGen)
				c.Assert(err, check.IsNil)
				c.Assert(id, check.Greater, 0)
				c.Assert(codec, check.NotNil)
			}
		}()
	}
	wg.Wait()
}

func (s *AvroSchemaRegistrySuite) TestHTTPRetry(c *check.C) {
	defer testleak.AfterTest(c)()
	payload := []byte("test")
	req, err := http.NewRequest("POST", "http://127.0.0.1:8081/may-fail", bytes.NewReader(payload))
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	resp, err := httpRetry(ctx, nil, req, false)
	c.Assert(err, check.IsNil)
	_ = resp.Body.Close()
}
