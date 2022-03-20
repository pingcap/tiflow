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
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type AvroSchemaRegistrySuite struct {
	suite.Suite
}

func TestAvroSchemaRegistrySuite(t *testing.T) {
	suite.Run(t, new(AvroSchemaRegistrySuite))
}

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

func startHTTPInterceptForTestingRegistry(t *testing.T) {
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
			reqBody, err := io.ReadAll(req.Body)
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
			data, _ := io.ReadAll(req.Body)
			require.Greater(t, len(data), 0)
			require.Equal(t, int64(len(data)), req.ContentLength)
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

func (s *AvroSchemaRegistrySuite) SetupSuite() {
	startHTTPInterceptForTestingRegistry(s.T())
}

func (s *AvroSchemaRegistrySuite) TearDownSuite() {
	stopHTTPInterceptForTestingRegistry()
}

func getTestingContext() context.Context {
	// nolint:govet
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	return ctx
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistry() {
	defer testleak.AfterTest(s.T())()
	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "http://127.0.0.1:8081", "-value")
	require.Nil(s.T(), err)

	err = manager.ClearRegistry(getTestingContext(), table)
	require.Nil(s.T(), err)

	_, _, err = manager.Lookup(getTestingContext(), table, 1)
	require.Error(s.T(), err, `.*not\sfound.*`)

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
	require.Nil(s.T(), err)

	_, err = manager.Register(getTestingContext(), table, codec)
	require.Nil(s.T(), err)

	var id int
	for i := 0; i < 2; i++ {
		_, id, err = manager.Lookup(getTestingContext(), table, 1)
		require.Nil(s.T(), err)
		require.Greater(s.T(), id, 0)
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
	require.Nil(s.T(), err)
	_, err = manager.Register(getTestingContext(), table, codec)
	require.Nil(s.T(), err)

	codec2, id2, err := manager.Lookup(getTestingContext(), table, 999)
	require.Nil(s.T(), err)
	require.NotEqual(s.T(), id2, id)
	require.Equal(s.T(), codec.CanonicalSchema(), codec2.CanonicalSchema())
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistryBad() {
	defer testleak.AfterTest(s.T())()
	_, err := NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "http://127.0.0.1:808", "-value")
	require.NotNil(s.T(), err)

	_, err = NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "https://127.0.0.1:8080", "-value")
	require.NotNil(s.T(), err)
}

func (s *AvroSchemaRegistrySuite) TestSchemaRegistryIdempotent() {
	defer testleak.AfterTest(s.T())()
	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "http://127.0.0.1:8081", "-value")
	require.Nil(s.T(), err)
	for i := 0; i < 20; i++ {
		err = manager.ClearRegistry(getTestingContext(), table)
		require.Nil(s.T(), err)
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
	require.Nil(s.T(), err)

	id := 0
	for i := 0; i < 20; i++ {
		id1, err := manager.Register(getTestingContext(), table, codec)
		require.Nil(s.T(), err)
		require.True(s.T(), id == 0 || id == id1)
		id = id1
	}
}

func (s *AvroSchemaRegistrySuite) TestGetCachedOrRegister() {
	defer testleak.AfterTest(s.T())()
	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), &security.Credential{}, "http://127.0.0.1:8081", "-value")
	require.Nil(s.T(), err)

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
	require.Nil(s.T(), err)
	require.Greater(s.T(), id, 0)
	require.NotNil(s.T(), codec)
	require.Equal(s.T(), called, 1)

	codec1, _, err := manager.GetCachedOrRegister(getTestingContext(), table, 1, schemaGen)
	require.Nil(s.T(), err)
	require.Equal(s.T(), codec1, codec)
	require.Equal(s.T(), called, 1)

	codec2, _, err := manager.GetCachedOrRegister(getTestingContext(), table, 2, schemaGen)
	require.Nil(s.T(), err)
	require.NotEqual(s.T(), codec2, codec)
	require.Equal(s.T(), called, 2)

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
				require.Nil(s.T(), err)
				require.Greater(s.T(), id, 0)
				require.NotNil(s.T(), codec)
			}
		}()
	}
	wg.Wait()
}

func (s *AvroSchemaRegistrySuite) TestHTTPRetry() {
	defer testleak.AfterTest(s.T())()
	payload := []byte("test")
	req, err := http.NewRequest("POST", "http://127.0.0.1:8081/may-fail", bytes.NewReader(payload))
	require.Nil(s.T(), err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	resp, err := httpRetry(ctx, nil, req, false)
	require.Nil(s.T(), err)
	_ = resp.Body.Close()
}
