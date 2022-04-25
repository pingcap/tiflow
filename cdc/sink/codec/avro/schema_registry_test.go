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

package avro

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
	"github.com/stretchr/testify/require"
)

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

			// require.Equal(t, "AVRO", reqData.SchemaType)

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
			require.Equal(t, req.ContentLength, int64(len(data)))
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

func getTestingContext() context.Context {
	// nolint:govet
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	return ctx
}

func TestSchemaRegistry(t *testing.T) {
	startHTTPInterceptForTestingRegistry(t)
	defer stopHTTPInterceptForTestingRegistry()

	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), nil, "http://127.0.0.1:8081", "-value")
	require.Nil(t, err)

	err = manager.ClearRegistry(getTestingContext(), table)
	require.Nil(t, err)

	_, _, err = manager.Lookup(getTestingContext(), table, 1)
	require.Regexp(t, `.*not\sfound.*`, err)

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
	require.Nil(t, err)

	_, err = manager.Register(getTestingContext(), table, codec)
	require.Nil(t, err)

	var id int
	for i := 0; i < 2; i++ {
		_, id, err = manager.Lookup(getTestingContext(), table, 1)
		require.Nil(t, err)
		require.Greater(t, id, 0)
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
	require.Nil(t, err)
	_, err = manager.Register(getTestingContext(), table, codec)
	require.Nil(t, err)

	codec2, id2, err := manager.Lookup(getTestingContext(), table, 999)
	require.Nil(t, err)
	require.NotEqual(t, id, id2)
	require.Equal(t, codec2.CanonicalSchema(), codec.CanonicalSchema())
}

func TestSchemaRegistryBad(t *testing.T) {
	startHTTPInterceptForTestingRegistry(t)
	defer stopHTTPInterceptForTestingRegistry()

	_, err := NewAvroSchemaManager(getTestingContext(), nil, "http://127.0.0.1:808", "-value")
	require.NotNil(t, err)

	_, err = NewAvroSchemaManager(getTestingContext(), nil, "https://127.0.0.1:8080", "-value")
	require.NotNil(t, err)
}

func TestSchemaRegistryIdempotent(t *testing.T) {
	startHTTPInterceptForTestingRegistry(t)
	defer stopHTTPInterceptForTestingRegistry()
	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), nil, "http://127.0.0.1:8081", "-value")
	require.Nil(t, err)
	for i := 0; i < 20; i++ {
		err = manager.ClearRegistry(getTestingContext(), table)
		require.Nil(t, err)
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
	require.Nil(t, err)

	id := 0
	for i := 0; i < 20; i++ {
		id1, err := manager.Register(getTestingContext(), table, codec)
		require.Nil(t, err)
		require.True(t, id == 0 || id == id1)
		id = id1
	}
}

func TestGetCachedOrRegister(t *testing.T) {
	startHTTPInterceptForTestingRegistry(t)
	defer stopHTTPInterceptForTestingRegistry()

	table := model.TableName{
		Schema: "testdb",
		Table:  "test1",
	}

	manager, err := NewAvroSchemaManager(getTestingContext(), nil, "http://127.0.0.1:8081", "-value")
	require.Nil(t, err)

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
	require.Nil(t, err)
	require.Greater(t, id, 0)
	require.NotNil(t, codec)
	require.Equal(t, 1, called)

	codec1, _, err := manager.GetCachedOrRegister(getTestingContext(), table, 1, schemaGen)
	require.Nil(t, err)
	require.Equal(t, codec, codec1)
	require.Equal(t, 1, called)

	codec2, _, err := manager.GetCachedOrRegister(getTestingContext(), table, 2, schemaGen)
	require.Nil(t, err)
	require.NotEqual(t, codec, codec2)
	require.Equal(t, 2, called)

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
				require.Nil(t, err)
				require.Greater(t, id, 0)
				require.NotNil(t, codec)
			}
		}()
	}
	wg.Wait()
}

func TestHTTPRetry(t *testing.T) {
	startHTTPInterceptForTestingRegistry(t)
	defer stopHTTPInterceptForTestingRegistry()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	payload := []byte("test")
	req, err := http.NewRequestWithContext(ctx,
		"POST", "http://127.0.0.1:8081/may-fail", bytes.NewReader(payload))
	require.Nil(t, err)

	resp, err := httpRetry(ctx, nil, req, false)
	require.Nil(t, err)
	_ = resp.Body.Close()
}
