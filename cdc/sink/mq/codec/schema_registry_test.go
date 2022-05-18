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

func startHTTPInterceptForTestingRegistry() {
	httpmock.Activate()

	registry := mockRegistry{
		subjects: make(map[string]*mockRegistrySchema),
		newID:    1,
	}

	httpmock.RegisterResponder(
		"GET",
		"http://127.0.0.1:8081",
		httpmock.NewStringResponder(200, "{}"),
	)

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

			var respData registerResponse
			registry.mu.Lock()
			item, exists := registry.subjects[subject]
			if !exists {
				item = &mockRegistrySchema{
					content: reqData.Schema,
					version: 1,
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
			item, exists := registry.subjects[subject]
			if !exists {
				return httpmock.NewStringResponse(404, ""), nil
			}

			delete(registry.subjects, subject)
			// simplify the response not returning all the versions
			return httpmock.NewJsonResponse(200, []int{item.version})
		})

	failCounter := 0
	httpmock.RegisterResponder("POST", `=~^http://127.0.0.1:8081/may-fail`,
		func(req *http.Request) (*http.Response, error) {
			io.ReadAll(req.Body)
			if failCounter < 3 {
				failCounter++
				return httpmock.NewStringResponse(500, ""), nil
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
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	manager, err := NewAvroSchemaManager(
		getTestingContext(),
		nil,
		"http://127.0.0.1:8081",
		"-value",
	)
	require.NoError(t, err)

	topic := "cdctest"

	err = manager.ClearRegistry(getTestingContext(), topic)
	require.NoError(t, err)

	_, _, err = manager.Lookup(getTestingContext(), topic, 1)
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
	require.NoError(t, err)

	_, err = manager.Register(getTestingContext(), topic, codec)
	require.NoError(t, err)

	var id int
	for i := 0; i < 2; i++ {
		_, id, err = manager.Lookup(getTestingContext(), topic, 1)
		require.NoError(t, err)
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
	require.NoError(t, err)
	_, err = manager.Register(getTestingContext(), topic, codec)
	require.NoError(t, err)

	codec2, id2, err := manager.Lookup(getTestingContext(), topic, 999)
	require.NoError(t, err)
	require.NotEqual(t, id, id2)
	require.Equal(t, codec.CanonicalSchema(), codec2.CanonicalSchema())
}

func TestSchemaRegistryBad(t *testing.T) {
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	_, err := NewAvroSchemaManager(getTestingContext(), nil, "http://127.0.0.1:808", "-value")
	require.NotNil(t, err)

	_, err = NewAvroSchemaManager(getTestingContext(), nil, "https://127.0.0.1:8080", "-value")
	require.NotNil(t, err)
}

func TestSchemaRegistryIdempotent(t *testing.T) {
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	manager, err := NewAvroSchemaManager(
		getTestingContext(),
		nil,
		"http://127.0.0.1:8081",
		"-value",
	)
	require.NoError(t, err)

	topic := "cdctest"

	for i := 0; i < 20; i++ {
		err = manager.ClearRegistry(getTestingContext(), topic)
		require.NoError(t, err)
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
	require.NoError(t, err)

	id := 0
	for i := 0; i < 20; i++ {
		id1, err := manager.Register(getTestingContext(), topic, codec)
		require.NoError(t, err)
		require.True(t, id == 0 || id == id1)
		id = id1
	}
}

func TestGetCachedOrRegister(t *testing.T) {
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	manager, err := NewAvroSchemaManager(
		getTestingContext(),
		nil,
		"http://127.0.0.1:8081",
		"-value",
	)
	require.NoError(t, err)

	called := 0
	// nolint:unparam
	// NOTICE:This is a function parameter definition, so it cannot be modified.
	schemaGen := func() (string, error) {
		called++
		return `{
       "type": "record",
       "name": "test1",
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
	topic := "cdctest"

	codec, id, err := manager.GetCachedOrRegister(getTestingContext(), topic, 1, schemaGen)
	require.NoError(t, err)
	require.Greater(t, id, 0)
	require.NotNil(t, codec)
	require.Equal(t, 1, called)

	codec1, _, err := manager.GetCachedOrRegister(getTestingContext(), topic, 1, schemaGen)
	require.NoError(t, err)
	require.True(t, codec == codec1) // check identity
	require.Equal(t, 1, called)

	codec2, _, err := manager.GetCachedOrRegister(getTestingContext(), topic, 2, schemaGen)
	require.NoError(t, err)
	require.NotEqual(t, codec, codec2)
	require.Equal(t, 2, called)

	schemaGen = func() (string, error) {
		return `{
       "type": "record",
       "name": "test1",
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
				codec, id, err := manager.GetCachedOrRegister(
					getTestingContext(),
					topic,
					uint64(finalI),
					schemaGen,
				)
				require.NoError(t, err)
				require.Greater(t, id, 0)
				require.NotNil(t, codec)
			}
		}()
	}
	wg.Wait()
}

func TestHTTPRetry(t *testing.T) {
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	payload := []byte("test")
	req, err := http.NewRequestWithContext(ctx,
		"POST", "http://127.0.0.1:8081/may-fail", bytes.NewReader(payload))
	require.NoError(t, err)

	resp, err := httpRetry(ctx, nil, req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	_ = resp.Body.Close()
}
