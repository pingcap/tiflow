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
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func getTestingContext() context.Context {
	// nolint:govet
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	return ctx
}

func TestSchemaRegistry(t *testing.T) {
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	topic := "cdctest"

	err = manager.ClearRegistry(ctx, topic)
	require.NoError(t, err)

	_, err = manager.Lookup(ctx, topic, schemaID{confluentSchemaID: 1})
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

	schemaID, err := manager.Register(ctx, topic, codec.Schema())
	require.NoError(t, err)

	codec2, err := manager.Lookup(ctx, topic, schemaID)
	require.NoError(t, err)
	require.Equal(t, codec.CanonicalSchema(), codec2.CanonicalSchema())

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
	schemaID, err = manager.Register(ctx, topic, codec.Schema())
	require.NoError(t, err)

	codec2, err = manager.Lookup(ctx, topic, schemaID)
	require.NoError(t, err)
	require.Equal(t, codec.CanonicalSchema(), codec2.CanonicalSchema())
}

func TestSchemaRegistryBad(t *testing.T) {
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	ctx := getTestingContext()
	_, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:808", nil)
	require.Error(t, err)

	_, err = NewConfluentSchemaManager(ctx, "https://127.0.0.1:8080", nil)
	require.Error(t, err)
}

func TestSchemaRegistryRedactsNestedURLErrors(t *testing.T) {
	const redirectToken = "test-redirect-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://example.invalid/%zz?token="+redirectToken)
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	for _, tc := range []struct {
		name, registryURL, credential, reason string
	}{
		{"invalid port", "https://user:test-registry-password", "test-registry-password", "invalid port"},
		{"malformed redirect", server.URL, redirectToken, "invalid redirect Location header"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zap.ErrorLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.ErrorLevel)}))

			_, err := NewConfluentSchemaManager(t.Context(), tc.registryURL, nil)
			require.Error(t, err)
			require.NotContains(t, err.Error(), tc.credential)
			require.ErrorContains(t, err, tc.reason)

			entries := logs.All()
			require.Len(t, entries, 1)
			require.Equal(t, "Test connection to Schema Registry failed", entries[0].Message)
			loggedErr := entries[0].ContextMap()["error"].(string)
			require.NotContains(t, loggedErr, tc.credential)
			require.Contains(t, loggedErr, tc.reason)
		})
	}
}

func TestSchemaRegistryIdempotent(t *testing.T) {
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	require.NoError(t, err)

	topic := "cdctest"

	for i := 0; i < 20; i++ {
		err = manager.ClearRegistry(ctx, topic)
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
		id1, err := manager.Register(ctx, topic, codec.Schema())
		require.NoError(t, err)
		require.True(t, id == 0 || id == id1.confluentSchemaID)
		id = id1.confluentSchemaID
	}
}

func TestGetCachedOrRegister(t *testing.T) {
	startHTTPInterceptForTestingRegistry()
	defer stopHTTPInterceptForTestingRegistry()

	ctx := getTestingContext()
	manager, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
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

	codec, header, err := manager.GetCachedOrRegister(ctx, topic, 1, schemaGen)
	require.NoError(t, err)
	cID, err := getConfluentSchemaIDFromHeader(header)
	require.NoError(t, err)
	require.Greater(t, cID, uint32(0))
	require.NotNil(t, codec)
	require.Equal(t, 1, called)

	codec1, _, err := manager.GetCachedOrRegister(ctx, topic, 1, schemaGen)
	require.NoError(t, err)
	require.True(t, codec == codec1) // check identity
	require.Equal(t, 1, called)

	codec2, _, err := manager.GetCachedOrRegister(ctx, topic, 2, schemaGen)
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
				codec, header, err := manager.GetCachedOrRegister(
					ctx,
					topic,
					uint64(finalI),
					schemaGen,
				)
				require.NoError(t, err)
				cID, err := getConfluentSchemaIDFromHeader(header)
				require.NoError(t, err)
				require.Greater(t, cID, uint32(0))
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
