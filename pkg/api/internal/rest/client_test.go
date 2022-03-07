// Copyright 2021 PingCAP, Inc.
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

package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func restClient(testServer *httptest.Server) (*CDCRESTClient, error) {
	c, err := CDCRESTClientFromConfig(&Config{
		Host:    testServer.URL,
		APIPath: "/api",
		Version: "v1",
	})
	return c, err
}

func TestRestRequestSuccess(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusOK)
		if r.URL.Path == "/api/v1/test" {
			_, _ = rw.Write([]byte(`{"cdc": "hello world"}`))
		}
	}))
	defer testServer.Close()

	c, err := restClient(testServer)
	require.Nil(t, err)
	body, err := c.Get().WithPrefix("test").Do(context.Background()).Raw()
	require.Equal(t, `{"cdc": "hello world"}`, string(body))
	require.NoError(t, err)
}

func TestRestRequestFailed(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusNotFound)
		_, _ = rw.Write([]byte(`{
			"error_msg": "test rest request failed",
			"error_code": "test rest request failed"
		}`))
	}))
	defer testServer.Close()

	c, err := restClient(testServer)
	require.Nil(t, err)
	err = c.Get().WithMaxRetries(1).Do(context.Background()).Error()
	require.NotNil(t, err)
}

func TestRestRawRequestFailed(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusNotFound)
		_, _ = rw.Write([]byte(`{
			"error_msg": "test rest request failed",
			"error_code": "test rest request failed"
		}`))
	}))
	defer testServer.Close()

	c, err := restClient(testServer)
	require.Nil(t, err)
	body, err := c.Get().WithMaxRetries(1).Do(context.Background()).Raw()
	require.NotNil(t, body)
	require.NotNil(t, err)
}

func TestHTTPMethods(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	c, _ := restClient(testServer)

	req := c.Post()
	require.NotNil(t, req)

	req = c.Get()
	require.NotNil(t, req)

	req = c.Put()
	require.NotNil(t, req)

	req = c.Delete()
	require.NotNil(t, req)
}
