package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func restClient(testServer *httptest.Server) (*RESTClient, error) {
	c, err := RESTClientFor(&Config{
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
			rw.Write([]byte(`{"cdc": "hello world"}`))
		}
	}))
	defer testServer.Close()

	c, err := restClient(testServer)
	require.Nil(t, err)
	body, err := c.Get().Prefix("test").Do(context.Background()).Raw()
	require.Equal(t, `{"cdc": "hello world"}`, string(body))
}

func TestRestRequestFailed(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte(`{
			"error_msg": "test rest request failed",
			"error_code": "test rest request failed"
		}`))
	}))
	defer testServer.Close()

	c, err := restClient(testServer)
	require.Nil(t, err)
	err = c.Get().MaxRetries(1).Do(context.Background()).Error()
	require.NotNil(t, err)
}

func TestRestRawRequestFailed(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusNotFound)
		rw.Write([]byte(`{
			"error_msg": "test rest request failed",
			"error_code": "test rest request failed"
		}`))
	}))
	defer testServer.Close()

	c, err := restClient(testServer)
	require.Nil(t, err)
	body, err := c.Get().Do(context.Background()).Raw()
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
