package rest

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/stretchr/testify/require"
)

func TestRequestParams(t *testing.T) {
	req := (&Request{}).Param("foo", "bar")
	require.Equal(t, req.params, url.Values{"foo": []string{"bar"}})

	req.Param("hello", "world")
	require.Equal(t, req.params, url.Values{"foo": []string{"bar"}, "hello": []string{"world"}})
}

func TestRequestURI(t *testing.T) {
	req := (&Request{}).Param("foo", "bar").Prefix("test")
	req.URI("/production?foo=hello&val=1024")
	require.Equal(t, req.pathPrefix, "/production")
	require.Equal(t, req.params, url.Values{"foo": []string{"hello"}, "val": []string{"1024"}})
}

type testStruct struct {
	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

func TestRequestBody(t *testing.T) {
	// test unsupported data type
	req := (&Request{}).Body(func() {})
	require.NotNil(t, req.err)
	require.Nil(t, req.body)

	// test data type which can be json marshalled
	p := &testStruct{Foo: "hello", Bar: 10}
	req = (&Request{}).Body(p)
	require.Nil(t, req.err)
	require.NotNil(t, req.body)

	// test data type io.Reader
	req = (&Request{}).Body(bytes.NewReader([]byte(`{"hello": "world"}`)))
	require.Nil(t, req.err)
	require.NotNil(t, req.body)
}

type clientFunc func(req *http.Request) (*http.Response, error)

func (f clientFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testClient(fn clientFunc) *httputil.Client {
	return &httputil.Client{
		Client: http.Client{
			Transport: fn,
		},
	}
}

func TestRequestHeader(t *testing.T) {
	cli := testClient(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, req.Header.Get("signature"), "test-header1")

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewReader([]byte{})),
		}, nil
	})
	req := NewRequestWithClient(&url.URL{Path: "/test"}, "", nil).Verb("get")
	req.SetHeader("signature", "test-header2")
	req.SetHeader("signature", "test-header1")
	req.c.Client = cli

	_ = req.Do(context.Background())
}

func TestRequestDoContext(t *testing.T) {
	received := make(chan struct{})
	blocked := make(chan struct{})
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		close(received)
		<-blocked
		rw.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()
	defer close(blocked)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-received
		cancel()
	}()
	c, err := RESTClientFor(&Config{
		Host:    testServer.URL,
		APIPath: "/api",
		Version: "v1",
	})
	require.Nil(t, err)
	err = c.Get().
		Prefix("/test").
		Timeout(time.Second).
		Do(ctx).
		Error()
	require.NotNil(t, err)
}

func TestRequestDoContextTimeout(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		rw.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := RESTClientFor(&Config{
		Host:    testServer.URL,
		APIPath: "/api",
		Version: "v1",
	})
	require.Nil(t, err)
	err = c.Get().
		Prefix("/test").
		Timeout(time.Second).
		Do(ctx).
		Error()
	require.NotNil(t, err)
}

func TestResultIntoError(t *testing.T) {
	result := Result{err: errors.New("test-error")}
	err := result.Into(&testStruct{})
	require.Equal(t, result.err, err)

	result = Result{
		body: []byte(`{"foo": "hello", "bar": 10}`),
	}

	var res testStruct
	err = result.Into(&res)
	require.Nil(t, err)
	require.Equal(t, res.Foo, "hello")
	require.Equal(t, res.Bar, 10)
}

func TestResultZeroLengthBody(t *testing.T) {
	result := Result{
		body: []byte{},
	}
	err := result.Into(&testStruct{})
	require.NotNil(t, err)
	require.Equal(t, strings.Contains(err.Error(), "0-length"), true)
}
