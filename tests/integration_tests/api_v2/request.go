// Copyright 2023 PingCAP, Inc.
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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

const (
	defaultBackoffBaseDelayInMs = 500
	defaultBackoffMaxDelayInMs  = 30 * 1000
	// The write operations other than 'GET' are not idempotent,
	// so we only try one time in request.Do() and let users specify the retrying behaviour
	defaultMaxRetries = 1
)

type HTTPMethod int

const (
	HTTPMethodPost = iota + 1
	HTTPMethodPut
	HTTPMethodGet
	HTTPMethodDelete
)

// String implements Stringer.String.
func (h HTTPMethod) String() string {
	switch h {
	case HTTPMethodPost:
		return "POST"
	case HTTPMethodPut:
		return "PUT"
	case HTTPMethodGet:
		return "GET"
	case HTTPMethodDelete:
		return "DELETE"
	default:
		return "unknown"
	}
}

// CDCRESTClient defines a TiCDC RESTful client
type CDCRESTClient struct {
	// base is the root URL for all invocations of the client.
	base *url.URL

	// versionedAPIPath is a http url prefix with api version. eg. /api/v1.
	versionedAPIPath string

	// Client is a wrapped http client.
	Client *httputil.Client
}

// NewCDCRESTClient creates a new CDCRESTClient.
func NewCDCRESTClient(baseURL *url.URL, versionedAPIPath string, client *httputil.Client) (*CDCRESTClient, error) {
	if !strings.HasSuffix(baseURL.Path, "/") {
		baseURL.Path += "/"
	}
	baseURL.RawQuery = ""
	baseURL.Fragment = ""

	return &CDCRESTClient{
		base:             baseURL,
		versionedAPIPath: versionedAPIPath,
		Client:           client,
	}, nil
}

// Method begins a request with a http method (GET, POST, PUT, DELETE).
func (c *CDCRESTClient) Method(method HTTPMethod) *Request {
	return NewRequest(c).WithMethod(method)
}

// Post begins a POST request. Short for c.Method(HTTPMethodPost).
func (c *CDCRESTClient) Post() *Request {
	return c.Method(HTTPMethodPost)
}

// Put begins a PUT request. Short for c.Method(HTTPMethodPut).
func (c *CDCRESTClient) Put() *Request {
	return c.Method(HTTPMethodPut)
}

// Delete begins a DELETE request. Short for c.Method(HTTPMethodDelete).
func (c *CDCRESTClient) Delete() *Request {
	return c.Method(HTTPMethodDelete)
}

// Get begins a GET request. Short for c.Method(HTTPMethodGet).
func (c *CDCRESTClient) Get() *Request {
	return c.Method(HTTPMethodGet)
}

// Request allows for building up a request to cdc server in a chained fasion.
// Any errors are stored until the end of your call, so you only have to
// check once.
type Request struct {
	c       *CDCRESTClient
	timeout time.Duration

	// generic components accessible via setters
	method     HTTPMethod
	pathPrefix string
	params     url.Values
	headers    http.Header

	// retry options
	backoffBaseDelay time.Duration
	backoffMaxDelay  time.Duration
	maxRetries       uint64

	// output
	err  error
	body io.Reader
}

// NewRequest creates a new request.
func NewRequest(c *CDCRESTClient) *Request {
	var pathPrefix string
	if c.base != nil {
		pathPrefix = path.Join("/", c.base.Path, c.versionedAPIPath)
	} else {
		pathPrefix = path.Join("/", c.versionedAPIPath)
	}

	var timeout time.Duration
	if c.Client != nil {
		timeout = c.Client.Timeout()
	}

	r := &Request{
		c:          c,
		timeout:    timeout,
		pathPrefix: pathPrefix,
		maxRetries: 1,
	}
	r.WithHeader("Accept", "application/json")
	r.WithHeader(middleware.ClientVersionHeader, version.ReleaseVersion)
	return r
}

// NewRequestWithClient creates a Request with an embedded CDCRESTClient for test.
func NewRequestWithClient(base *url.URL, versionedAPIPath string, client *httputil.Client) *Request {
	return NewRequest(&CDCRESTClient{
		base:             base,
		versionedAPIPath: versionedAPIPath,
		Client:           client,
	})
}

// WithPrefix adds segments to the beginning of request url.
func (r *Request) WithPrefix(segments ...string) *Request {
	if r.err != nil {
		return r
	}

	r.pathPrefix = path.Join(r.pathPrefix, path.Join(segments...))
	return r
}

// WithURI sets the server relative URI.
func (r *Request) WithURI(uri string) *Request {
	if r.err != nil {
		return r
	}
	u, err := url.Parse(uri)
	if err != nil {
		r.err = err
		return r
	}
	r.pathPrefix = path.Join(r.pathPrefix, u.Path)
	vals := u.Query()
	if len(vals) > 0 {
		if r.params == nil {
			r.params = make(url.Values)
		}
		for k, v := range vals {
			r.params[k] = v
		}
	}
	return r
}

// WithParam sets the http request query params.
func (r *Request) WithParam(name, value string) *Request {
	if r.err != nil {
		return r
	}
	if r.params == nil {
		r.params = make(url.Values)
	}
	r.params[name] = append(r.params[name], value)
	return r
}

// WithMethod sets the method this request will use.
func (r *Request) WithMethod(method HTTPMethod) *Request {
	r.method = method
	return r
}

// WithHeader set the http request header.
func (r *Request) WithHeader(key string, values ...string) *Request {
	if r.headers == nil {
		r.headers = http.Header{}
	}
	r.headers.Del(key)
	for _, value := range values {
		r.headers.Add(key, value)
	}
	return r
}

// WithTimeout specifies overall timeout of a request.
func (r *Request) WithTimeout(d time.Duration) *Request {
	if r.err != nil {
		return r
	}
	r.timeout = d
	return r
}

// WithBackoffBaseDelay specifies the base backoff sleep duration.
func (r *Request) WithBackoffBaseDelay(delay time.Duration) *Request {
	if r.err != nil {
		return r
	}
	r.backoffBaseDelay = delay
	return r
}

// WithBackoffMaxDelay specifies the maximum backoff sleep duration.
func (r *Request) WithBackoffMaxDelay(delay time.Duration) *Request {
	if r.err != nil {
		return r
	}
	r.backoffMaxDelay = delay
	return r
}

// WithMaxRetries specifies the maximum times a request will retry.
func (r *Request) WithMaxRetries(maxRetries uint64) *Request {
	if r.err != nil {
		return r
	}
	if maxRetries > 0 {
		r.maxRetries = maxRetries
	} else {
		r.maxRetries = defaultMaxRetries
	}
	return r
}

// WithBody makes http request use obj as its body.
// only supports two types now:
//  1. io.Reader
//  2. type which can be json marshalled
func (r *Request) WithBody(obj interface{}) *Request {
	if r.err != nil {
		return r
	}

	if rd, ok := obj.(io.Reader); ok {
		r.body = rd
	} else {
		b, err := json.Marshal(obj)
		if err != nil {
			r.err = err
			return r
		}
		r.body = bytes.NewReader(b)
		r.WithHeader("Content-Type", "application/json")
	}
	return r
}

// URL returns the current working URL.
func (r *Request) URL() *url.URL {
	p := r.pathPrefix

	finalURL := &url.URL{}
	if r.c.base != nil {
		*finalURL = *r.c.base
	}
	finalURL.Path = p

	query := url.Values{}
	for key, values := range r.params {
		for _, value := range values {
			query.Add(key, value)
		}
	}

	finalURL.RawQuery = query.Encode()
	return finalURL
}

func (r *Request) newHTTPRequest(ctx context.Context) (*http.Request, error) {
	url := r.URL().String()
	req, err := http.NewRequest(r.method.String(), url, r.body)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.Header = r.headers
	return req, nil
}

// Do formats and executes the request.
func (r *Request) Do(ctx context.Context) (res *Result) {
	if r.err != nil {
		log.Info("error in request", zap.Error(r.err))
		return &Result{err: r.err}
	}

	client := r.c.Client
	if client == nil {
		client = &httputil.Client{}
	}

	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	baseDelay := r.backoffBaseDelay.Milliseconds()
	if baseDelay == 0 {
		baseDelay = defaultBackoffBaseDelayInMs
	}
	maxDelay := r.backoffMaxDelay.Milliseconds()
	if maxDelay == 0 {
		maxDelay = defaultBackoffMaxDelayInMs
	}
	maxRetries := r.maxRetries
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}

	fn := func() error {
		req, err := r.newHTTPRequest(ctx)
		if err != nil {
			return err
		}
		// rewind the request body when r.body is not nil
		if seeker, ok := r.body.(io.Seeker); ok && r.body != nil {
			if _, err := seeker.Seek(0, 0); err != nil {
				return cerrors.ErrRewindRequestBodyError
			}
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Error("failed to send a http request", zap.Error(err))
			return err
		}

		defer func() {
			if resp == nil {
				return
			}
			// close the body to let the TCP connection be reused after reconnecting
			// see https://github.com/golang/go/blob/go1.18.1/src/net/http/response.go#L62-L64
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()

		res = r.checkResponse(resp)
		if res.Error() != nil {
			return res.Error()
		}
		return nil
	}

	var err error
	if maxRetries > 1 {
		err = retry.Do(ctx, fn,
			retry.WithBackoffBaseDelay(baseDelay),
			retry.WithBackoffMaxDelay(maxDelay),
			retry.WithMaxTries(maxRetries),
			retry.WithIsRetryableErr(cerrors.IsRetryableError),
		)
	} else {
		err = fn()
	}

	if res == nil && err != nil {
		return &Result{err: err}
	}

	return
}

// check http response and unmarshal error message if necessary.
func (r *Request) checkResponse(resp *http.Response) *Result {
	var body []byte
	if resp.Body != nil {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return &Result{err: err}
		}
		body = data
	}

	contentType := resp.Header.Get("Content-Type")
	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusPartialContent {
		var jsonErr model.HTTPError
		err := json.Unmarshal(body, &jsonErr)
		if err == nil {
			err = errors.New(jsonErr.Error)
		} else {
			err = fmt.Errorf(
				"call cdc api failed, url=%s, "+
					"code=%d, contentType=%s, response=%s",
				r.URL().String(),
				resp.StatusCode, contentType, string(body))
		}

		return &Result{
			body:        body,
			contentType: contentType,
			statusCode:  resp.StatusCode,
			err:         err,
		}
	}

	return &Result{
		body:        body,
		contentType: contentType,
		statusCode:  resp.StatusCode,
	}
}

// Result contains the result of calling Request.Do().
type Result struct {
	body        []byte
	contentType string
	err         error
	statusCode  int
}

// Raw returns the raw result.
func (r Result) Raw() ([]byte, error) {
	return r.body, r.err
}

// Error returns the request error.
func (r Result) Error() error {
	return r.err
}

// Into stores the http response body into obj.
func (r Result) Into(obj interface{}) error {
	if r.err != nil {
		return r.err
	}

	if len(r.body) == 0 {
		return cerrors.ErrZeroLengthResponseBody.GenWithStackByArgs(r.statusCode)
	}

	return json.Unmarshal(r.body, obj)
}
