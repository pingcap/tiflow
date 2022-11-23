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

package httputil

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/security"
)

// Client wraps an HTTP client and support TLS requests.
type Client struct {
	client http.Client
}

// NewClient creates an HTTP client with the given Credential.
func NewClient(credential *security.Credential) (*Client, error) {
	transport := http.DefaultTransport
	if credential != nil {
		tlsConf, err := credential.ToTLSConfigWithVerify()
		if err != nil {
			return nil, err
		}
		if tlsConf != nil {
			httpTrans := http.DefaultTransport.(*http.Transport).Clone()
			httpTrans.TLSClientConfig = tlsConf
			transport = httpTrans
		}
	}
	// TODO: specific timeout in http client
	return &Client{
		client: http.Client{Transport: transport},
	}, nil
}

// Timeout returns the timeout of the client.
func (c *Client) Timeout() time.Duration {
	return c.client.Timeout
}

// SetTimeout specifies a time limit for requests made by this Client.
// See http.Client.Timeout.
func (c *Client) SetTimeout(timeout time.Duration) {
	c.client.Timeout = timeout
}

// Get issues a GET to the specified URL with context.
// See http.Client.Get.
func (c *Client) Get(ctx context.Context, url string) (resp *http.Response, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.client.Do(req)
}

// PostForm issues a POST to the specified URL,
// with data's keys and values URL-encoded as the request body.
// See http.Client.PostForm.
func (c *Client) PostForm(
	ctx context.Context, url string, data url.Values,
) (resp *http.Response, err error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, url, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, errors.Trace(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return c.client.Do(req)
}

// Do sends an HTTP request and returns an HTTP response.
// See http.Client.Do.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}

// DoRequest sends an request and returns an HTTP response content.
func (c *Client) DoRequest(
	ctx context.Context, url, method string, headers http.Header, body io.Reader,
) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for key, values := range headers {
		for _, v := range values {
			req.Header.Add(key, v)
		}
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// treat http 2xx as valid
	if resp.StatusCode/100 != 2 {
		return nil, errors.Errorf("[%d] %s", resp.StatusCode, content)
	}
	return content, nil
}

// CloseIdleConnections closes any connections are now sitting idle.
// See http.Client.CloseIdleConnections.
func (c *Client) CloseIdleConnections() {
	c.client.CloseIdleConnections()
}
