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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/security"
)

// Client wraps an HTTP client and support TLS requests.
type Client struct {
	http.Client
}

// NewClient creates an HTTP client with the given Credential.
func NewClient(credential *security.Credential) (*Client, error) {
	transport := http.DefaultTransport
	if credential != nil {
		tlsConf, err := credential.ToTLSConfig()
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
		Client: http.Client{Transport: transport},
	}, nil
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
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("[%d] %s", resp.StatusCode, content)
	}
	return content, nil
}
