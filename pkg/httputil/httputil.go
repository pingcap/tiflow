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
	"net/http"

	"github.com/pingcap/ticdc/pkg/security"
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
	return &Client{
		Client: http.Client{Transport: transport},
	}, nil
}
