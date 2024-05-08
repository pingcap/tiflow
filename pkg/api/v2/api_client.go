// Copyright 2022 PingCAP, Inc.
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

package v2

import (
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
	"github.com/pingcap/tiflow/pkg/security"
)

// APIV2Interface is an abstraction for TiCDC capture/changefeed/processor operations.
// We can create a fake api client which mocks TiCDC operations by implement this interface.
type APIV2Interface interface {
	RESTClient() rest.CDCRESTInterface
	ChangefeedsGetter
	TsoGetter
	UnsafeGetter
	StatusGetter
	CapturesGetter
	ProcessorsGetter
}

// APIV2Client implements APIV1Interface and it is used to interact with cdc owner http api.
type APIV2Client struct {
	restClient rest.CDCRESTInterface
}

// RESTClient returns a RESTClient that is used to communicate with owner api
// by this client implementation.
func (c *APIV2Client) RESTClient() rest.CDCRESTInterface {
	if c == nil {
		return nil
	}
	return c.restClient
}

// Tso returns a TsoInterface to communicate with cdc api
func (c *APIV2Client) Tso() TsoInterface {
	if c == nil {
		return nil
	}
	return newTso(c)
}

// Unsafe returns a UnsafeInterface to communicate with cdc api
func (c *APIV2Client) Unsafe() UnsafeInterface {
	if c == nil {
		return nil
	}
	return newUnsafe(c)
}

// Changefeeds returns a ChangefeedInterface with cdc api
func (c *APIV2Client) Changefeeds() ChangefeedInterface {
	if c == nil {
		return nil
	}
	return newChangefeeds(c)
}

// Status returns a StatusInterface to communicate with cdc api
func (c *APIV2Client) Status() StatusInterface {
	if c == nil {
		return nil
	}
	return newStatus(c)
}

// Captures returns a CaptureInterface which abstracts capture operations.
func (c *APIV2Client) Captures() CaptureInterface {
	return newCaptures(c)
}

// Processors returns a ProcessorInterface abstracting processor operations.
func (c *APIV2Client) Processors() ProcessorInterface {
	if c == nil {
		return nil
	}
	return newProcessors(c)
}

// NewAPIClient creates a new APIV1Client.
func NewAPIClient(serverAddr string, credential *security.Credential, values url.Values) (*APIV2Client, error) {
	c := &rest.Config{}
	c.APIPath = "/api"
	c.Version = "v2"
	c.Host = serverAddr
	c.Credential = credential
	c.Values = values
	client, err := rest.CDCRESTClientFromConfig(c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &APIV2Client{client}, nil
}
