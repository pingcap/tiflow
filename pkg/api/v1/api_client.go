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

package v1

import (
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
	"github.com/pingcap/tiflow/pkg/security"
)

// APIV1Interface is an abstraction for TiCDC capture/changefeed/processor operations.
// We can create a fake api client which mocks TiCDC operations by implement this interface.
type APIV1Interface interface {
	RESTClient() rest.CDCRESTInterface
	CapturesGetter
	ChangefeedsGetter
	ProcessorsGetter
	StatusGetter
}

// APIV1Client implements APIV1Interface and it is used to interact with cdc owner http api.
type APIV1Client struct {
	restClient rest.CDCRESTInterface
}

// Captures returns a CaptureInterface which abstracts capture operations.
func (c *APIV1Client) Captures() CaptureInterface {
	return newCaptures(c)
}

// Changefeeds returns a ChangefeedInterface which abstracts changefeed operations.
func (c *APIV1Client) Changefeeds() ChangefeedInterface {
	return newChangefeeds(c)
}

// Processors returns a ProcessorInterface which abstracts processor operations.
func (c *APIV1Client) Processors() ProcessorInterface {
	return newProcessors(c)
}

// RESTClient returns a RESTClient that is used to communicate with owner api
// by this client implementation.
func (c *APIV1Client) RESTClient() rest.CDCRESTInterface {
	if c == nil {
		return nil
	}
	return c.restClient
}

// Status returns a StatusInterface to communicate with cdc api
func (c *APIV1Client) Status() StatusInterface {
	if c == nil {
		return nil
	}
	return newStatus(c)
}

// NewAPIClient creates a new APIV1Client.
func NewAPIClient(ownerAddr string, credential *security.Credential) (*APIV1Client, error) {
	c := &rest.Config{}
	c.APIPath = "/api"
	c.Version = "v1"
	c.Host = ownerAddr
	c.Credential = credential
	client, err := rest.CDCRESTClientFromConfig(c)
	if err != nil {
		return nil, err
	}

	return &APIV1Client{client}, nil
}
