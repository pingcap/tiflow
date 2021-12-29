package v1

import (
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
	"github.com/pingcap/tiflow/pkg/security"
)

// ApiV1Interface is an abstraction for TiCDC capture/changefeed/processor operations.
// We can create a fake api client which mocks TiCDC operations by implement this interface.
type ApiV1Interface interface {
	RESTClient() rest.CDCRESTInterface
	CapturesGetter
	ChangefeedsGetter
	ProcessorsGetter
}

// ApiV1Client implements ApiV1Interface and it is used to interact with cdc owner http api.
type ApiV1Client struct {
	restClient rest.CDCRESTInterface
}

// Captures returns a CaptureInterface which abstracts capture operations.
func (c *ApiV1Client) Captures() CaptureInterface {
	return newCaptures(c)
}

// Changefeeds returns a ChangefeedInterface which abstracts changefeed operations.
func (c *ApiV1Client) Changefeeds() ChangefeedInterface {
	return newChangefeeds(c)
}

// Processors returns a ProcessorInterface which abstracts processor operations.
func (c *ApiV1Client) Processors() ProcessorInterface {
	return newProcessors(c)
}

// RESTClient returns a RESTClient that is used to communicate with owner api
// by this client implementation.
func (c *ApiV1Client) RESTClient() rest.CDCRESTInterface {
	if c == nil {
		return nil
	}
	return c.restClient
}

// NewApiClient creates a new ApiV1Client.
func NewApiClient(ownerAddr string, credential *security.Credential) (*ApiV1Client, error) {
	c := &rest.Config{}
	c.APIPath = "/api"
	c.Version = "v1"
	c.Host = ownerAddr
	c.Credential = credential
	client, err := rest.CDCRESTClientFromConfig(c)
	if err != nil {
		return nil, err
	}

	return &ApiV1Client{client}, nil
}
