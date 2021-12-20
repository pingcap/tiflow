package rest

import (
	"net/url"
	"strings"

	"github.com/pingcap/ticdc/pkg/httputil"
)

// Enum types for HTTP methods.
type HTTPMethod int

// Valid HTTP methods.
const (
	HTTPMethodPost = iota
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

// RESTInterface includes a set of operations to interact with TiCDC RESTful apis.
type RESTInterface interface {
	Method(method string) *Request
	Post() *Request
	Put() *Request
	Get() *Request
	Delete() *Request
}

// RESTClient defines a TiCDC RESTful client
type RESTClient struct {
	// base is the root URL for all invocations of the client.
	base *url.URL

	// versionedAPIPath is a http url prefix with api version. eg. /api/v1.
	versionedAPIPath string

	// Client is a wrapped http client.
	Client *httputil.Client
}

// NewRESTClient creates a new RESTClient.
func NewRESTClient(baseURL *url.URL, versionedAPIPath string, client *httputil.Client) (*RESTClient, error) {
	if !strings.HasSuffix(baseURL.Path, "/") {
		baseURL.Path += "/"
	}
	baseURL.RawQuery = ""
	baseURL.Fragment = ""

	return &RESTClient{
		base:             baseURL,
		versionedAPIPath: versionedAPIPath,
		Client:           client,
	}, nil
}

// Method begins a request with a http method (GET, POST, PUT, DELETE).
func (c *RESTClient) Method(method HTTPMethod) *Request {
	return NewRequest(c).WithMethod(method)
}

// Post begins a POST request. Short for c.Method(HTTPMethodPost).
func (c *RESTClient) Post() *Request {
	return c.Method(HTTPMethodPost)
}

// Put begins a PUT request. Short for c.Method(HTTPMethodPut).
func (c *RESTClient) Put() *Request {
	return c.Method(HTTPMethodPut)
}

// Delete begins a DELETE request. Short for c.Method(HTTPMethodDelete).
func (c *RESTClient) Delete() *Request {
	return c.Method(HTTPMethodDelete)
}

// Get begins a GET request. Short for c.Method(HTTPMethodGet).
func (c *RESTClient) Get() *Request {
	return c.Method(HTTPMethodGet)
}
