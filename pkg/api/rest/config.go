package rest

import (
	"fmt"
	"net/url"
	"path"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"golang.org/x/time/rate"
)

// Config holds the common attributes that can be passed to a cdc REST client
type Config struct {
	// Host must be a host string, a host:port pair, or a URL to the base of the cdc server.
	Host string
	// APIPath is a sub-path that points to an API root.
	APIPath string
	// Credential holds the security Credential used for generating tls config
	Credential *security.Credential
	// Rate limiter for limiting connections to the cdc owner from this client.
	RateLimiter *rate.Limiter
	// Api verion
	Version string
}

// defaultServerUrlFor is used to build base URL and api path.
func defaultServerUrlFor(config *Config) (*url.URL, string, error) {
	host := config.Host
	if host == "" {
		host = "127.0.0.1"
	}
	base := host
	hostURL, err := url.Parse(base)
	if err != nil || hostURL.Scheme == "" || hostURL.Host == "" {
		scheme := "http://"
		if config.Credential != nil && config.Credential.IsTLSEnabled() {
			scheme = "https://"
		}
		hostURL, err = url.Parse(scheme + base)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		if hostURL.Path != "" && hostURL.Path != "/" {
			return nil, "", fmt.Errorf("host must be a URL or a host:port pair: %q", base)
		}
	}
	versionedPath := path.Join("/", config.APIPath, config.Version)
	return hostURL, versionedPath, nil
}

func RESTClientForConfig(config *Config, httpClient *httputil.Client) (*RESTClient, error) {
	baseURL, versionedAPIPath, err := defaultServerUrlFor(config)
	restClient, err := NewRESTClient(baseURL, versionedAPIPath, config.RateLimiter, httpClient)
	return restClient, errors.Trace(err)
}

func RESTClientFor(config *Config) (*RESTClient, error) {
	if config.Version == "" {
		return nil, errors.New("Version is required when initializing a RESTClient")
	}
	if config.APIPath == "" {
		return nil, errors.New("APIPath is required when initializing a RESTClient")
	}
	_, _, err := defaultServerUrlFor(config)
	if err != nil {
		return nil, err
	}

	httpClient, err := httputil.NewClient(config.Credential)
	if err != nil {
		return nil, err
	}

	return RESTClientForConfig(config, httpClient)
}
