package rest

import (
	"net/url"
	"path"

	"github.com/pingcap/errors"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
)

// Config holds the common attributes that can be passed to a cdc REST client
type Config struct {
	// Host must be a host string, a host:port pair, or a URL to the base of the cdc server.
	Host string
	// APIPath is a sub-path that points to an API root.
	APIPath string
	// Credential holds the security Credential used for generating tls config
	Credential *security.Credential
	// API verion
	Version string
}

// defaultServerUrlFromConfig is used to build base URL and api path.
func defaultServerUrlFromConfig(config *Config) (*url.URL, string, error) {
	host := config.Host
	if host == "" {
		host = "127.0.0.1:8300"
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
			return nil, "", cerrors.ErrInvalidHost.GenWithStackByArgs(base)
		}
	}
	versionedPath := path.Join("/", config.APIPath, config.Version)
	return hostURL, versionedPath, nil
}

// RESTClientFromConfig creates a RESTClient from specific config items.
func RESTClientFromConfig(config *Config) (*RESTClient, error) {
	if config.Version == "" {
		return nil, errors.New("Version is required when initializing a RESTClient")
	}
	if config.APIPath == "" {
		return nil, errors.New("APIPath is required when initializing a RESTClient")
	}

	httpClient, err := httputil.NewClient(config.Credential)
	if err != nil {
		return nil, errors.Trace(err)
	}

	baseURL, versionedAPIPath, err := defaultServerUrlFromConfig(config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	restClient, err := NewRESTClient(baseURL, versionedAPIPath, httpClient)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return restClient, nil
}
