package rest

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestRESTClientCommonConfigs(t *testing.T) {
	_, err := RESTClientFromConfig(&Config{Host: "127.0.0.1"})
	require.NotNil(t, err)

	_, err = RESTClientFromConfig(&Config{Host: "127.0.0.1", Version: "v1"})
	require.NotNil(t, err)

	_, err = RESTClientFromConfig(&Config{Host: "127.0.0.1", APIPath: "/api"})
	require.NotNil(t, err)

	_, err = RESTClientFromConfig(&Config{Host: "http://127.0.0.1:2379", APIPath: "/api", Version: "v1"})
	require.Nil(t, err)

	_, err = RESTClientFromConfig(&Config{Host: "127.0.0.1:2379", APIPath: "/api", Version: "v2"})
	require.Nil(t, err)
}

func checkTLS(config *Config) bool {
	baseURL, _, err := defaultServerUrlFromConfig(config)
	if err != nil {
		return false
	}
	return baseURL.Scheme == "https"
}

func TestRESTClientUsingTLS(t *testing.T) {
	testCases := []struct {
		Config   *Config
		UsingTLS bool
	}{
		{
			Config:   &Config{},
			UsingTLS: false,
		},
		{
			Config: &Config{
				Host: "https://127.0.0.1",
			},
			UsingTLS: true,
		},
		{
			Config: &Config{
				Host: "127.0.0.1",
				Credential: &security.Credential{
					CAPath:   "foo",
					CertPath: "bar",
					KeyPath:  "test",
				},
			},
			UsingTLS: true,
		},
		{
			Config: &Config{
				Host: "///:://127.0.0.1",
				Credential: &security.Credential{
					CAPath:   "foo",
					CertPath: "bar",
					KeyPath:  "test",
				},
			},
			UsingTLS: false,
		},
	}

	for _, tc := range testCases {
		usingTLS := checkTLS(tc.Config)
		require.Equal(t, usingTLS, tc.UsingTLS)
	}
}
