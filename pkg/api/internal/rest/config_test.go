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

package rest

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestCDCRESTClientCommonConfigs(t *testing.T) {
	_, err := CDCRESTClientFromConfig(&Config{Host: "127.0.0.1"})
	require.NotNil(t, err)

	_, err = CDCRESTClientFromConfig(&Config{Host: "127.0.0.1", Version: "v1"})
	require.NotNil(t, err)

	_, err = CDCRESTClientFromConfig(&Config{Host: "127.0.0.1", APIPath: "/api"})
	require.NotNil(t, err)

	_, err = CDCRESTClientFromConfig(&Config{Host: "http://127.0.0.1:2379", APIPath: "/api", Version: "v1"})
	require.Nil(t, err)

	_, err = CDCRESTClientFromConfig(&Config{Host: "127.0.0.1:2379", APIPath: "/api", Version: "v2"})
	require.Nil(t, err)
}

func checkTLS(config *Config) bool {
	baseURL, _, err := defaultServerURLFromConfig(config)
	if err != nil {
		return false
	}
	return baseURL.Scheme == "https"
}

func TestCDCRESTClientUsingTLS(t *testing.T) {
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
