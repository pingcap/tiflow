// Copyright 2023 PingCAP, Inc.
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

package kafka

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestNewTokenProvider(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		config      *Config
		expectedErr string
	}{
		{
			name: "valid",
			config: &Config{
				SASL: &security.SASL{
					OAuth2: security.OAuth2{
						ClientID:     "client-id",
						ClientSecret: "client-secret",
						TokenURL:     "http://localhost:8080/oauth2/token",
						Scopes:       []string{"scope1", "scope2"},
						GrantType:    "client_credentials",
					},
				},
			},
		},
		{
			name: "invalid token URL",
			config: &Config{
				SASL: &security.SASL{
					OAuth2: security.OAuth2{
						ClientID:     "client-id",
						ClientSecret: "client-secret",
						TokenURL:     "http://test.com/Segment%%2815197306101420000%29",
						Scopes:       []string{"scope1", "scope2"},
						GrantType:    "client_credentials",
					},
				},
			},
			expectedErr: "invalid URL escape",
		},
	} {
		ts := test
		t.Run(ts.name, func(t *testing.T) {
			t.Parallel()
			_, err := newTokenProvider(context.TODO(), ts.config)
			if ts.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), ts.expectedErr)
			}
		})
	}
}
