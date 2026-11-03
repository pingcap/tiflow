// Copyright 2026 PingCAP, Inc.
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

package config

import (
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestServerConfigClientAuthorization(t *testing.T) {
	t.Parallel()
	cfg := GetDefaultServerConfig()
	_, err := toml.Decode(`
[security]
client-user-required = true
client-allowed-user = ["reader", "admin"]
[[security.client-authorization.bindings]]
user = "reader"
role = "viewer"
namespace = "team-a"
changefeed = "orders"
[[security.client-authorization.bindings]]
user = "admin"
role = "admin"
`, cfg)
	require.NoError(t, err)
	require.NoError(t, cfg.ValidateAndAdjust())
	clone := cfg.Clone()
	require.Equal(t, cfg, clone)
	clone.Security.ClientAuthorization.Bindings[0].Namespace = "team-b"
	require.Equal(t, "team-a", cfg.Security.ClientAuthorization.Bindings[0].Namespace)

	for _, tc := range []struct {
		name   string
		mutate func(*security.Credential)
	}{
		{"no authentication", func(c *security.Credential) { c.ClientUserRequired = false }},
		{"empty bindings", func(c *security.Credential) { c.ClientAuthorization.Bindings = nil }},
		{"unknown user", func(c *security.Credential) { c.ClientAuthorization.Bindings[0].User = "unknown" }},
		{"empty user", func(c *security.Credential) { c.ClientAuthorization.Bindings[0].User = "" }},
		{"unknown role", func(c *security.Credential) { c.ClientAuthorization.Bindings[0].Role = "root" }},
		{"missing namespace", func(c *security.Credential) { c.ClientAuthorization.Bindings[0].Namespace = "" }},
		{"missing changefeed", func(c *security.Credential) { c.ClientAuthorization.Bindings[0].Changefeed = "" }},
		{"scoped admin", func(c *security.Credential) { c.ClientAuthorization.Bindings[1].Namespace = "team-a" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := cfg.Clone()
			tc.mutate(invalid.Security)
			require.ErrorContains(t, invalid.ValidateAndAdjust(), "client-authorization")
		})
	}
}
