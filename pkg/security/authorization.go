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

package security

import "fmt"

const (
	// RoleViewer allows reading the bound changefeeds.
	RoleViewer = "viewer"
	// RoleOperator also allows modifying the bound changefeeds.
	RoleOperator = "operator"
	// RoleAdmin allows cluster administration, including unsafe operations.
	RoleAdmin = "admin"
	// RoleMonitor allows reading cluster-wide metrics without management access.
	RoleMonitor = "monitor"
)

// ClientAuthorization enables explicit, deny-by-default API authorization.
// A nil policy preserves the legacy administrator allow-list behavior.
type ClientAuthorization struct {
	Bindings []ClientRoleBinding `toml:"bindings" json:"bindings"`
}

// ClientRoleBinding grants a role to an authenticated default-upstream user.
// Scoped roles require an explicit namespace and changefeed; "*" matches all.
// Admin and monitor bindings must omit scope fields because they are cluster-wide.
type ClientRoleBinding struct {
	User       string `toml:"user" json:"user"`
	Role       string `toml:"role" json:"role"`
	Namespace  string `toml:"namespace" json:"namespace"`
	Changefeed string `toml:"changefeed" json:"changefeed"`
}

// ValidateClientAuthorization rejects policies that could silently lose scope
// or run without an authenticated application identity.
func (s *Credential) ValidateClientAuthorization() error {
	if s.ClientAuthorization == nil {
		return nil
	}
	if !s.ClientUserRequired {
		return fmt.Errorf("client-authorization requires client-user-required = true")
	}
	if len(s.ClientAuthorization.Bindings) == 0 {
		return fmt.Errorf("client-authorization bindings must not be empty")
	}
	for _, binding := range s.ClientAuthorization.Bindings {
		allowed := false
		for _, user := range s.ClientAllowedUser {
			if user != "" && binding.User == user {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("client-authorization user %q must appear in client-allowed-user", binding.User)
		}
		switch binding.Role {
		case RoleAdmin, RoleMonitor:
			if binding.Namespace != "" || binding.Changefeed != "" {
				return fmt.Errorf("client-authorization %s binding must not specify a resource scope", binding.Role)
			}
		case RoleViewer, RoleOperator:
			if binding.Namespace == "" || binding.Changefeed == "" {
				return fmt.Errorf("client-authorization %s binding requires namespace and changefeed (use * explicitly for all)", binding.Role)
			}
		default:
			return fmt.Errorf("unknown client-authorization role %q", binding.Role)
		}
	}
	return nil
}
