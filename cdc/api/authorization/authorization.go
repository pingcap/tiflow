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

// Package authorization implements TiCDC API role and resource authorization.
package authorization

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
)

// Action is an API permission, independent of HTTP method or API version.
type Action string

const (
	Read   Action = "changefeed.read"
	Write  Action = "changefeed.write"
	List   Action = "changefeed.list"
	Create Action = "changefeed.create"
	// InspectUpstream permits the read-only helpers used by CLI create/resume.
	InspectUpstream Action = "upstream.inspect"
	Admin           Action = "cluster.manage"
	Unsafe          Action = "cluster.unsafe"
)

// Principal identifies a user authenticated against the default upstream.
// UpstreamID records the identity domain, not the requested resource upstream.
type Principal struct {
	Username   string
	UpstreamID uint64
}

type principalKey struct{}

// WithPrincipal attaches a verified identity, including for net/http handlers.
func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// GetPrincipal returns the verified identity, never a user-supplied header.
func GetPrincipal(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalKey{}).(Principal)
	return principal, ok && principal.Username != ""
}

// Can checks the configured policy. Legacy deployments retain allow-list
// administrator semantics; the authentication middleware still gates access.
// List only permits entering a collection handler, which must filter every item.
// Create only permits parsing a create request, which must then check Write
// against the namespace and changefeed from the parsed body before any effects.
func Can(ctx context.Context, action Action, resource model.ChangeFeedID) bool {
	credential := config.GetGlobalServerConfig().Security
	if credential == nil || credential.ClientAuthorization == nil {
		return true
	}
	principal, ok := GetPrincipal(ctx)
	if !ok || !credential.ClientUserRequired {
		return false
	}
	for _, binding := range credential.ClientAuthorization.Bindings {
		if binding.User != principal.Username {
			continue
		}
		switch action {
		case Read, Write, List, Create, InspectUpstream, Admin, Unsafe:
		default:
			return false
		}
		if binding.Role == security.RoleAdmin && binding.Namespace == "" && binding.Changefeed == "" {
			return true
		}
		if binding.Namespace == "" || binding.Changefeed == "" {
			continue
		}
		switch action {
		case Read, List:
			if binding.Role != security.RoleViewer && binding.Role != security.RoleOperator {
				continue
			}
		case Write, Create, InspectUpstream:
			if binding.Role != security.RoleOperator {
				continue
			}
		default:
			continue
		}
		if action == List || action == Create || action == InspectUpstream {
			return true
		}
		if (binding.Namespace == "*" || binding.Namespace == resource.Namespace) &&
			(binding.Changefeed == "*" || binding.Changefeed == resource.ID) {
			return true
		}
	}
	return false
}
