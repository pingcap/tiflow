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

package authorization

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestAuthorization(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"viewer", "operator", "admin", "monitor", "unbound"}
	cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
		{User: "viewer", Role: security.RoleViewer, Namespace: "team-a", Changefeed: "orders"},
		{User: "operator", Role: security.RoleOperator, Namespace: "team-b", Changefeed: "*"},
		{User: "admin", Role: security.RoleAdmin},
		{User: "monitor", Role: security.RoleMonitor},
	}}
	config.StoreGlobalServerConfig(cfg)
	for _, tc := range []struct {
		user, namespace, id string
		action              Action
		allowed             bool
	}{
		{"viewer", "team-a", "orders", Read, true},
		{"viewer", "team-a", "other", Read, false},
		{"viewer", "team-b", "orders", Read, false},
		{"viewer", "team-a", "orders", Write, false},
		{"viewer", "", "", List, true},
		{"viewer", "", "", Create, false},
		{"viewer", "", "", InspectUpstream, false},
		{"viewer", "", "", ReadTSO, true},
		{"viewer", "", "", Metrics, false},
		{"viewer", "", "", Admin, false},
		{"viewer", "", "", Unsafe, false},
		{"operator", "team-b", "orders", Write, true},
		{"operator", "team-b", "", Write, true},
		{"operator", "team-a", "orders", Write, false},
		{"operator", "", "", Create, true},
		{"operator", "", "", InspectUpstream, true},
		{"operator", "", "", Unsafe, false},
		{"admin", "team-a", "orders", Write, true},
		{"admin", "", "", Unsafe, true},
		{"admin", "", "", Metrics, true},
		{"monitor", "", "", Metrics, true},
		{"monitor", "", "", ReadTSO, false},
		{"monitor", "", "", InspectUpstream, false},
		{"monitor", "", "", List, false},
		{"monitor", "", "", Unsafe, false},
		{"monitor", "", "", Admin, false},
		{"admin", "", "", Action("unregistered"), false},
		{"unbound", "team-a", "orders", Read, false},
		{"unbound", "", "", List, false},
		{"", "team-a", "orders", Read, false},
	} {
		t.Run(tc.user+"/"+string(tc.action)+"/"+tc.namespace+"/"+tc.id, func(t *testing.T) {
			ctx := WithPrincipal(context.Background(), Principal{Username: tc.user, UpstreamID: 42})
			require.Equal(t, tc.allowed, Can(ctx, tc.action, model.ChangeFeedID{Namespace: tc.namespace, ID: tc.id}))
		})
	}
	require.False(t, Can(context.Background(), Read, model.DefaultChangeFeedID("orders")))
	cfg.Security.ClientUserRequired = false
	require.False(t, Can(WithPrincipal(context.Background(), Principal{Username: "admin"}), Unsafe, model.ChangeFeedID{}))
	cfg.Security.ClientAuthorization = nil
	require.True(t, Can(context.Background(), Read, model.DefaultChangeFeedID("orders")))
}
