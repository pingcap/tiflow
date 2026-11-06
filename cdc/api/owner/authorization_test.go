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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package owner

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/api/authorization"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestLegacyOwnerAccess(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	for _, scoped := range []bool{false, true} {
		t.Run(fmt.Sprintf("role-bindings=%t", scoped), func(t *testing.T) {
			cfg := config.GetDefaultServerConfig()
			cfg.Security.ClientUserRequired = true
			cfg.Security.ClientAllowedUser = []string{"reader", "operator", "admin"}
			if scoped {
				cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
					{User: "reader", Role: security.RoleViewer, Namespace: "default", Changefeed: "orders"},
					{User: "operator", Role: security.RoleOperator, Namespace: "default", Changefeed: "orders"},
					{User: "admin", Role: security.RoleAdmin},
				}}
			}
			config.StoreGlobalServerConfig(cfg)
			cp := mock_capture.NewMockCapture(gomock.NewController(t))
			// An unknown account must fail before contacting TiDB or the owner.
			cp.EXPECT().GetUpstreamManager().Return(upstream.NewManager4Test(nil), nil).Times(5)
			router := gin.New()
			// Register only this group: the global router must not be necessary
			// to protect the legacy endpoints.
			RegisterOwnerAPIRoutes(router, cp)
			admitted := gin.New()
			RegisterOwnerAPIRoutes(admitted, nil)
			for _, path := range []string{"/admin", "/resign", "/rebalance_trigger", "/move_table", "/changefeed/query"} {
				t.Run(path, func(t *testing.T) {
					for _, credential := range []string{"", "Bearer untrusted", "unknown-user"} {
						req := legacyOwnerRequest(path, url.Values{api.OpVarAdminJob: {"3"}, api.OpVarChangefeedID: {"orders"}})
						if credential == "unknown-user" {
							req.SetBasicAuth(credential, "password-must-not-be-logged")
						} else {
							req.Header.Set("Authorization", credential)
						}
						req.Header.Set("X-TiCDC-Role", "admin")
						req.Header.Set("TiCDC-ForwardFromCapture", "trusted-capture")
						w := httptest.NewRecorder()
						router.ServeHTTP(w, req)
						require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
					}
					for _, user := range []string{"reader", "operator", "admin"} {
						req := legacyOwnerRequest(path, nil)
						req = req.WithContext(authorization.WithPrincipal(req.Context(), authorization.Principal{
							Username: user, UpstreamID: 42,
						}))
						w := httptest.NewRecorder()
						admitted.ServeHTTP(w, req)
						if scoped && user != "admin" {
							require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
						} else {
							// The nil-capture handler returns this only after access
							// control admits the request.
							require.Equal(t, http.StatusBadRequest, w.Code)
							require.Equal(t, concurrency.ErrElectionNotLeader.Error(), w.Body.String())
						}
					}
				})
			}
		})
	}
}

func TestLegacyAdminJobValidationAndAudit(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"admin"}
	cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
		{User: "admin", Role: security.RoleAdmin},
	}}
	config.StoreGlobalServerConfig(cfg)
	core, logs := observer.New(zap.InfoLevel)
	t.Cleanup(log.ReplaceGlobals(zap.New(core), nil))
	for _, tc := range []struct {
		job, id string
		status  int
	}{
		{"1", "orders", http.StatusOK},
		{"2", "orders", http.StatusOK},
		{"3", "orders", http.StatusOK},
		{"", "orders", http.StatusBadRequest},
		{"invalid", "orders", http.StatusBadRequest},
		{"-1", "orders", http.StatusBadRequest},
		{"0", "orders", http.StatusBadRequest},
		{"4", "orders", http.StatusBadRequest},
		{"999", "orders", http.StatusBadRequest},
		{"18446744073709551619", "orders", http.StatusBadRequest},
		{"3", "", http.StatusBadRequest},
		{"3", "invalid/id", http.StatusBadRequest},
	} {
		t.Run(tc.job+"/"+tc.id, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cp := mock_capture.NewMockCapture(ctrl)
			if tc.status == http.StatusOK {
				o := mock_owner.NewMockOwner(ctrl)
				cp.EXPECT().GetOwner().Return(o, nil)
				o.EXPECT().EnqueueJob(gomock.Any(), gomock.Any()).Do(func(job model.AdminJob, done chan<- error) {
					require.Equal(t, model.DefaultChangeFeedID(tc.id), job.CfID)
					require.Equal(t, tc.job, fmt.Sprint(int(job.Type)))
					done <- nil
				})
			}
			router := gin.New()
			RegisterOwnerAPIRoutes(router, cp)
			req := legacyOwnerRequest("/admin", url.Values{
				api.OpVarAdminJob: {tc.job}, api.OpVarChangefeedID: {tc.id}, "password": {"body-secret"},
			})
			// A claimed username must not replace the authenticated actor.
			req.SetBasicAuth("forged-actor", "header-secret")
			req = req.WithContext(authorization.WithPrincipal(req.Context(), authorization.Principal{
				Username: "admin", UpstreamID: 42,
			}))
			logs.TakeAll()
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			entries := logs.FilterMessage("cdc open api request").All()
			require.Len(t, entries, 1)
			fields := entries[0].ContextMap()
			require.Equal(t, "admin", fields["username"])
			require.Equal(t, true, fields["authenticated"])
			require.Equal(t, uint64(42), fields["upstreamID"])
			require.Equal(t, "/capture/owner/admin", fields["path"])
			require.Equal(t, int64(tc.status), fields["status"])
			require.Equal(t, tc.id, fields["changefeed"])
			require.Equal(t, tc.job, fields["adminJobType"])
			require.NotContains(t, fmt.Sprint(entries), "header-secret")
			require.NotContains(t, fmt.Sprint(entries), "body-secret")
		})
	}
}

func TestLegacySchedulingPreconditions(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	cfg := config.GetDefaultServerConfig()
	cfg.Security.ClientUserRequired = true
	cfg.Security.ClientAllowedUser = []string{"admin"}
	cfg.Security.ClientAuthorization = &security.ClientAuthorization{Bindings: []security.ClientRoleBinding{
		{User: "admin", Role: security.RoleAdmin},
	}}
	config.StoreGlobalServerConfig(cfg)
	id := model.DefaultChangeFeedID("orders")
	for _, path := range []string{"/rebalance_trigger", "/move_table"} {
		for _, tc := range []struct {
			name   string
			err    error
			status int
		}{
			{"existing", nil, http.StatusOK},
			{"missing", cerror.ErrChangeFeedNotExists.GenWithStackByArgs(id), http.StatusBadRequest},
			{"status unavailable", cerror.ErrPDEtcdAPIError.GenWithStackByArgs(), http.StatusInternalServerError},
			{"not owner", cerror.ErrNotOwner.GenWithStackByArgs(), http.StatusInternalServerError},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				cp := mock_capture.NewMockCapture(ctrl)
				if tc.name == "not owner" {
					cp.EXPECT().StatusProvider().Return(nil)
				} else {
					provider := mock_owner.NewMockStatusProvider(ctrl)
					cp.EXPECT().StatusProvider().Return(provider)
					provider.EXPECT().GetChangeFeedStatus(gomock.Any(), id).
						Return(&model.ChangeFeedStatusForAPI{CheckpointTs: 1}, tc.err)
				}
				// Missing or unavailable status must not enqueue a scheduling job.
				if tc.err == nil {
					o := mock_owner.NewMockOwner(ctrl)
					cp.EXPECT().GetOwner().Return(o, nil)
					if path == "/rebalance_trigger" {
						o.EXPECT().RebalanceTables(id, gomock.Any()).Do(func(_ model.ChangeFeedID, done chan<- error) {
							done <- nil
						})
					} else {
						o.EXPECT().ScheduleTable(id, "capture", int64(42), gomock.Any()).
							Do(func(_ model.ChangeFeedID, _ string, _ int64, done chan<- error) { done <- nil })
					}
				}
				router := gin.New()
				RegisterOwnerAPIRoutes(router, cp)
				req := legacyOwnerRequest(path, url.Values{
					api.OpVarChangefeedID: {id.ID}, api.OpVarTargetCaptureID: {"capture"}, api.OpVarTableID: {"42"},
				})
				req = req.WithContext(authorization.WithPrincipal(req.Context(), authorization.Principal{
					Username: "admin", UpstreamID: 42,
				}))
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				require.Equal(t, tc.status, w.Code, w.Body.String())
			})
		}
	}
}

func legacyOwnerRequest(path string, form url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/capture/owner"+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
}
