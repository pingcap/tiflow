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

package status

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	mock_capture "github.com/pingcap/tiflow/cdc/capture/mock"
	"github.com/pingcap/tiflow/pkg/config"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestDebugInfoRedactsEtcdCredentials(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() { config.StoreGlobalServerConfig(original) })
	config.StoreGlobalServerConfig(config.GetDefaultServerConfig())
	ctrl := gomock.NewController(t)
	cp := mock_capture.NewMockCapture(ctrl)
	cp.EXPECT().WriteDebugInfo(gomock.Any(), gomock.Any())
	etcdClient := mock_etcd.NewMockCDCEtcdClient(ctrl)
	cp.EXPECT().GetEtcdClient().Return(etcdClient)
	etcdClient.EXPECT().GetAllCDCInfo(gomock.Any()).Return([]*mvccpb.KeyValue{{
		Key:   []byte("/tidb/cdc/default/team-a/changefeed/info/orders"),
		Value: []byte(`{"sink-uri":"mysql://user:debug-secret@host/db?token=token-secret"}`),
	}}, nil)
	router := gin.New()
	RegisterStatusAPIRoutes(router, cp)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/info", nil))
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "changefeed/info/orders")
	require.NotContains(t, w.Body.String(), "debug-secret")
	require.NotContains(t, w.Body.String(), "token-secret")
}
