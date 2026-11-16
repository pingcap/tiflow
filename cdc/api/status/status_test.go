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
	"bytes"
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	mock_etcd "github.com/pingcap/tiflow/pkg/etcd/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestWriteEtcdInfoRedactsCredentials(t *testing.T) {
	client := mock_etcd.NewMockCDCEtcdClient(gomock.NewController(t))
	client.EXPECT().GetAllCDCInfo(gomock.Any()).Return([]*mvccpb.KeyValue{{
		Key:   []byte("/tidb/cdc/default/default/changefeed/info/test"),
		Value: []byte(`{"sink-uri":"mysql://user:downstream-password-sentinel@localhost:3306/db","config":{"memory-quota":123}}`),
	}}, nil)
	var output bytes.Buffer
	(&statusAPI{}).writeEtcdInfo(context.Background(), client, &output)
	require.Contains(t, output.String(), "/tidb/cdc/default/default/changefeed/info/test")
	require.Contains(t, output.String(), `"memory-quota":123`)
	require.Contains(t, output.String(), "mysql://user:xxxxx@localhost:3306/db")
	require.NotContains(t, output.String(), "downstream-password-sentinel")
}
