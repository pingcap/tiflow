// Copyright 2022 PingCAP, Inc.
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

package migrate

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 1. create an etcd server
// 2. put some old metadata to etcd cluster
// 3. use 3 goroutine to mock cdc nodes, one is owner, which will migrate data,
// the other two are non-owner nodes, which will wait for migrating done
// 3. migrate the data to new meta version
// 4. check the data is migrated correctly
func TestMigration(t *testing.T) {
	s := &etcd.Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)
	curl := s.ClientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()
	info1 := model.ChangeFeedInfo{
		SinkURI: "test1",
		StartTs: 1, TargetTs: 100, State: model.StateNormal,
	}
	status1 := model.ChangeFeedStatus{ResolvedTs: 2, CheckpointTs: 1}
	info2 := model.ChangeFeedInfo{
		SinkURI: "test1",
		StartTs: 2, TargetTs: 200, State: model.StateError,
	}
	status2 := model.ChangeFeedStatus{ResolvedTs: 3, CheckpointTs: 2}
	info3 := model.ChangeFeedInfo{
		SinkURI: "test1",
		StartTs: 3, TargetTs: 300, State: model.StateFailed,
	}
	status3 := model.ChangeFeedStatus{ResolvedTs: 4, CheckpointTs: 3}

	testCases := []struct {
		id     string
		info   model.ChangeFeedInfo
		status model.ChangeFeedStatus
	}{
		{"test1", info1, status1},
		{"test2", info2, status2},
		{"test3", info3, status3},
	}
	const oldInfoKeyBase = "/tidb/cdc/changefeed/info/%s"
	const oldStatusKeyBase = "/tidb/cdc/job/%s"

	// 1.put old version meta data to etcd
	for _, tc := range testCases {
		iv, err := tc.info.Marshal()
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), fmt.Sprintf(oldInfoKeyBase, tc.id), iv)
		require.NoError(t, err)
		sv, err := tc.status.Marshal()
		require.NoError(t, err)
		_, err = cli.Put(context.Background(), fmt.Sprintf(oldStatusKeyBase, tc.id), sv)
		require.NoError(t, err)
	}
	// 2. check old version data in etcd is expected
	for _, tc := range testCases {
		infoResp, err := cli.Get(context.Background(),
			fmt.Sprintf(oldInfoKeyBase, tc.id))
		require.NoError(t, err)
		info := model.ChangeFeedInfo{}
		err = info.Unmarshal(infoResp.Kvs[0].Value)
		require.NoError(t, err)
		require.Equal(t, tc.info, info)
		statusResp, err := cli.Get(context.Background(),
			fmt.Sprintf(oldStatusKeyBase, tc.id))
		require.NoError(t, err)
		status := model.ChangeFeedStatus{}
		err = status.Unmarshal(statusResp.Kvs[0].Value)
		require.NoError(t, err)
		require.Equal(t, tc.status, status)
	}

	// set timeout to make sure this test will finished
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cdcCli, err := etcd.NewCDCEtcdClient(ctx, cli, "default")
	require.Nil(t, err)

	m := NewMigrator(&cdcCli, []string{}, config.GetGlobalServerConfig())
	migrator := m.(*migrator)
	migrator.migrateGcServiceSafePointFunc = func(ctx context.Context,
		pdClient pd.Client, config *security.Credential,
		gcServiceID string, ttl int64,
	) error {
		return nil
	}
	migrator.createPDClientFunc = func(ctx context.Context,
		pdEndpoints []string, conf *security.Credential,
	) (pd.Client, error) {
		mock := gc.MockPDClient{
			ClusterID: 1,
			UpdateServiceGCSafePointFunc: func(ctx context.Context,
				serviceID string, ttl int64,
				safePoint uint64,
			) (uint64, error) {
				return 1, nil
			},
		}
		return &mock, nil
	}

	// 3. tow non-owner node wait for meta migrating done
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := migrator.WaitMetaVersionMatched(ctx)
		require.NoError(t, err)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := migrator.WaitMetaVersionMatched(ctx)
		require.NoError(t, err)
	}()

	wg.Add(1)
	// 4.owner note migrates meta data
	go func() {
		defer wg.Done()
		// 5. test ShouldMigrate works as expected
		should, err := migrator.ShouldMigrate(ctx)
		require.NoError(t, err)
		if should {
			// migrate
			err = migrator.Migrate(ctx)
			require.NoError(t, err)
		}
	}()

	// 6. wait for migration done
	wg.Wait()

	// 7. check new version data in etcd is expected
	for _, tc := range testCases {
		infoResp, err := cli.Get(context.Background(),
			fmt.Sprintf("%s%s/%s", etcd.DefaultClusterAndNamespacePrefix,
				etcd.ChangefeedInfoKey, tc.id))
		require.NoError(t, err)
		info := model.ChangeFeedInfo{}
		err = info.Unmarshal(infoResp.Kvs[0].Value)
		require.NoError(t, err)
		require.Equal(t, uint64(1), info.UpstreamID)
		tc.info.UpstreamID = info.UpstreamID
		require.Equal(t, model.DefaultNamespace, info.Namespace)
		tc.info.Namespace = info.Namespace
		require.Equal(t, tc.info, info)
		statusResp, err := cli.Get(context.Background(),
			fmt.Sprintf("%s%s/%s", etcd.DefaultClusterAndNamespacePrefix,
				etcd.ChangefeedStatusKey, tc.id))
		require.NoError(t, err)
		status := model.ChangeFeedStatus{}
		err = status.Unmarshal(statusResp.Kvs[0].Value)
		require.NoError(t, err)
		require.Equal(t, tc.status, status)
	}
}
