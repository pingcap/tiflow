// Copyright 2020 PingCAP, Inc.
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

package etcd

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Captures []*model.CaptureInfo

func (c Captures) Len() int           { return len(c) }
func (c Captures) Less(i, j int) bool { return c[i].ID < c[j].ID }
func (c Captures) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func TestEmbedEtcd(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)
	curl := s.ClientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer cli.Close()

	var (
		key = "test-key"
		val = "test-val"
	)
	_, err = cli.Put(context.Background(), key, val)
	require.NoError(t, err)
	resp, err2 := cli.Get(context.Background(), key)
	require.NoError(t, err2)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, resp.Kvs[0].Value, []byte(val))
}

func TestGetChangeFeeds(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)
	testCases := []struct {
		ids     []string
		details []string
	}{
		{ids: nil, details: nil},
		{ids: []string{"id"}, details: []string{"detail"}},
		{ids: []string{"id", "id1", "id2"}, details: []string{"detail", "detail1", "detail2"}},
	}
	for _, tc := range testCases {
		for i := 0; i < len(tc.ids); i++ {
			_, err := s.client.GetEtcdClient().Put(context.Background(),
				GetEtcdKeyChangeFeedInfo(DefaultCDCClusterID,
					model.DefaultChangeFeedID(tc.ids[i])),
				tc.details[i])
			require.NoError(t, err)
		}
		_, result, err := s.client.GetChangeFeeds(context.Background())
		require.NoError(t, err)
		require.NoError(t, err)
		require.Equal(t, len(result), len(tc.ids))
		for i := 0; i < len(tc.ids); i++ {
			rawKv, ok := result[model.DefaultChangeFeedID(tc.ids[i])]
			require.True(t, ok)
			require.Equal(t, string(rawKv.Value), tc.details[i])
		}
	}
	_, result, err := s.client.GetChangeFeeds(context.Background())
	require.NoError(t, err)
	require.Equal(t, len(result), 3)

	err = s.client.ClearAllCDCInfo(context.Background())
	require.NoError(t, err)

	_, result, err = s.client.GetChangeFeeds(context.Background())
	require.NoError(t, err)
	require.Equal(t, len(result), 0)
}

func TestOpChangeFeedDetail(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)
	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
		SortDir: "/old-version/sorter",
	}
	cfID := model.DefaultChangeFeedID("test-op-cf")

	err := s.client.SaveChangeFeedInfo(ctx, detail, cfID)
	require.NoError(t, err)

	d, err := s.client.GetChangeFeedInfo(ctx, cfID)
	require.NoError(t, err)
	require.Equal(t, d.SinkURI, detail.SinkURI)
	require.Equal(t, d.SortDir, detail.SortDir)

	err = s.client.DeleteChangeFeedInfo(ctx, cfID)
	require.NoError(t, err)

	_, err = s.client.GetChangeFeedInfo(ctx, cfID)
	require.True(t, cerror.ErrChangeFeedNotExists.Equal(err))
}

func TestGetAllChangeFeedInfo(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)
	ctx := context.Background()
	infos := []struct {
		id   string
		info *model.ChangeFeedInfo
	}{
		{
			id: "a",
			info: &model.ChangeFeedInfo{
				SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
				SortDir: "/old-version/sorter",
			},
		},
		{
			id: "b",
			info: &model.ChangeFeedInfo{
				SinkURI: "root@tcp(127.0.0.1:4000)/mysql",
			},
		},
	}

	for _, item := range infos {
		err := s.client.SaveChangeFeedInfo(ctx,
			item.info,
			model.DefaultChangeFeedID(item.id))
		require.NoError(t, err)
	}

	allChangFeedInfo, err := s.client.GetAllChangeFeedInfo(ctx)
	require.NoError(t, err)

	for _, item := range infos {
		obtained, found := allChangFeedInfo[model.DefaultChangeFeedID(item.id)]
		require.True(t, found)
		require.Equal(t, item.info.SinkURI, obtained.SinkURI)
		require.Equal(t, item.info.SortDir, obtained.SortDir)
	}
}

func TestCheckMultipleCDCClusterExist(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)

	ctx := context.Background()
	rawEtcdClient := s.client.GetEtcdClient().cli
	defaultClusterKey := DefaultClusterAndNamespacePrefix + "/test-key"
	_, err := rawEtcdClient.Put(ctx, defaultClusterKey, "test-value")
	require.NoError(t, err)

	err = s.client.CheckMultipleCDCClusterExist(ctx)
	require.NoError(t, err)

	for _, reserved := range config.ReservedClusterIDs {
		newClusterKey := "/tidb/cdc/" + reserved
		_, err = rawEtcdClient.Put(ctx, newClusterKey, "test-value")
		require.NoError(t, err)
		err = s.client.CheckMultipleCDCClusterExist(ctx)
		require.NoError(t, err)
	}

	newClusterKey := NamespacedPrefix("new-cluster", "new-namespace") +
		"/test-key"
	_, err = rawEtcdClient.Put(ctx, newClusterKey, "test-value")
	require.NoError(t, err)

	err = s.client.CheckMultipleCDCClusterExist(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ErrMultipleCDCClustersExist")
}

func TestCreateChangefeed(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)

	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		UpstreamID: 1,
		Namespace:  "test",
		ID:         "create-changefeed",
		SinkURI:    "root@tcp(127.0.0.1:3306)/mysql",
	}

	upstreamInfo := &model.UpstreamInfo{ID: 1}
	err := s.client.CreateChangefeedInfo(ctx, upstreamInfo, detail)
	require.NoError(t, err)

	err = s.client.CreateChangefeedInfo(ctx,
		upstreamInfo, detail)
	require.True(t, cerror.ErrMetaOpFailed.Equal(err))
	require.Equal(t, "[DFLOW:ErrMetaOpFailed]unexpected meta operation failure: Create changefeed test/create-changefeed", err.Error())
}

func TestUpdateChangefeedAndUpstream(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)

	ctx := context.Background()
	upstreamInfo := &model.UpstreamInfo{
		ID:          1,
		PDEndpoints: "http://127.0.0.1:2385",
	}
	changeFeedID := model.DefaultChangeFeedID("test-update-cf-and-up")
	changeFeedInfo := &model.ChangeFeedInfo{
		UpstreamID: upstreamInfo.ID,
		ID:         changeFeedID.ID,
		Namespace:  changeFeedID.Namespace,
		SinkURI:    "blackhole://",
	}

	err := s.client.SaveChangeFeedInfo(ctx, changeFeedInfo, changeFeedID)
	require.NoError(t, err)

	err = s.client.UpdateChangefeedAndUpstream(ctx, upstreamInfo, changeFeedInfo)
	require.NoError(t, err)

	var upstreamResult *model.UpstreamInfo
	var changefeedResult *model.ChangeFeedInfo

	upstreamResult, err = s.client.GetUpstreamInfo(ctx, 1, changeFeedID.Namespace)
	require.NoError(t, err)
	require.Equal(t, upstreamInfo.PDEndpoints, upstreamResult.PDEndpoints)

	changefeedResult, err = s.client.GetChangeFeedInfo(ctx, changeFeedID)
	require.NoError(t, err)
	require.Equal(t, changeFeedInfo.SinkURI, changefeedResult.SinkURI)
}

func TestGetAllCaptureLeases(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testCases := []*model.CaptureInfo{
		{
			ID:            "a3f41a6a-3c31-44f4-aa27-344c1b8cd658",
			AdvertiseAddr: "127.0.0.1:8301",
		},
		{
			ID:            "cdb041d9-ccdd-480d-9975-e97d7adb1185",
			AdvertiseAddr: "127.0.0.1:8302",
		},
		{
			ID:            "e05e5d34-96ea-44af-812d-ca72aa19e1e5",
			AdvertiseAddr: "127.0.0.1:8303",
		},
	}
	leases := make(map[string]int64)

	for _, cinfo := range testCases {
		sess, err := concurrency.NewSession(s.client.GetEtcdClient().Unwrap(),
			concurrency.WithTTL(10), concurrency.WithContext(ctx))
		require.NoError(t, err)
		err = s.client.PutCaptureInfo(ctx, cinfo, sess.Lease())
		require.NoError(t, err)
		leases[cinfo.ID] = int64(sess.Lease())
	}

	_, captures, err := s.client.GetCaptures(ctx)
	require.NoError(t, err)
	require.Len(t, captures, len(testCases))
	sort.Sort(Captures(captures))
	require.Equal(t, captures, testCases)

	queryLeases, err := s.client.GetCaptureLeases(ctx)
	require.NoError(t, err)
	require.Equal(t, queryLeases, leases)

	// make sure the RevokeAllLeases function can ignore the lease not exist
	leases["/fake/capture/info"] = 200
	err = s.client.RevokeAllLeases(ctx, leases)
	require.NoError(t, err)
	queryLeases, err = s.client.GetCaptureLeases(ctx)
	require.NoError(t, err)
	require.Equal(t, queryLeases, map[string]int64{})
}

const (
	testOwnerRevisionForMaxEpochs = 16
)

func TestGetOwnerRevision(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First we check that GetOwnerRevision correctly reports errors
	// Note that there is no owner for now.
	_, err := s.client.GetOwnerRevision(ctx, "fake-capture-id")
	require.Contains(t, err.Error(), "ErrOwnerNotFound")

	var (
		ownerRev int64
		epoch    int32
		wg       sync.WaitGroup
	)

	// We will create 3 mock captures, and they will become the owner one by one.
	// While each is the owner, it tries to get its owner revision, and
	// checks that the global monotonicity is guaranteed.

	wg.Add(3)
	for i := 0; i < 3; i++ {
		i := i
		go func() {
			defer wg.Done()
			sess, err := concurrency.NewSession(s.client.GetEtcdClient().Unwrap(),
				concurrency.WithTTL(10 /* seconds */))
			require.Nil(t, err)
			election := concurrency.NewElection(sess,
				CaptureOwnerKey(DefaultCDCClusterID))

			mockCaptureID := fmt.Sprintf("capture-%d", i)

			for {
				err = election.Campaign(ctx, mockCaptureID)
				if err != nil {
					require.Contains(t, err.Error(), "context canceled")
					return
				}

				rev, err := s.client.GetOwnerRevision(ctx, mockCaptureID)
				require.NoError(t, err)

				_, err = s.client.GetOwnerRevision(ctx, "fake-capture-id")
				require.Contains(t, err.Error(), "ErrNotOwner")

				lastRev := atomic.SwapInt64(&ownerRev, rev)
				require.Less(t, lastRev, rev)

				err = election.Resign(ctx)
				if err != nil {
					require.Contains(t, err.Error(), "context canceled")
					return
				}

				if atomic.AddInt32(&epoch, 1) >= testOwnerRevisionForMaxEpochs {
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestExtractKeySuffix(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		input  string
		expect string
		hasErr bool
	}{
		{"/tidb/cdc/capture/info/6a6c6dd290bc8732", "6a6c6dd290bc8732", false},
		{"/tidb/cdc/capture/info/6a6c6dd290bc8732/", "", false},
		{"/tidb/cdc", "cdc", false},
		{"/tidb", "tidb", false},
		{"", "", true},
	}
	for _, tc := range testCases {
		key, err := extractKeySuffix(tc.input)
		if tc.hasErr {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
			require.Equal(t, tc.expect, key)
		}
	}
}

func TestMigrateBackupKey(t *testing.T) {
	t.Parallel()

	key := MigrateBackupKey(1, "/tidb/cdc/capture/abcd")
	require.Equal(t, "/tidb/cdc/__backup__/1/tidb/cdc/capture/abcd", key)
	key = MigrateBackupKey(1, "abcdc")
	require.Equal(t, "/tidb/cdc/__backup__/1/abcdc", key)
}

func TestDeleteCaptureInfo(t *testing.T) {
	t.Parallel()

	s := &Tester{}
	s.SetUpTest(t)
	defer s.TearDownTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	captureID := "test-capture-id"

	changefeedStatus := map[model.ChangeFeedID]model.ChangeFeedStatus{
		model.DefaultChangeFeedID("test-cf-1"): {CheckpointTs: 1},
	}

	for id, status := range changefeedStatus {
		val, err := status.Marshal()
		require.NoError(t, err)
		statusKey := fmt.Sprintf("%s/%s", ChangefeedStatusKeyPrefix(DefaultCDCClusterID, id.Namespace), id.ID)
		_, err = s.client.Client.Put(ctx, statusKey, val)
		require.NoError(t, err)

		_, err = s.client.Client.Put(
			ctx, GetEtcdKeyTaskPosition(DefaultCDCClusterID, id, captureID),
			fmt.Sprintf("task-%s", id.ID))
		require.NoError(t, err)
	}
	err := s.client.DeleteCaptureInfo(ctx, captureID)
	require.NoError(t, err)
	for id := range changefeedStatus {
		taskPositionKey := GetEtcdKeyTaskPosition(DefaultCDCClusterID, id, captureID)
		v, err := s.client.Client.Get(ctx, taskPositionKey)
		require.NoError(t, err)
		require.Equal(t, 0, len(v.Kvs))
	}
}
