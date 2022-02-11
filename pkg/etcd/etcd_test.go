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
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func Test(t *testing.T) { check.TestingT(t) }

type etcdSuite struct {
	etcd      *embed.Etcd
	clientURL *url.URL
}

var _ = check.Suite(&etcdSuite{})

// Set up a embeded etcd using free ports.
func (s *etcdSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	curl, e, err := SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	s.clientURL = curl
	s.etcd = e
}

func (s *etcdSuite) TearDownTest(c *check.C) {
	s.etcd.Close()
logEtcdError:
	for {
		select {
		case err := <-s.etcd.Err():
			c.Logf("etcd server error: %v", err)
		default:
			break logEtcdError
		}
	}
}

func (s *etcdSuite) TestEmbedEtcd(c *check.C) {
	defer testleak.AfterTest(c)()
	curl := s.clientURL.String()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{curl},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	defer cli.Close()

	var (
		key = "test-key"
		val = "test-val"
	)
	_, err = cli.Put(context.Background(), key, val)
	c.Assert(err, check.IsNil)
	resp, err2 := cli.Get(context.Background(), key)
<<<<<<< HEAD
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Kvs, check.HasLen, 1)
	c.Assert(resp.Kvs[0].Value, check.DeepEquals, []byte(val))
	s.TearDownTest(c)
=======
	require.NoError(t, err2)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, resp.Kvs[0].Value, []byte(val))
}

func TestGetChangeFeeds(t *testing.T) {
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)
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
			_, err := s.client.Client.Put(context.Background(), GetEtcdKeyChangeFeedInfo(tc.ids[i]), tc.details[i])
			require.NoError(t, err)
		}
		_, result, err := s.client.GetChangeFeeds(context.Background())
		require.NoError(t, err)
		require.NoError(t, err)
		require.Equal(t, len(result), len(tc.ids))
		for i := 0; i < len(tc.ids); i++ {
			rawKv, ok := result[tc.ids[i]]
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

func TestGetPutTaskStatus(t *testing.T) {
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)
	ctx := context.Background()
	info := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {StartTs: 100},
		},
	}

	feedID := "feedid"
	captureID := "captureid"

	err := s.client.PutTaskStatus(ctx, feedID, captureID, info)
	require.NoError(t, err)

	_, getInfo, err := s.client.GetTaskStatus(ctx, feedID, captureID)
	require.NoError(t, err)
	require.Equal(t, getInfo, info)

	err = s.client.ClearAllCDCInfo(context.Background())
	require.NoError(t, err)
	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	require.True(t, cerror.ErrTaskStatusNotExists.Equal(err))
}

func TestGetPutTaskPosition(t *testing.T) {
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)
	ctx := context.Background()
	info := &model.TaskPosition{
		ResolvedTs:   99,
		CheckPointTs: 77,
	}

	feedID := "feedid"
	captureID := "captureid"

	updated, err := s.client.PutTaskPositionOnChange(ctx, feedID, captureID, info)
	require.NoError(t, err)
	require.True(t, updated)

	updated, err = s.client.PutTaskPositionOnChange(ctx, feedID, captureID, info)
	require.NoError(t, err)
	require.False(t, updated)

	info.CheckPointTs = 99
	updated, err = s.client.PutTaskPositionOnChange(ctx, feedID, captureID, info)
	require.NoError(t, err)
	require.True(t, updated)

	_, getInfo, err := s.client.GetTaskPosition(ctx, feedID, captureID)
	require.NoError(t, err)
	require.Equal(t, getInfo, info)

	err = s.client.ClearAllCDCInfo(ctx)
	require.NoError(t, err)
	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	require.True(t, cerror.ErrTaskStatusNotExists.Equal(err))
}

func TestOpChangeFeedDetail(t *testing.T) {
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)
	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
		SortDir: "/old-version/sorter",
	}
	cfID := "test-op-cf"

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
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)
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
		err := s.client.SaveChangeFeedInfo(ctx, item.info, item.id)
		require.NoError(t, err)
	}

	allChangFeedInfo, err := s.client.GetAllChangeFeedInfo(ctx)
	require.NoError(t, err)

	for _, item := range infos {
		obtained, found := allChangFeedInfo[item.id]
		require.True(t, found)
		require.Equal(t, item.info.SinkURI, obtained.SinkURI)
		require.Equal(t, item.info.SortDir, obtained.SortDir)
	}
}

func TestGetAllChangeFeedStatus(t *testing.T) {
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)

	changefeeds := map[model.ChangeFeedID]*model.ChangeFeedStatus{
		"cf1": {
			ResolvedTs:   100,
			CheckpointTs: 90,
		},
		"cf2": {
			ResolvedTs:   100,
			CheckpointTs: 70,
		},
	}
	for id, cf := range changefeeds {
		err := s.client.PutChangeFeedStatus(context.Background(), id, cf)
		require.NoError(t, err)
	}
	statuses, err := s.client.GetAllChangeFeedStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, statuses, changefeeds)
}

func TestCreateChangefeed(t *testing.T) {
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)

	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
	}

	err := s.client.CreateChangefeedInfo(ctx, detail, "test-id")
	require.NoError(t, err)

	err = s.client.CreateChangefeedInfo(ctx, detail, "test-id")
	require.True(t, cerror.ErrChangeFeedAlreadyExists.Equal(err))
}

func TestGetAllCaptureLeases(t *testing.T) {
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)

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
		sess, err := concurrency.NewSession(s.client.Client.Unwrap(),
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
	s := &etcdTester{}
	s.setUpTest(t)
	defer s.tearDownTest(t)

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

	// We will create 3 mock captures and they take turns to be the owner.
	// While each is the owner, it tries to get its owner revision, and
	// checks that the global monotonicity is guaranteed.

	wg.Add(3)
	for i := 0; i < 3; i++ {
		i := i
		go func() {
			defer wg.Done()
			sess, err := concurrency.NewSession(s.client.Client.Unwrap(),
				concurrency.WithTTL(10 /* seconds */))
			require.Nil(t, err)
			election := concurrency.NewElection(sess, CaptureOwnerKey)

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
>>>>>>> 8a709d748 (cdc/metrics: Integrate sarama producer metrics (#4520))
}
