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

package kv

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type etcdSuite struct {
	e         *embed.Etcd
	clientURL *url.URL
	client    CDCEtcdClient
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

var _ = check.Suite(&etcdSuite{})

func (s *etcdSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	var err error
	s.clientURL, s.e, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	s.client = NewCDCEtcdClient(context.TODO(), client)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.e.Err(), func(e error) { c.Log(e) })
}

func (s *etcdSuite) TearDownTest(c *check.C) {
	s.e.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
}

func (s *etcdSuite) TestGetChangeFeeds(c *check.C) {
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
			c.Assert(err, check.IsNil)
		}
		_, result, err := s.client.GetChangeFeeds(context.Background())
		c.Assert(err, check.IsNil)
		c.Assert(len(result), check.Equals, len(tc.ids))
		for i := 0; i < len(tc.ids); i++ {
			rawKv, ok := result[tc.ids[i]]
			c.Assert(ok, check.IsTrue)
			c.Assert(string(rawKv.Value), check.Equals, tc.details[i])
		}
	}
	_, result, err := s.client.GetChangeFeeds(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(len(result), check.Equals, 3)

	err = s.client.ClearAllCDCInfo(context.Background())
	c.Assert(err, check.IsNil)

	_, result, err = s.client.GetChangeFeeds(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(len(result), check.Equals, 0)
}

func (s *etcdSuite) TestGetPutTaskStatus(c *check.C) {
	ctx := context.Background()
	info := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {StartTs: 100},
		},
	}

	feedID := "feedid"
	captureID := "captureid"

	err := s.client.PutTaskStatus(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	_, getInfo, err := s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(getInfo, check.DeepEquals, info)

	err = s.client.ClearAllCDCInfo(context.Background())
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskStatusNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestDeleteTaskStatus(c *check.C) {
	ctx := context.Background()
	info := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {StartTs: 100},
		},
	}
	feedID := "feedid"
	captureID := "captureid"

	err := s.client.PutTaskStatus(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	err = s.client.DeleteTaskStatus(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskStatusNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestGetPutTaskPosition(c *check.C) {
	ctx := context.Background()
	info := &model.TaskPosition{
		ResolvedTs:   66,
		CheckPointTs: 77,
	}

	feedID := "feedid"
	captureID := "captureid"

	err := s.client.PutTaskPosition(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	_, getInfo, err := s.client.GetTaskPosition(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(getInfo, check.DeepEquals, info)

	err = s.client.ClearAllCDCInfo(ctx)
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskStatusNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestDeleteTaskPosition(c *check.C) {
	ctx := context.Background()
	info := &model.TaskPosition{
		ResolvedTs:   77,
		CheckPointTs: 88,
	}
	feedID := "feedid"
	captureID := "captureid"

	err := s.client.PutTaskPosition(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	err = s.client.DeleteTaskPosition(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetTaskPosition(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskPositionNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestOpChangeFeedDetail(c *check.C) {
	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
	}
	cfID := "test-op-cf"

	err := s.client.SaveChangeFeedInfo(ctx, detail, cfID)
	c.Assert(err, check.IsNil)

	d, err := s.client.GetChangeFeedInfo(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(d.SinkURI, check.Equals, detail.SinkURI)

	err = s.client.DeleteChangeFeedInfo(ctx, cfID)
	c.Assert(err, check.IsNil)

	_, err = s.client.GetChangeFeedInfo(ctx, cfID)
	c.Assert(cerror.ErrChangeFeedNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestPutAllChangeFeedStatus(c *check.C) {
	var (
		status1 = &model.ChangeFeedStatus{
			ResolvedTs:   2200,
			CheckpointTs: 2000,
		}
		status2 = &model.ChangeFeedStatus{
			ResolvedTs:   2600,
			CheckpointTs: 2500,
		}
		err error
	)
	largeTxnInfo := make(map[string]*model.ChangeFeedStatus, embed.DefaultMaxTxnOps+1)
	for i := 0; i < int(embed.DefaultMaxTxnOps)+1; i++ {
		changefeedID := fmt.Sprintf("changefeed%d", i+1)
		largeTxnInfo[changefeedID] = status1
	}
	testCases := []struct {
		infos map[model.ChangeFeedID]*model.ChangeFeedStatus
	}{
		{infos: nil},
		{infos: map[string]*model.ChangeFeedStatus{"changefeed1": status1}},
		{infos: map[string]*model.ChangeFeedStatus{"changefeed1": status1, "changefeed2": status2}},
		{infos: largeTxnInfo},
	}

	for _, tc := range testCases {
		for changefeedID := range tc.infos {
			_, err = s.client.Client.Delete(context.Background(), GetEtcdKeyChangeFeedStatus(changefeedID))
			c.Assert(err, check.IsNil)
		}

		err = s.client.PutAllChangeFeedStatus(context.Background(), tc.infos)
		c.Assert(err, check.IsNil)

		for changefeedID, info := range tc.infos {
			resp, err := s.client.Client.Get(context.Background(), GetEtcdKeyChangeFeedStatus(changefeedID))
			c.Assert(err, check.IsNil)
			c.Assert(resp.Count, check.Equals, int64(1))
			infoStr, err := info.Marshal()
			c.Assert(err, check.IsNil)
			c.Assert(string(resp.Kvs[0].Value), check.Equals, infoStr)
		}
	}
}

func (s etcdSuite) TestGetAllChangeFeedStatus(c *check.C) {
	var (
		changefeeds = map[model.ChangeFeedID]*model.ChangeFeedStatus{
			"cf1": {
				ResolvedTs:   100,
				CheckpointTs: 90,
			},
			"cf2": {
				ResolvedTs:   100,
				CheckpointTs: 70,
			},
		}
	)
	err := s.client.PutAllChangeFeedStatus(context.Background(), changefeeds)
	c.Assert(err, check.IsNil)
	statuses, err := s.client.GetAllChangeFeedStatus(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(statuses, check.DeepEquals, changefeeds)
}

func (s *etcdSuite) TestRemoveChangeFeedStatus(c *check.C) {
	ctx := context.Background()
	changefeedID := "test-remove-changefeed-status"
	status := &model.ChangeFeedStatus{
		ResolvedTs: 1,
	}
	err := s.client.PutChangeFeedStatus(ctx, changefeedID, status)
	c.Assert(err, check.IsNil)
	status, _, err = s.client.GetChangeFeedStatus(ctx, changefeedID)
	c.Assert(err, check.IsNil)
	c.Assert(status, check.DeepEquals, status)
	err = s.client.RemoveChangeFeedStatus(ctx, changefeedID)
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetChangeFeedStatus(ctx, changefeedID)
	c.Assert(cerror.ErrChangeFeedNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestSetChangeFeedStatusTTL(c *check.C) {
	ctx := context.Background()
	err := s.client.PutChangeFeedStatus(ctx, "test1", &model.ChangeFeedStatus{
		ResolvedTs: 1,
	})
	c.Assert(err, check.IsNil)
	status, _, err := s.client.GetChangeFeedStatus(ctx, "test1")
	c.Assert(err, check.IsNil)
	c.Assert(status, check.DeepEquals, &model.ChangeFeedStatus{
		ResolvedTs: 1,
	})
	err = s.client.SetChangeFeedStatusTTL(ctx, "test1", 1 /* second */)
	c.Assert(err, check.IsNil)
	status, _, err = s.client.GetChangeFeedStatus(ctx, "test1")
	c.Assert(err, check.IsNil)
	c.Assert(status, check.DeepEquals, &model.ChangeFeedStatus{
		ResolvedTs: 1,
	})
	for i := 0; i < 50; i++ {
		_, _, err = s.client.GetChangeFeedStatus(ctx, "test1")
		log.Warn("nil", zap.Error(err))
		if err != nil {
			if cerror.ErrChangeFeedNotExists.Equal(err) {
				return
			}
			c.Fatal("got unexpected error", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Fatal("the change feed status is still exists after 5 seconds")
}

func (s *etcdSuite) TestDeleteTaskWorkload(c *check.C) {
	ctx := context.Background()
	workload := &model.TaskWorkload{
		1001: model.WorkloadInfo{Workload: 1},
		1002: model.WorkloadInfo{Workload: 3},
	}
	feedID := "feedid"
	captureID := "captureid"

	err := s.client.PutTaskWorkload(ctx, feedID, captureID, workload)
	c.Assert(err, check.IsNil)

	err = s.client.DeleteTaskWorkload(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)

	tw, err := s.client.GetTaskWorkload(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(len(tw), check.Equals, 0)
}

func (s *etcdSuite) TestGetAllTaskWorkload(c *check.C) {
	ctx := context.Background()
	feeds := []string{"feed1", "feed2"}
	captures := []string{"capture1", "capture2", "capture3"}
	expected := []map[string]*model.TaskWorkload{
		{
			"capture1": {1000: model.WorkloadInfo{Workload: 1}},
			"capture2": {1001: model.WorkloadInfo{Workload: 1}},
			"capture3": {1002: model.WorkloadInfo{Workload: 1}},
		},
		{
			"capture1": {2000: model.WorkloadInfo{Workload: 1}},
			"capture2": {2001: model.WorkloadInfo{Workload: 1}},
			"capture3": {2002: model.WorkloadInfo{Workload: 1}},
		},
	}

	for i, feed := range feeds {
		for j, capture := range captures {
			err := s.client.PutTaskWorkload(ctx, feed, capture, &model.TaskWorkload{
				int64(1000*(i+1) + j): model.WorkloadInfo{Workload: 1},
			})
			c.Assert(err, check.IsNil)
		}
	}
	for i := range feeds {
		workloads, err := s.client.GetAllTaskWorkloads(ctx, feeds[i])
		c.Assert(err, check.IsNil)
		c.Assert(workloads, check.DeepEquals, expected[i])
	}
}

func (s *etcdSuite) TestCreateChangefeed(c *check.C) {
	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
	}

	err := s.client.CreateChangefeedInfo(ctx, detail, "bad.id👻")
	c.Assert(err, check.ErrorMatches, ".*bad changefeed id.*")

	err = s.client.CreateChangefeedInfo(ctx, detail, "test-id")
	c.Assert(err, check.IsNil)

	err = s.client.CreateChangefeedInfo(ctx, detail, "test-id")
	c.Assert(cerror.ErrChangeFeedAlreadyExists.Equal(err), check.IsTrue)
}

type Captures []*model.CaptureInfo

func (c Captures) Len() int           { return len(c) }
func (c Captures) Less(i, j int) bool { return c[i].ID < c[j].ID }
func (c Captures) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func (s *etcdSuite) TestGetAllCaptureLeases(c *check.C) {
	ctx := context.Background()
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
		sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(10))
		c.Assert(err, check.IsNil)
		err = s.client.PutCaptureInfo(ctx, cinfo, sess.Lease())
		c.Assert(err, check.IsNil)
		leases[cinfo.ID] = int64(sess.Lease())
	}

	_, captures, err := s.client.GetCaptures(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(captures, check.HasLen, len(testCases))
	sort.Sort(Captures(captures))
	c.Assert(captures, check.DeepEquals, testCases)

	queryLeases, err := s.client.GetCaptureLeases(ctx)
	c.Assert(err, check.IsNil)
	c.Check(queryLeases, check.DeepEquals, leases)

	err = s.client.RevokeAllLeases(ctx, leases)
	c.Assert(err, check.IsNil)
	queryLeases, err = s.client.GetCaptureLeases(ctx)
	c.Assert(err, check.IsNil)
	c.Check(queryLeases, check.DeepEquals, map[string]int64{})
}
