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
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	logConfig := logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL.String()},
		DialTimeout: 3 * time.Second,
		LogConfig:   &logConfig,
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
	s.client.Close() //nolint:errcheck
}

func (s *etcdSuite) TestGetChangeFeeds(c *check.C) {
	defer testleak.AfterTest(c)()
	// `TearDownTest` must be called before leak test, so we take advantage of
	// the stack feature of defer. Ditto for all tests with etcdSuite.
	defer s.TearDownTest(c)
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
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
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
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
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

	sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
	c.Assert(err, check.IsNil)
	err = s.client.LeaseGuardDeleteTaskStatus(ctx, feedID, captureID, sess.Lease())
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskStatusNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestGetPutTaskPosition(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	info := &model.TaskPosition{
		ResolvedTs:   99,
		CheckPointTs: 77,
	}

	feedID := "feedid"
	captureID := "captureid"

	updated, err := s.client.PutTaskPositionOnChange(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)
	c.Assert(updated, check.IsTrue)

	updated, err = s.client.PutTaskPositionOnChange(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)
	c.Assert(updated, check.IsFalse)

	info.CheckPointTs = 99
	updated, err = s.client.PutTaskPositionOnChange(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)
	c.Assert(updated, check.IsTrue)

	_, getInfo, err := s.client.GetTaskPosition(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(getInfo, check.DeepEquals, info)

	err = s.client.ClearAllCDCInfo(ctx)
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskStatusNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestDeleteTaskPosition(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	info := &model.TaskPosition{
		ResolvedTs:   77,
		CheckPointTs: 88,
	}
	feedID := "feedid"
	captureID := "captureid"

	_, err := s.client.PutTaskPositionOnChange(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
	c.Assert(err, check.IsNil)
	err = s.client.LeaseGuardDeleteTaskPosition(ctx, feedID, captureID, sess.Lease())
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetTaskPosition(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskPositionNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestOpChangeFeedDetail(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
		SortDir: "/old-version/sorter",
	}
	cfID := "test-op-cf"

	sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
	c.Assert(err, check.IsNil)
	err = s.client.LeaseGuardSaveChangeFeedInfo(ctx, detail, cfID, sess.Lease())
	c.Assert(err, check.IsNil)

	d, err := s.client.GetChangeFeedInfo(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(d.SinkURI, check.Equals, detail.SinkURI)
	c.Assert(d.SortDir, check.Equals, detail.SortDir)

	err = s.client.LeaseGuardDeleteChangeFeedInfo(ctx, cfID, sess.Lease())
	c.Assert(err, check.IsNil)

	_, err = s.client.GetChangeFeedInfo(ctx, cfID)
	c.Assert(cerror.ErrChangeFeedNotExists.Equal(err), check.IsTrue)
}

func (s etcdSuite) TestGetAllChangeFeedInfo(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

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
		c.Assert(err, check.IsNil)
	}

	allChangFeedInfo, err := s.client.GetAllChangeFeedInfo(ctx)
	c.Assert(err, check.IsNil)

	for _, item := range infos {
		obtained, found := allChangFeedInfo[item.id]
		c.Assert(found, check.IsTrue)
		c.Assert(item.info.SinkURI, check.Equals, obtained.SinkURI)
		c.Assert(item.info.SortDir, check.Equals, obtained.SortDir)
	}
}

func (s *etcdSuite) TestRemoveAllTaskXXX(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	status := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {StartTs: 100},
		},
	}
	position := &model.TaskPosition{
		ResolvedTs:   100,
		CheckPointTs: 100,
	}

	feedID := "feedid"
	captureID := "captureid"

	sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
	c.Assert(err, check.IsNil)

	err = s.client.PutTaskStatus(ctx, feedID, captureID, status)
	c.Assert(err, check.IsNil)
	_, err = s.client.PutTaskPositionOnChange(ctx, feedID, captureID, position)
	c.Assert(err, check.IsNil)
	err = s.client.LeaseGuardRemoveAllTaskStatus(ctx, feedID, sess.Lease())
	c.Assert(err, check.IsNil)
	err = s.client.LeaseGuardRemoveAllTaskPositions(ctx, feedID, sess.Lease())
	c.Assert(err, check.IsNil)

	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskStatusNotExists.Equal(err), check.IsTrue)
	_, _, err = s.client.GetTaskPosition(ctx, feedID, captureID)
	c.Assert(cerror.ErrTaskPositionNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestPutAllChangeFeedStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
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

		sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
		c.Assert(err, check.IsNil)
		err = s.client.LeaseGuardPutAllChangeFeedStatus(context.Background(), tc.infos, sess.Lease())
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
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
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
	err := s.client.PutAllChangeFeedStatus(context.Background(), changefeeds)
	c.Assert(err, check.IsNil)
	statuses, err := s.client.GetAllChangeFeedStatus(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(statuses, check.DeepEquals, changefeeds)
}

func (s *etcdSuite) TestRemoveChangeFeedStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	changefeedID := "test-remove-changefeed-status"
	status := &model.ChangeFeedStatus{
		ResolvedTs: 1,
	}

	sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
	c.Assert(err, check.IsNil)
	err = s.client.LeaseGuardPutChangeFeedStatus(ctx, changefeedID, status, sess.Lease())
	c.Assert(err, check.IsNil)
	status, _, err = s.client.GetChangeFeedStatus(ctx, changefeedID)
	c.Assert(err, check.IsNil)
	c.Assert(status, check.DeepEquals, status)

	err = s.client.LeaseGuardRemoveChangeFeedStatus(ctx, changefeedID, sess.Lease())
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetChangeFeedStatus(ctx, changefeedID)
	c.Assert(cerror.ErrChangeFeedNotExists.Equal(err), check.IsTrue)
}

func (s *etcdSuite) TestSetChangeFeedStatusTTL(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
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
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	workload := &model.TaskWorkload{
		1001: model.WorkloadInfo{Workload: 1},
		1002: model.WorkloadInfo{Workload: 3},
	}
	feedID := "feedid"
	captureID := "captureid"

	err := s.client.PutTaskWorkload(ctx, feedID, captureID, workload)
	c.Assert(err, check.IsNil)

	sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
	c.Assert(err, check.IsNil)
	err = s.client.LeaseGuardDeleteTaskWorkload(ctx, feedID, captureID, sess.Lease())
	c.Assert(err, check.IsNil)

	tw, err := s.client.GetTaskWorkload(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(len(tw), check.Equals, 0)
}

func (s *etcdSuite) TestGetAllTaskWorkload(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
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
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
	}

	err := s.client.CreateChangefeedInfo(ctx, detail, "test-id")
	c.Assert(err, check.IsNil)

	err = s.client.CreateChangefeedInfo(ctx, detail, "test-id")
	c.Assert(cerror.ErrChangeFeedAlreadyExists.Equal(err), check.IsTrue)
}

type Captures []*model.CaptureInfo

func (c Captures) Len() int           { return len(c) }
func (c Captures) Less(i, j int) bool { return c[i].ID < c[j].ID }
func (c Captures) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func (s *etcdSuite) TestGetAllCaptureLeases(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
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

	// make sure the RevokeAllLeases function can ignore the lease not exist
	leases["/fake/capture/info"] = 200
	err = s.client.RevokeAllLeases(ctx, leases)
	c.Assert(err, check.IsNil)
	queryLeases, err = s.client.GetCaptureLeases(ctx)
	c.Assert(err, check.IsNil)
	c.Check(queryLeases, check.DeepEquals, map[string]int64{})
}

func (s *etcdSuite) TestGetAllCDCInfo(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	captureID := "CAPTURE_ID"
	changefeedID := "CHANGEFEED_ID"
	ctx := context.Background()
	err := s.client.PutTaskWorkload(ctx, changefeedID, captureID, &model.TaskWorkload{
		11: model.WorkloadInfo{Workload: 1},
		22: model.WorkloadInfo{Workload: 22},
	})
	c.Assert(err, check.IsNil)
	err = s.client.PutTaskStatus(ctx, changefeedID, captureID, &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{11: {StartTs: 22}},
	})
	c.Assert(err, check.IsNil)
	kvs, err := s.client.GetAllCDCInfo(ctx)
	c.Assert(err, check.IsNil)
	expected := []struct {
		key   string
		value string
	}{{
		key:   "/tidb/cdc/task/status/CAPTURE_ID/CHANGEFEED_ID",
		value: "{\"tables\":{\"11\":{\"start-ts\":22,\"mark-table-id\":0}},\"operation\":null,\"admin-job-type\":0}",
	}, {
		key:   "/tidb/cdc/task/workload/CAPTURE_ID/CHANGEFEED_ID",
		value: "{\"11\":{\"workload\":1},\"22\":{\"workload\":22}}",
	}}
	for i, kv := range kvs {
		c.Assert(string(kv.Key), check.Equals, expected[i].key)
		c.Assert(string(kv.Value), check.Equals, expected[i].value)
	}
}

func (s *etcdSuite) TestAtomicPutTaskStatus(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	status := &model.TaskStatus{
		Tables: map[model.TableID]*model.TableReplicaInfo{
			1: {StartTs: 100},
		},
	}
	feedID := "feedid"
	captureID := "captureid"

	sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
	c.Assert(err, check.IsNil)
	err = s.client.PutTaskStatus(ctx, feedID, captureID, status)
	c.Assert(err, check.IsNil)

	status.Tables[2] = &model.TableReplicaInfo{StartTs: 120}
	_, revision, err := s.client.LeaseGuardAtomicPutTaskStatus(
		ctx, feedID, captureID, sess.Lease(),
		func(modRevision int64, taskStatus *model.TaskStatus) (bool, error) {
			taskStatus.Tables = status.Tables
			taskStatus.Operation = status.Operation
			return true, nil
		},
	)
	c.Assert(err, check.IsNil)
	modRevision, newStatus, err := s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(modRevision, check.Equals, revision)
	c.Assert(newStatus, check.DeepEquals, status)
}

func (s *etcdSuite) TestLeaseGuardWorks(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	// embed etcd election timeout is 1s, minimum session ttl is 2s
	sess, err := concurrency.NewSession(s.client.Client.Unwrap(), concurrency.WithTTL(2))
	c.Assert(err, check.IsNil)
	ctx, _, err := s.client.contextWithSafeLease(context.Background(), sess.Lease())
	c.Assert(err, check.IsNil)
	time.Sleep(time.Second * 2)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		c.Errorf("context is not done as expected")
	}

	_, err = s.client.Client.Revoke(context.Background(), sess.Lease())
	c.Assert(err, check.IsNil)
	_, _, err = s.client.contextWithSafeLease(context.Background(), sess.Lease())
	c.Assert(cerror.ErrLeaseTimeout.Equal(err), check.IsTrue)
}
