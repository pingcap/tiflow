// Copyright 2019 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
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
	s.client = NewCDCEtcdClient(client)
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
		TableInfos: []*model.ProcessTableInfo{
			{ID: 1, StartTs: 100},
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
	c.Assert(errors.Cause(err), check.Equals, model.ErrTaskStatusNotExists)
}

func (s *etcdSuite) TestDeleteTaskStatus(c *check.C) {
	ctx := context.Background()
	info := &model.TaskStatus{
		TableInfos: []*model.ProcessTableInfo{
			{ID: 1, StartTs: 100},
		},
	}
	feedID := "feedid"
	captureID := "captureid"

	err := s.client.PutTaskStatus(ctx, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	err = s.client.DeleteTaskStatus(ctx, feedID, captureID)
	c.Assert(err, check.IsNil)
	_, _, err = s.client.GetTaskStatus(ctx, feedID, captureID)
	c.Assert(errors.Cause(err), check.Equals, model.ErrTaskStatusNotExists)
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
	c.Assert(errors.Cause(err), check.Equals, model.ErrTaskStatusNotExists)
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
	c.Assert(errors.Cause(err), check.Equals, model.ErrTaskPositionNotExists)
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
	c.Assert(errors.Cause(err), check.Equals, model.ErrChangeFeedNotExists)
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

func (s *etcdSuite) TestGetEtcdKeyProcessorInfo(c *check.C) {
	tests := []struct {
		cid string // capture id
		pid string // processor id
		key string // generated key
	}{
		{"a", "b", ProcessorInfoKeyPrefix + "/a/b"},
		{"", "b", ProcessorInfoKeyPrefix + "//b"},
		{"a", "", ProcessorInfoKeyPrefix + "/a/"},
		{"", "", ProcessorInfoKeyPrefix + "//"},
	}

	for _, t := range tests {
		c.Assert(GetEtcdKeyProcessorInfo(t.cid, t.pid), check.Equals, t.key)
	}
}

func setupProcessors(s *etcdSuite, c *check.C) clientv3.LeaseID {
	ctx := context.Background()
	lease, err := s.client.Client.Grant(ctx, 3600)
	c.Assert(err, check.IsNil)
	// setup processors:
	//   a/b
	//   a/c
	//   d/e
	c.Assert(s.client.PutProcessorInfo(ctx, "a", &model.ProcessorInfo{
		ID: "b",
	}, lease.ID), check.IsNil)

	c.Assert(s.client.PutProcessorInfo(ctx, "a", &model.ProcessorInfo{
		ID: "c",
	}, lease.ID), check.IsNil)

	c.Assert(s.client.PutProcessorInfo(ctx, "d", &model.ProcessorInfo{
		ID: "e",
	}, lease.ID), check.IsNil)
	return lease.ID
}
func teardownProcessors(s *etcdSuite, c *check.C, leaseID clientv3.LeaseID) {
	ctx := context.Background()
	c.Assert(s.client.DeleteProcessorInfo(ctx, "a", "b"), check.IsNil)
	c.Assert(s.client.DeleteProcessorInfo(ctx, "a", "c"), check.IsNil)
	c.Assert(s.client.DeleteProcessorInfo(ctx, "d", "e"), check.IsNil)
	_, err := s.client.Client.Revoke(ctx, leaseID)
	c.Assert(err, check.IsNil)
}
func (s *etcdSuite) TestGetProcessorsFromPrefix(c *check.C) {
	ctx := context.Background()
	leaseID := setupProcessors(s, c)
	defer teardownProcessors(s, c, leaseID)

	tests := []struct {
		prefix     string
		processors []*model.ProcessorInfo
	}{
		// list all processors
		{ProcessorInfoKeyPrefix, []*model.ProcessorInfo{
			{ID: "b"},
			{ID: "c"},
			{ID: "e"},
		}},

		// list processors for capture "a"
		{ProcessorInfoKeyPrefix + "/a/", []*model.ProcessorInfo{
			{ID: "b"},
			{ID: "c"},
		}},

		// list processors for capture "d"
		{ProcessorInfoKeyPrefix + "/d/", []*model.ProcessorInfo{
			{ID: "e"},
		}},

		// list processors for a non-exist capture "f"
		{ProcessorInfoKeyPrefix + "/f/", nil},
	}

	for _, t := range tests {
		c.Log("testing on ", t.prefix)
		rev, processors, err := s.client.getProcessorsFromPrefix(ctx, t.prefix)
		c.Assert(rev, check.Greater, int64(0))
		c.Assert(err, check.IsNil)
		if t.processors != nil {
			c.Assert(len(processors), check.Equals, len(t.processors))
			for i, p := range processors {
				c.Assert(p.ID, check.Equals, t.processors[i].ID)
			}

		} else {
			c.Assert(processors, check.IsNil)
		}
	}
}

func (s *etcdSuite) TestGetProcessors(c *check.C) {
	ctx := context.Background()
	leaseID := setupProcessors(s, c)
	defer teardownProcessors(s, c, leaseID)

	tests := []struct {
		cid        string // capture id
		processors []*model.ProcessorInfo
	}{
		// list processors for capture "a"
		{"a", []*model.ProcessorInfo{
			{ID: "b"},
			{ID: "c"},
		}},

		// list processors for capture "d"
		{"d", []*model.ProcessorInfo{
			{ID: "e"},
		}},

		// list processors for a non-exist capture "f"
		{"f", nil},
	}
	for _, t := range tests {
		c.Log("testing on ", t.cid)
		rev, processors, err := s.client.GetProcessors(ctx, t.cid)
		c.Assert(rev, check.Greater, int64(0))
		c.Assert(err, check.IsNil)
		if t.processors != nil {
			c.Assert(len(processors), check.Equals, len(t.processors))
			for i, p := range processors {
				c.Assert(p.ID, check.Equals, t.processors[i].ID)
			}

		} else {
			c.Assert(processors, check.IsNil)
		}
	}
}

func (s *etcdSuite) TestGetAllProcessors(c *check.C) {
	ctx := context.Background()
	leaseID := setupProcessors(s, c)
	defer teardownProcessors(s, c, leaseID)
	rev, processors, err := s.client.GetAllProcessors(ctx)
	c.Assert(rev, check.Greater, int64(0))
	c.Assert(err, check.IsNil)
	ids := []string{"b", "c", "e"}
	for i, p := range processors {
		c.Assert(p.ID, check.Equals, ids[i])
	}
}

func (s *etcdSuite) TestPutProcessorInfo(c *check.C) {
	ctx := context.Background()
	lease, err := s.client.Client.Grant(ctx, 3600)
	c.Assert(err, check.IsNil)

	tests := []struct {
		cid       string
		processor *model.ProcessorInfo
	}{
		{"a", &model.ProcessorInfo{ID: "b"}},
		{"a", &model.ProcessorInfo{ID: "c"}},
		{"d", &model.ProcessorInfo{ID: "e"}},
	}

	for _, t := range tests {
		c.Assert(s.client.PutProcessorInfo(ctx, t.cid, t.processor, lease.ID), check.IsNil)
	}
}

func (s *etcdSuite) TestDeleteProcessorInfo(c *check.C) {
	ctx := context.Background()
	setupProcessors(s, c)
	processors := []*model.ProcessorInfo{
		{CaptureID: "a", ID: "b"},
		{CaptureID: "a", ID: "c"},
		{CaptureID: "d", ID: "e"},
	}
	for _, p := range processors {
		c.Assert(s.client.DeleteProcessorInfo(ctx, p.CaptureID, p.ID), check.IsNil)
	}

	rev, procs, err := s.client.GetAllProcessors(ctx)
	c.Assert(rev, check.Greater, int64(0))
	c.Assert(procs, check.IsNil)
	c.Assert(err, check.IsNil)
}
