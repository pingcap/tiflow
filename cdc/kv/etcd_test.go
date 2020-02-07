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
	client    *clientv3.Client
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
	s.client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
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
			_, err := s.client.Put(context.Background(), GetEtcdKeyChangeFeedInfo(tc.ids[i]), tc.details[i])
			c.Assert(err, check.IsNil)
		}
		_, result, err := GetChangeFeeds(context.Background(), s.client)
		c.Assert(err, check.IsNil)
		c.Assert(len(result), check.Equals, len(tc.ids))
		for i := 0; i < len(tc.ids); i++ {
			rawKv, ok := result[tc.ids[i]]
			c.Assert(ok, check.IsTrue)
			c.Assert(string(rawKv.Value), check.Equals, tc.details[i])
		}
	}
	_, result, err := GetChangeFeeds(context.Background(), s.client)
	c.Assert(err, check.IsNil)
	c.Assert(len(result), check.Equals, 3)

	err = ClearAllCDCInfo(context.Background(), s.client)
	c.Assert(err, check.IsNil)

	_, result, err = GetChangeFeeds(context.Background(), s.client)
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

	err := PutTaskStatus(ctx, s.client, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	_, getInfo, err := GetTaskStatus(ctx, s.client, feedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(getInfo, check.DeepEquals, info)

	err = ClearAllCDCInfo(context.Background(), s.client)
	c.Assert(err, check.IsNil)
	_, _, err = GetTaskStatus(ctx, s.client, feedID, captureID)
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

	err := PutTaskStatus(ctx, s.client, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	err = DeleteTaskStatus(ctx, s.client, feedID, captureID)
	c.Assert(err, check.IsNil)
	_, _, err = GetTaskStatus(ctx, s.client, feedID, captureID)
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

	err := PutTaskPosition(ctx, s.client, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	_, getInfo, err := GetTaskPosition(ctx, s.client, feedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(getInfo, check.DeepEquals, info)

	err = ClearAllCDCInfo(context.Background(), s.client)
	c.Assert(err, check.IsNil)
	_, _, err = GetTaskStatus(ctx, s.client, feedID, captureID)
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

	err := PutTaskPosition(ctx, s.client, feedID, captureID, info)
	c.Assert(err, check.IsNil)

	err = DeleteTaskPosition(ctx, s.client, feedID, captureID)
	c.Assert(err, check.IsNil)
	_, _, err = GetTaskPosition(ctx, s.client, feedID, captureID)
	c.Assert(errors.Cause(err), check.Equals, model.ErrTaskPositionNotExists)
}

func (s *etcdSuite) TestOpChangeFeedDetail(c *check.C) {
	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
	}
	cfID := "test-op-cf"

	err := SaveChangeFeedInfo(ctx, s.client, detail, cfID)
	c.Assert(err, check.IsNil)

	d, err := GetChangeFeedInfo(ctx, s.client, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(d.SinkURI, check.Equals, detail.SinkURI)

	err = DeleteChangeFeedInfo(ctx, s.client, cfID)
	c.Assert(err, check.IsNil)

	_, err = GetChangeFeedInfo(ctx, s.client, cfID)
	c.Assert(errors.Cause(err), check.Equals, model.ErrChangeFeedNotExists)
}
