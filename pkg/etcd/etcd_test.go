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
	"sort"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

func Test(t *testing.T) { check.TestingT(t) }

type Captures []*model.CaptureInfo

func (c Captures) Len() int           { return len(c) }
func (c Captures) Less(i, j int) bool { return c[i].ID < c[j].ID }
func (c Captures) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type etcdSuite struct {
	etcd      *embed.Etcd
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
	s.clientURL, s.etcd, err = SetupEmbedEtcd(dir)
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
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.etcd.Err(), func(e error) { c.Log(e) })
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
	defer s.TearDownTest(c)
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
	c.Assert(err2, check.IsNil)
	c.Assert(resp.Kvs, check.HasLen, 1)
	c.Assert(resp.Kvs[0].Value, check.DeepEquals, []byte(val))
	s.TearDownTest(c)
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

func (s *etcdSuite) TestOpChangeFeedDetail(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx := context.Background()
	detail := &model.ChangeFeedInfo{
		SinkURI: "root@tcp(127.0.0.1:3306)/mysql",
		SortDir: "/old-version/sorter",
	}
	cfID := "test-op-cf"

	err := s.client.SaveChangeFeedInfo(ctx, detail, cfID)
	c.Assert(err, check.IsNil)

	d, err := s.client.GetChangeFeedInfo(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(d.SinkURI, check.Equals, detail.SinkURI)
	c.Assert(d.SortDir, check.Equals, detail.SortDir)

	err = s.client.DeleteChangeFeedInfo(ctx, cfID)
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
	for id, cf := range changefeeds {
		err := s.client.PutChangeFeedStatus(context.Background(), id, cf)
		c.Assert(err, check.IsNil)
	}
	statuses, err := s.client.GetAllChangeFeedStatus(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(statuses, check.DeepEquals, changefeeds)
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
