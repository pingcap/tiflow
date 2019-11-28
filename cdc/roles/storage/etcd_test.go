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

package storage

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

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
	s.errg.Wait()
}

func (s *etcdSuite) TestInfoReader(c *check.C) {
	var (
		info1 = map[model.CaptureID]*model.SubChangeFeedInfo{
			"capture1": {
				CheckPointTs: 1000,
				ResolvedTs:   1024,
				TableInfos: []*model.ProcessTableInfo{
					{ID: 1000, StartTs: 0},
					{ID: 1001, StartTs: 100},
				},
			},
			"capture2": {
				CheckPointTs: 1000,
				ResolvedTs:   1500,
				TableInfos: []*model.ProcessTableInfo{
					{ID: 1002, StartTs: 150},
					{ID: 1003, StartTs: 200},
				},
			},
		}
		err error
	)
	testCases := []struct {
		ids    []string
		pinfos map[string]model.ProcessorsInfos
	}{
		{ids: nil, pinfos: nil},
		{ids: []string{"changefeed1"}, pinfos: map[string]model.ProcessorsInfos{"changefeed1": info1}},
		{ids: []string{"changefeed1", "changefeed2"}, pinfos: map[string]model.ProcessorsInfos{"changefeed1": info1, "changefeed2": info1}},
	}

	rw := NewChangeFeedInfoEtcdRWriter(s.client)
	for _, tc := range testCases {
		_, err = s.client.Delete(context.Background(), kv.GetEtcdKeyChangeFeedList(), clientv3.WithPrefix())
		c.Assert(err, check.IsNil)
		for _, changefeedID := range tc.ids {
			_, err = s.client.Delete(context.Background(), kv.GetEtcdKeySubChangeFeedList(changefeedID), clientv3.WithPrefix())
			c.Assert(err, check.IsNil)
		}
		for i := 0; i < len(tc.ids); i++ {
			changefeedID := tc.ids[i]
			err = kv.SaveChangeFeedDetail(context.Background(), s.client, &model.ChangeFeedDetail{}, changefeedID)
			c.Assert(err, check.IsNil)
			for captureID, cinfo := range tc.pinfos[changefeedID] {
				sinfo, err := cinfo.Marshal()
				c.Assert(err, check.IsNil)
				_, err = s.client.Put(context.Background(), kv.GetEtcdKeySubChangeFeed(changefeedID, captureID), sinfo)
				c.Assert(err, check.IsNil)
			}
		}
		cfs, pinfos, err := rw.Read(context.Background())
		c.Assert(err, check.IsNil)
		c.Assert(len(cfs), check.Equals, len(tc.ids))
		c.Assert(len(pinfos), check.Equals, len(tc.ids))
		for _, changefeedID := range tc.ids {
			c.Assert(pinfos[changefeedID], check.DeepEquals, tc.pinfos[changefeedID])
		}
	}
}

func (s *etcdSuite) TestInfoWriter(c *check.C) {
	var (
		info1 = &model.ChangeFeedInfo{
			ResolvedTs:   2200,
			CheckpointTs: 2000,
		}
		info2 = &model.ChangeFeedInfo{
			ResolvedTs:   2600,
			CheckpointTs: 2500,
		}
		err error
	)
	largeTxnInfo := make(map[string]*model.ChangeFeedInfo, embed.DefaultMaxTxnOps+1)
	for i := 0; i < int(embed.DefaultMaxTxnOps)+1; i++ {
		changefeedID := fmt.Sprintf("changefeed%d", i+1)
		largeTxnInfo[changefeedID] = info1
	}
	testCases := []struct {
		infos map[model.ChangeFeedID]*model.ChangeFeedInfo
	}{
		{infos: nil},
		{infos: map[string]*model.ChangeFeedInfo{"changefeed1": info1}},
		{infos: map[string]*model.ChangeFeedInfo{"changefeed1": info1, "changefeed2": info2}},
		{infos: largeTxnInfo},
	}

	rw := NewChangeFeedInfoEtcdRWriter(s.client)
	for _, tc := range testCases {
		for changefeedID := range tc.infos {
			_, err = s.client.Delete(context.Background(), kv.GetEtcdKeyChangeFeedStatus(changefeedID))
			c.Assert(err, check.IsNil)
		}

		err = rw.Write(context.Background(), tc.infos)
		c.Assert(err, check.IsNil)

		for changefeedID, info := range tc.infos {
			resp, err := s.client.Get(context.Background(), kv.GetEtcdKeyChangeFeedStatus(changefeedID))
			c.Assert(err, check.IsNil)
			c.Assert(resp.Count, check.Equals, int64(1))
			infoStr, err := info.Marshal()
			c.Assert(err, check.IsNil)
			c.Assert(string(resp.Kvs[0].Value), check.Equals, infoStr)
		}
	}
}

func (s *etcdSuite) TestProcessorTsWriter(c *check.C) {
	var (
		changefeedID = "test-ts-writer-changefeed"
		captureID    = "test-ts-writer-capture"
		err          error
		revision     int64
		info         = &model.SubChangeFeedInfo{
			TableInfos: []*model.ProcessTableInfo{
				{ID: 11}, {ID: 12},
			},
		}
	)

	// create a subchangefeed record in etcd
	sinfo, err := info.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Put(context.Background(), kv.GetEtcdKeySubChangeFeed(changefeedID, captureID), sinfo)
	c.Assert(err, check.IsNil)

	// test WriteResolvedTs
	rw := NewProcessorTsEtcdRWriter(s.client, changefeedID, captureID)
	err = rw.WriteResolvedTs(context.Background(), uint64(128))
	c.Assert(err, check.IsNil)
	revision, info, err = kv.GetSubChangeFeedInfo(context.Background(), s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(revision, check.Equals, rw.modRevision)
	c.Assert(info.ResolvedTs, check.Equals, uint64(128))

	// test WriteCheckpointTs
	err = rw.WriteCheckpointTs(context.Background(), uint64(96))
	c.Assert(err, check.IsNil)
	revision, info, err = kv.GetSubChangeFeedInfo(context.Background(), s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(revision, check.Equals, rw.modRevision)
	c.Assert(info.CheckPointTs, check.Equals, uint64(96))

	// test table info changed, should retry successfully
	info.TableInfos = []*model.ProcessTableInfo{{ID: 11}, {ID: 12}, {ID: 13}}
	sinfo, err = info.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Put(context.Background(), kv.GetEtcdKeySubChangeFeed(changefeedID, captureID), sinfo)
	c.Assert(err, check.IsNil)

	err = rw.WriteResolvedTs(context.Background(), uint64(196))
	c.Assert(err, check.IsNil)
	revision, info, err = kv.GetSubChangeFeedInfo(context.Background(), s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(revision, check.Equals, rw.modRevision)
	c.Assert(info.ResolvedTs, check.Equals, uint64(196))
}

func (s *etcdSuite) TestProcessorTsReader(c *check.C) {
	var (
		changefeedID = "test-ts-reader-changefeed"
		captureID    = "test-ts-reader-capture"
		resolvedTs   uint64
		err          error
		info         = &model.ChangeFeedInfo{
			ResolvedTs:   1000,
			CheckpointTs: 900,
		}
	)

	// create a changefeed info in etcd
	sinfo, err := info.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Put(context.Background(), kv.GetEtcdKeyChangeFeedStatus(changefeedID), sinfo)
	c.Assert(err, check.IsNil)

	rw := NewProcessorTsEtcdRWriter(s.client, changefeedID, captureID)
	resolvedTs, err = rw.ReadGlobalResolvedTs(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(resolvedTs, check.Equals, info.ResolvedTs)
}

func (s *etcdSuite) TestCloneSubChangeFeedInfo(c *check.C) {
	var (
		changefeedID = "test-copy-changefeed"
		captureID    = "test-copy-capture"
		info         = &model.SubChangeFeedInfo{
			TableInfos: []*model.ProcessTableInfo{
				{ID: 1, StartTs: 1000},
				{ID: 2, StartTs: 2000},
			},
		}
	)
	rw := NewProcessorTsEtcdRWriter(s.client, changefeedID, captureID)
	rw.info = info
	copyInfo, err := rw.CloneSubChangeFeedInfo()
	c.Assert(err, check.IsNil)
	c.Assert(rw.info, check.DeepEquals, copyInfo)

	copyInfo.TableInfos = append(copyInfo.TableInfos, &model.ProcessTableInfo{ID: 3, StartTs: 3000})
	c.Assert(rw.info, check.DeepEquals, info)
	c.Assert(len(copyInfo.TableInfos), check.Greater, len(info.TableInfos))
}

func (s *etcdSuite) TestWriteTableCLock(c *check.C) {
	var (
		changefeedID = "test-write-clock-changefeed"
		captureID    = "test-write-clock-capture"
		info         = &model.SubChangeFeedInfo{
			TablePLock: &model.TableLock{Ts: 100},
		}
		checkpointTs uint64 = 300
	)
	rw := NewProcessorTsEtcdRWriter(s.client, changefeedID, captureID)
	rw.info = info
	err := rw.WriteTableCLock(context.Background(), checkpointTs)
	c.Assert(err, check.IsNil)
	c.Assert(rw.info.TableCLock, check.NotNil)
	c.Assert(rw.info.TableCLock.CheckpointTs, check.Equals, checkpointTs)

	revision, rinfo, err := kv.GetSubChangeFeedInfo(context.Background(), s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(revision, check.Equals, rw.modRevision)
	c.Assert(rinfo, check.DeepEquals, rw.info)
}
