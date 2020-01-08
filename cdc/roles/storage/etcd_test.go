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
	"github.com/pingcap/errors"
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
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
}

func (s *etcdSuite) TestInfoReader(c *check.C) {
	var (
		info1 = map[model.CaptureID]*model.TaskStatus{
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
			_, err = s.client.Delete(context.Background(), kv.GetEtcdKeyTaskList(changefeedID), clientv3.WithPrefix())
			c.Assert(err, check.IsNil)
		}
		for i := 0; i < len(tc.ids); i++ {
			changefeedID := tc.ids[i]
			err = kv.SaveChangeFeedDetail(context.Background(), s.client, &model.ChangeFeedDetail{}, changefeedID)
			c.Assert(err, check.IsNil)
			for captureID, cinfo := range tc.pinfos[changefeedID] {
				sinfo, err := cinfo.Marshal()
				c.Assert(err, check.IsNil)
				_, err = s.client.Put(context.Background(), kv.GetEtcdKeyTask(changefeedID, captureID), sinfo)
				c.Assert(err, check.IsNil)
			}
		}
		cfs, pinfos, err := rw.Read(context.Background())
		c.Assert(err, check.IsNil)
		c.Assert(len(cfs), check.Equals, len(tc.ids))
		c.Assert(len(pinfos), check.Equals, len(tc.ids))
		for _, changefeedID := range tc.ids {
			// don't check ModRevision
			for _, si := range pinfos[changefeedID] {
				si.ModRevision = 0
			}
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

func (s *etcdSuite) TestNewProcessorTsEtcdRWriter(c *check.C) {
	changefeedID := "feed id"
	captureID := "capture id"
	_, err := NewProcessorTsEtcdRWriter(s.client, captureID, changefeedID)
	c.Assert(err, check.NotNil)

	// create a task record in etcd
	info := new(model.TaskStatus)
	sinfo, err := info.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Put(context.Background(), kv.GetEtcdKeyTask(changefeedID, captureID), sinfo)
	c.Assert(err, check.IsNil)

	_, err = NewProcessorTsEtcdRWriter(s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
}

func (s *etcdSuite) TestProcessorTsWriter(c *check.C) {
	var (
		changefeedID = "test-ts-writer-changefeed"
		captureID    = "test-ts-writer-capture"
		err          error
		revision     int64
		info         = &model.TaskStatus{
			TableInfos: []*model.ProcessTableInfo{
				{ID: 11}, {ID: 12},
			},
		}
		getInfo *model.TaskStatus
	)

	// create a task record in etcd
	sinfo, err := info.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Put(context.Background(), kv.GetEtcdKeyTask(changefeedID, captureID), sinfo)
	c.Assert(err, check.IsNil)

	// test WriteResolvedTs
	rw, err := NewProcessorTsEtcdRWriter(s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(rw.GetTaskStatus(), check.DeepEquals, info)

	info = rw.GetTaskStatus()
	info.ResolvedTs = 128
	err = rw.WriteInfoIntoStorage(context.Background())
	c.Assert(err, check.IsNil)

	revision, getInfo, err = kv.GetTaskStatus(context.Background(), s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(revision, check.Equals, rw.modRevision)
	c.Assert(getInfo.ResolvedTs, check.Equals, uint64(128))

	// test WriteCheckpointTs
	info.CheckPointTs = 96
	err = rw.WriteInfoIntoStorage(context.Background())
	c.Assert(err, check.IsNil)

	revision, getInfo, err = kv.GetTaskStatus(context.Background(), s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(revision, check.Equals, rw.modRevision)
	c.Assert(getInfo.CheckPointTs, check.Equals, uint64(96))

	// test table taskStatus changed, should return ErrWriteTsConflict.
	getInfo = info.Clone()
	getInfo.TableInfos = []*model.ProcessTableInfo{{ID: 11}, {ID: 12}, {ID: 13}}
	sinfo, err = getInfo.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Put(context.Background(), kv.GetEtcdKeyTask(changefeedID, captureID), sinfo)
	c.Assert(err, check.IsNil)

	info.ResolvedTs = 196
	err = rw.WriteInfoIntoStorage(context.Background())
	c.Assert(errors.Cause(err), check.Equals, model.ErrWriteTsConflict)

	oldInfo, newInfo, err := rw.UpdateInfo(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(oldInfo, check.DeepEquals, info)
	c.Assert(newInfo, check.DeepEquals, getInfo)
	info = rw.GetTaskStatus()

	// update success again.
	info.ResolvedTs = 196
	err = rw.WriteInfoIntoStorage(context.Background())
	c.Assert(err, check.IsNil)
	revision, getInfo, err = kv.GetTaskStatus(context.Background(), s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(revision, check.Equals, rw.modRevision)
	c.Assert(getInfo.ResolvedTs, check.Equals, uint64(196))
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

	// create a changefeed taskStatus in etcd
	sinfo, err := info.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Put(context.Background(), kv.GetEtcdKeyChangeFeedStatus(changefeedID), sinfo)
	c.Assert(err, check.IsNil)

	// create a task record in etcd
	subInfo := new(model.TaskStatus)
	subInfoData, err := subInfo.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Put(context.Background(), kv.GetEtcdKeyTask(changefeedID, captureID), subInfoData)
	c.Assert(err, check.IsNil)

	rw, err := NewProcessorTsEtcdRWriter(s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)

	resolvedTs, err = rw.ReadGlobalResolvedTs(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(resolvedTs, check.Equals, info.ResolvedTs)
}

func (s *etcdSuite) TestOwnerTableInfoWriter(c *check.C) {
	var (
		changefeedID = "test-owner-table-writer-changefeed"
		captureID    = "test-owner-table-writer-capture"
		info         = &model.TaskStatus{}
		err          error
	)

	ow := NewOwnerTaskStatusEtcdWriter(s.client)

	// owner adds table to processor
	info.TableInfos = append(info.TableInfos, &model.ProcessTableInfo{ID: 50, StartTs: 100})
	info, err = ow.Write(context.Background(), changefeedID, captureID, info, false)
	c.Assert(err, check.IsNil)
	c.Assert(info.TableInfos, check.HasLen, 1)

	// simulate processor updates the task status
	infoClone := info.Clone()
	infoClone.ResolvedTs = 200
	infoClone.CheckPointTs = 100
	err = kv.PutTaskStatus(context.Background(), s.client, changefeedID, captureID, infoClone)
	c.Assert(err, check.IsNil)

	// owner adds table to processor when remote data is updated
	info.TableInfos = append(info.TableInfos, &model.ProcessTableInfo{ID: 52, StartTs: 100})
	info, err = ow.Write(context.Background(), changefeedID, captureID, info, false)
	c.Assert(err, check.IsNil)
	c.Assert(info.TableInfos, check.HasLen, 2)
	// check ModRevision after write
	revision, _, err := kv.GetTaskStatus(context.Background(), s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(info.ModRevision, check.Equals, revision)

	// owner removes table from processor
	info.TableInfos = info.TableInfos[:len(info.TableInfos)-1]
	info, err = ow.Write(context.Background(), changefeedID, captureID, info, true)
	c.Assert(err, check.IsNil)
	c.Assert(info.TableInfos, check.HasLen, 1)
	c.Assert(info.TablePLock, check.NotNil)

	// owner can't add table when plock is not resolved
	info.TableInfos = append(info.TableInfos, &model.ProcessTableInfo{ID: 52, StartTs: 100})
	info, err = ow.Write(context.Background(), changefeedID, captureID, info, false)
	c.Assert(errors.Cause(err), check.Equals, model.ErrFindPLockNotCommit)
	c.Assert(info.TableInfos, check.HasLen, 2)

	// owner can't remove table when plock is not resolved
	info.TableInfos = info.TableInfos[:0]
	info, err = ow.Write(context.Background(), changefeedID, captureID, info, true)
	c.Assert(errors.Cause(err), check.Equals, model.ErrFindPLockNotCommit)
	c.Assert(info.TableInfos, check.HasLen, 0)

	// simulate processor removes table and commit table p-lock
	info.TableCLock = &model.TableLock{Ts: info.TablePLock.Ts, CheckpointTs: 200}
	err = kv.PutTaskStatus(context.Background(), s.client, changefeedID, captureID, info)
	c.Assert(err, check.IsNil)
	info.TableCLock = nil

	// owner adds table to processor again
	info.TableInfos = append(info.TableInfos, &model.ProcessTableInfo{ID: 54, StartTs: 300})
	info, err = ow.Write(context.Background(), changefeedID, captureID, info, false)
	c.Assert(err, check.IsNil)
	c.Assert(info.TableInfos, check.HasLen, 1)
}
