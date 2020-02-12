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
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type etcdSuite struct {
	e         *embed.Etcd
	clientURL *url.URL
	client    kv.CDCEtcdClient
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
	s.client = kv.NewCDCEtcdClient(client)
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

func (s *etcdSuite) TestNewProcessorTsEtcdRWriter(c *check.C) {
	changefeedID := "feed id"
	captureID := "capture id"
	_, err := NewProcessorTsEtcdRWriter(s.client, captureID, changefeedID)
	c.Assert(err, check.NotNil)

	// create a task record in etcd
	info := new(model.TaskStatus)
	sinfo, err := info.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Client.Put(context.Background(), kv.GetEtcdKeyTaskStatus(changefeedID, captureID), sinfo)
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
	_, err = s.client.Client.Put(context.Background(), kv.GetEtcdKeyTaskStatus(changefeedID, captureID), sinfo)
	c.Assert(err, check.IsNil)

	// test WriteResolvedTs
	rw, err := NewProcessorTsEtcdRWriter(s.client, changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(rw.GetTaskStatus(), check.DeepEquals, info)

	pos := &model.TaskPosition{
		CheckPointTs: 96,
		ResolvedTs:   128,
	}
	err = rw.WritePosition(context.Background(), pos)
	c.Assert(err, check.IsNil)

	_, getPos, err := s.client.GetTaskPosition(context.Background(), changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(getPos.ResolvedTs, check.Equals, pos.ResolvedTs)
	c.Assert(getPos.CheckPointTs, check.Equals, pos.CheckPointTs)

	rw.GetTaskStatus().AdminJobType = model.AdminStop
	err = rw.WriteInfoIntoStorage(context.Background())
	c.Assert(err, check.IsNil)
	revision, getStatus, err := s.client.GetTaskStatus(context.Background(), changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(getStatus, check.DeepEquals, rw.GetTaskStatus())
	c.Assert(revision, check.Equals, rw.modRevision)

	// test table taskStatus changed, should return ErrWriteTsConflict.
	getInfo = info.Clone()
	getInfo.TableInfos = []*model.ProcessTableInfo{{ID: 11}, {ID: 12}, {ID: 13}}
	sinfo, err = getInfo.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Client.Put(context.Background(), kv.GetEtcdKeyTaskStatus(changefeedID, captureID), sinfo)
	c.Assert(err, check.IsNil)

	info.TableCLock = &model.TableLock{Ts: 6}
	err = rw.WriteInfoIntoStorage(context.Background())
	c.Assert(errors.Cause(err), check.Equals, model.ErrWriteTsConflict)

	changed, err := rw.UpdateInfo(context.Background())
	c.Assert(err, check.IsNil)
	c.Assert(changed, check.IsTrue)
	c.Assert(rw.GetTaskStatus(), check.DeepEquals, getInfo)
	info = rw.GetTaskStatus()

	// update success again.
	info.TableCLock = &model.TableLock{Ts: 6}
	err = rw.WriteInfoIntoStorage(context.Background())
	c.Assert(err, check.IsNil)
	revision, getInfo, err = s.client.GetTaskStatus(context.Background(), changefeedID, captureID)
	c.Assert(err, check.IsNil)
	c.Assert(revision, check.Equals, rw.modRevision)
	c.Assert(getInfo.TableCLock.Ts, check.Equals, uint64(6))
}

func (s *etcdSuite) TestProcessorTsWritePos(c *check.C) {

}

func (s *etcdSuite) TestProcessorTsReader(c *check.C) {
	var (
		changefeedID = "test-ts-reader-changefeed"
		captureID    = "test-ts-reader-capture"
		resolvedTs   uint64
		err          error
		info         = &model.ChangeFeedStatus{
			ResolvedTs:   1000,
			CheckpointTs: 900,
		}
	)

	// create a changefeed taskStatus in etcd
	sinfo, err := info.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Client.Put(context.Background(), kv.GetEtcdKeyChangeFeedStatus(changefeedID), sinfo)
	c.Assert(err, check.IsNil)

	// create a task record in etcd
	subInfo := new(model.TaskStatus)
	subInfoData, err := subInfo.Marshal()
	c.Assert(err, check.IsNil)
	_, err = s.client.Client.Put(context.Background(), kv.GetEtcdKeyTaskStatus(changefeedID, captureID), subInfoData)
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

	// owner adds table to processor when remote data is updated
	info.TableInfos = append(info.TableInfos, &model.ProcessTableInfo{ID: 52, StartTs: 100})
	info, err = ow.Write(context.Background(), changefeedID, captureID, info, false)
	c.Assert(err, check.IsNil)
	c.Assert(info.TableInfos, check.HasLen, 2)
	// check ModRevision after write
	revision, _, err := s.client.GetTaskStatus(context.Background(), changefeedID, captureID)
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
	err = s.client.PutTaskStatus(context.Background(), changefeedID, captureID, info)
	c.Assert(err, check.IsNil)
	info.TableCLock = nil

	// owner adds table to processor again
	info.TableInfos = append(info.TableInfos, &model.ProcessTableInfo{ID: 54, StartTs: 300})
	info, err = ow.Write(context.Background(), changefeedID, captureID, info, false)
	c.Assert(err, check.IsNil)
	c.Assert(info.TableInfos, check.HasLen, 1)
}
