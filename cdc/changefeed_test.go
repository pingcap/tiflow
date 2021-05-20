// Copyright 2021 PingCAP, Inc.
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

package cdc

import (
	"context"
	"net/url"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

type changefeedSuite struct {
	e         *embed.Etcd
	clientURL *url.URL
	client    kv.CDCEtcdClient
	sess      *concurrency.Session
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

var _ = check.Suite(&changefeedSuite{})

func (s *changefeedSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	var err error
	s.clientURL, s.e, err = etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
	sess, err := concurrency.NewSession(client)
	c.Assert(err, check.IsNil)
	s.sess = sess
	s.client = kv.NewCDCEtcdClient(context.Background(), client)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.errg = util.HandleErrWithErrGroup(s.ctx, s.e.Err(), func(e error) { c.Log(e) })
}

func (s *changefeedSuite) TearDownTest(c *check.C) {
	s.e.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
	s.client.Close() //nolint:errcheck
}

func (s *changefeedSuite) TestHandleMoveTableJobs(c *check.C) {
	defer testleak.AfterTest(c)()
	changefeed := new(changeFeed)
	captureID1 := "capture1"
	captureID2 := "capture2"
	changefeed.id = "changefeed-test"
	changefeed.leaseID = s.sess.Lease()
	changefeed.etcdCli = s.client
	changefeed.status = new(model.ChangeFeedStatus)
	changefeed.orphanTables = map[model.TableID]model.Ts{}
	captures := make(map[model.CaptureID]*model.CaptureInfo)
	captures[captureID1] = &model.CaptureInfo{
		ID: captureID1,
	}
	captures[captureID2] = &model.CaptureInfo{
		ID: captureID2,
	}
	changefeed.taskStatus = make(map[model.CaptureID]*model.TaskStatus)
	changefeed.taskStatus[captureID1] = &model.TaskStatus{Tables: map[model.TableID]*model.TableReplicaInfo{1: {}}}
	changefeed.taskStatus[captureID2] = &model.TaskStatus{Tables: map[model.TableID]*model.TableReplicaInfo{}}
	changefeed.moveTableJobs = make(map[model.TableID]*model.MoveTableJob)
	changefeed.moveTableJobs[1] = &model.MoveTableJob{
		TableID:          1,
		From:             captureID1,
		To:               captureID2,
		TableReplicaInfo: new(model.TableReplicaInfo),
	}
	err := changefeed.handleMoveTableJobs(s.ctx, captures)
	c.Assert(err, check.IsNil)
	taskStatuses, err := s.client.GetAllTaskStatus(s.ctx, changefeed.id)
	c.Assert(err, check.IsNil)
	taskStatuses[captureID1].ModRevision = 0
	c.Assert(taskStatuses, check.DeepEquals, model.ProcessorsInfos{captureID1: {
		Tables: map[model.TableID]*model.TableReplicaInfo{},
		Operation: map[model.TableID]*model.TableOperation{1: {
			Delete: true,
			Flag:   model.OperFlagMoveTable,
		}},
	}})

	// finish operation
	err = s.client.PutTaskStatus(s.ctx, changefeed.id, captureID1, &model.TaskStatus{
		Tables:    map[model.TableID]*model.TableReplicaInfo{},
		Operation: map[model.TableID]*model.TableOperation{},
	})
	c.Assert(err, check.IsNil)
	delete(changefeed.taskStatus[captureID1].Operation, 1)

	// capture2 offline
	delete(captures, captureID2)
	delete(changefeed.taskStatus, captureID2)

	err = changefeed.handleMoveTableJobs(s.ctx, captures)
	c.Assert(err, check.IsNil)
	c.Assert(changefeed.orphanTables, check.HasKey, model.TableID(1))
	c.Assert(changefeed.moveTableJobs, check.HasLen, 0)
}
