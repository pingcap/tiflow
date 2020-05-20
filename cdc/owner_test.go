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

package cdc

/*
import (
	"context"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/pingcap/ticdc/cdc/entry"

	"go.etcd.io/etcd/mvcc/mvccpb"

	"github.com/google/uuid"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/roles"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

type ownerSuite struct {
	owner     *ownerImpl
	e         *embed.Etcd
	clientURL *url.URL
	client    kv.CDCEtcdClient
	ctx       context.Context
	cancel    context.CancelFunc
	errg      *errgroup.Group
}

var _ = check.Suite(&ownerSuite{})

func (s *ownerSuite) SetUpTest(c *check.C) {
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

func (s *ownerSuite) TearDownTest(c *check.C) {
	s.e.Close()
	s.cancel()
	err := s.errg.Wait()
	if err != nil {
		c.Errorf("Error group error: %s", err)
	}
}

type handlerForPrueDMLTest struct {
	mu               sync.RWMutex
	index            int
	resolvedTs1      []uint64
	resolvedTs2      []uint64
	expectResolvedTs []uint64
	c                *check.C
	cancel           func()
}

func (h *handlerForPrueDMLTest) PullDDL() (resolvedTs uint64, ddl []*model.DDL, err error) {
	return uint64(math.MaxUint64), nil, nil
}

func (h *handlerForPrueDMLTest) ExecDDL(context.Context, string, map[string]string, model.Txn) error {
	panic("unreachable")
}

func (h *handlerForPrueDMLTest) Close() error {
	return nil
}

var _ ChangeFeedRWriter = &handlerForPrueDMLTest{}

func (h *handlerForPrueDMLTest) GetChangeFeeds(ctx context.Context) (int64, map[string]*mvccpb.KeyValue, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cfInfo := &model.ChangeFeedInfo{
		TargetTs: 100,
	}
	cfInfoJSON, err := cfInfo.Marshal()
	h.c.Assert(err, check.IsNil)
	rawKV := &mvccpb.KeyValue{
		Value: []byte(cfInfoJSON),
	}
	return 0, map[model.ChangeFeedID]*mvccpb.KeyValue{
		"test_change_feed": rawKV,
	}, nil
}

func (h *handlerForPrueDMLTest) GetAllTaskStatus(ctx context.Context, changefeedID string) (model.ProcessorsInfos, error) {
	if changefeedID != "test_change_feed" {
		return nil, model.ErrTaskStatusNotExists
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.index++
	return model.ProcessorsInfos{
		"capture_1": {},
		"capture_2": {},
	}, nil
}

func (h *handlerForPrueDMLTest) GetAllTaskPositions(ctx context.Context, changefeedID string) (map[string]*model.TaskPosition, error) {
	if changefeedID != "test_change_feed" {
		return nil, model.ErrTaskStatusNotExists
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.index++
	return map[string]*model.TaskPosition{
		"capture_1": {
			ResolvedTs: h.resolvedTs1[h.index],
		},
		"capture_2": {
			ResolvedTs: h.resolvedTs2[h.index],
		},
	}, nil
}

func (h *handlerForPrueDMLTest) GetChangeFeedStatus(ctx context.Context, id string) (*model.ChangeFeedStatus, error) {
	return nil, model.ErrChangeFeedNotExists
}

func (h *handlerForPrueDMLTest) PutAllChangeFeedStatus(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedStatus) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	info, exist := infos["test_change_feed"]
	h.c.Assert(exist, check.IsTrue)
	h.c.Assert(info.ResolvedTs, check.Equals, h.expectResolvedTs[h.index])
	// h.c.Assert(info.State, check.Equals, model.ChangeFeedSyncDML)
	if h.index >= len(h.expectResolvedTs)-1 {
		log.Info("cancel")
		h.cancel()
	}
	return nil
}

func (s *ownerSuite) TestPureDML(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	handler := &handlerForPrueDMLTest{
		index:            -1,
		resolvedTs1:      []uint64{10, 22, 64, 92, 99, 120},
		resolvedTs2:      []uint64{8, 36, 53, 88, 103, 108},
		expectResolvedTs: []uint64{8, 22, 53, 88, 99, 100},
		cancel:           cancel,
		c:                c,
	}

	tables := map[uint64]entry.TableName{1: {Schema: "any"}}

	changeFeeds := map[model.ChangeFeedID]*changeFeed{
		"test_change_feed": {
			tables:   tables,
			status:   &model.ChangeFeedStatus{},
			targetTs: 100,
			ddlState: model.ChangeFeedSyncDML,
			taskStatus: model.ProcessorsInfos{
				"capture_1": {},
				"capture_2": {},
			},
			taskPositions: map[string]*model.TaskPosition{
				"capture_1": {},
				"capture_2": {},
			},
			ddlHandler: handler,
		},
	}

	manager := roles.NewMockManager(uuid.New().String(), cancel)
	err := manager.CampaignOwner(ctx)
	c.Assert(err, check.IsNil)
	owner := &ownerImpl{
		cancelWatchCapture: cancel,
		changeFeeds:        changeFeeds,
		cfRWriter:          handler,
		etcdClient:         s.client,
		manager:            manager,
	}
	s.owner = owner
	err = owner.Run(ctx, 50*time.Millisecond)
	c.Assert(err.Error(), check.Equals, "context canceled")
}

type handlerForDDLTest struct {
	mu sync.RWMutex

	ddlIndex      int
	ddls          []*model.DDL
	ddlResolvedTs []uint64

	ddlExpectIndex int

	dmlIndex                int
	resolvedTs1             []uint64
	resolvedTs2             []uint64
	currentGlobalResolvedTs uint64

	dmlExpectIndex   int
	expectResolvedTs []uint64
	expectStatus     []model.ChangeFeedDDLState

	c      *check.C
	cancel func()
}

func (h *handlerForDDLTest) PullDDL() (resolvedTs uint64, jobs []*model.DDL, err error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.ddlIndex < len(h.ddls)-1 {
		h.ddlIndex++
	}
	return h.ddlResolvedTs[h.ddlIndex], []*model.DDL{h.ddls[h.ddlIndex]}, nil
}

func (h *handlerForDDLTest) ExecDDL(ctx context.Context, sinkURI string, _ map[string]string, txn model.Txn) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ddlExpectIndex++
	h.c.Assert(txn.DDL, check.DeepEquals, h.ddls[h.ddlExpectIndex])
	h.c.Assert(txn.DDL.Job.BinlogInfo.FinishedTS, check.Equals, h.currentGlobalResolvedTs)
	return nil
}

func (h *handlerForDDLTest) Close() error {
	return nil
}

func (h *handlerForDDLTest) GetChangeFeeds(ctx context.Context) (int64, map[string]*mvccpb.KeyValue, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	cfInfo := &model.ChangeFeedInfo{
		TargetTs: 100,
	}
	cfInfoJSON, err := cfInfo.Marshal()
	h.c.Assert(err, check.IsNil)
	rawKV := &mvccpb.KeyValue{
		Value: []byte(cfInfoJSON),
	}
	return 0, map[model.ChangeFeedID]*mvccpb.KeyValue{
		"test_change_feed": rawKV,
	}, nil
}

func (h *handlerForDDLTest) GetAllTaskStatus(ctx context.Context, changefeedID string) (model.ProcessorsInfos, error) {
	if changefeedID != "test_change_feed" {
		return nil, model.ErrTaskStatusNotExists
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.dmlIndex < len(h.resolvedTs1)-1 {
		h.dmlIndex++
	}
	return model.ProcessorsInfos{
		"capture_1": {},
		"capture_2": {},
	}, nil
}

func (h *handlerForDDLTest) GetAllTaskPositions(ctx context.Context, changefeedID string) (map[string]*model.TaskPosition, error) {
	if changefeedID != "test_change_feed" {
		return nil, model.ErrTaskStatusNotExists
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.dmlIndex < len(h.resolvedTs1)-1 {
		h.dmlIndex++
	}
	return map[string]*model.TaskPosition{
		"capture_1": {
			ResolvedTs:   h.resolvedTs1[h.dmlIndex],
			CheckPointTs: h.currentGlobalResolvedTs,
		},
		"capture_2": {
			ResolvedTs:   h.resolvedTs2[h.dmlIndex],
			CheckPointTs: h.currentGlobalResolvedTs,
		},
	}, nil
}

func (h *handlerForDDLTest) GetChangeFeedStatus(ctx context.Context, id string) (*model.ChangeFeedStatus, error) {
	return nil, model.ErrChangeFeedNotExists
}

func (h *handlerForDDLTest) PutAllChangeFeedStatus(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedStatus) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dmlExpectIndex++
	info, exist := infos["test_change_feed"]
	h.c.Assert(exist, check.IsTrue)
	h.currentGlobalResolvedTs = info.ResolvedTs
	h.c.Assert(info.ResolvedTs, check.Equals, h.expectResolvedTs[h.dmlExpectIndex])
	// h.c.Assert(info.State, check.Equals, h.expectStatus[h.dmlExpectIndex])
	if h.dmlExpectIndex >= len(h.expectResolvedTs)-1 {
		log.Info("cancel")
		h.cancel()
	}
	return nil
}

func (s *ownerSuite) TestDDL(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())

	handler := &handlerForDDLTest{
		ddlIndex:      -1,
		ddlResolvedTs: []uint64{5, 8, 49, 91, 113},
		ddls: []*model.DDL{
			{Job: &timodel.Job{
				ID: 1,
				BinlogInfo: &timodel.HistoryInfo{
					FinishedTS: 3,
				},
			}},
			{Job: &timodel.Job{
				ID: 2,
				BinlogInfo: &timodel.HistoryInfo{
					FinishedTS: 7,
				},
			}},
			{Job: &timodel.Job{
				ID: 3,
				BinlogInfo: &timodel.HistoryInfo{
					FinishedTS: 11,
				},
			}},
			{Job: &timodel.Job{
				ID: 4,
				BinlogInfo: &timodel.HistoryInfo{
					FinishedTS: 89,
				},
			}},
			{Job: &timodel.Job{
				ID: 5,
				BinlogInfo: &timodel.HistoryInfo{
					FinishedTS: 111,
				},
			}},
		},

		ddlExpectIndex: -1,

		dmlIndex:                -1,
		resolvedTs1:             []uint64{10, 22, 64, 92, 99, 120},
		resolvedTs2:             []uint64{8, 36, 53, 88, 103, 108},
		currentGlobalResolvedTs: 0,

		dmlExpectIndex: -1,
		expectResolvedTs: []uint64{
			3, 3,
			7, 7,
			11, 11,
			89, 89,
			100},
		expectStatus: []model.ChangeFeedDDLState{
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedSyncDML},

		cancel: cancel,
		c:      c,
	}

	tables := map[uint64]entry.TableName{1: {Schema: "any"}}

	filter, err := newTxnFilter(&model.ReplicaConfig{})
	c.Assert(err, check.IsNil)
	changeFeeds := map[model.ChangeFeedID]*changeFeed{
		"test_change_feed": {
			tables:   tables,
			info:     &model.ChangeFeedInfo{},
			status:   &model.ChangeFeedStatus{},
			targetTs: 100,
			ddlState: model.ChangeFeedSyncDML,
			taskStatus: model.ProcessorsInfos{
				"capture_1": {},
				"capture_2": {},
			},
			taskPositions: map[string]*model.TaskPosition{
				"capture_1": {},
				"capture_2": {},
			},
			ddlHandler: handler,
			filter:     filter,
		},
	}

	manager := roles.NewMockManager(uuid.New().String(), cancel)
	err = manager.CampaignOwner(ctx)
	c.Assert(err, check.IsNil)
	owner := &ownerImpl{
		cancelWatchCapture: cancel,
		changeFeeds:        changeFeeds,

		// ddlHandler: handler,
		etcdClient: s.client,
		cfRWriter:  handler,
		manager:    manager,
	}
	s.owner = owner
	err = owner.Run(ctx, 50*time.Millisecond)
	c.Assert(errors.Cause(err), check.DeepEquals, context.Canceled)
}

func (s *ownerSuite) TestHandleAdmin(c *check.C) {
	cfID := "test_handle_admin"
	sampleCF := &changeFeed{
		id:       cfID,
		info:     &model.ChangeFeedInfo{},
		status:   &model.ChangeFeedStatus{},
		ddlState: model.ChangeFeedSyncDML,
		taskStatus: model.ProcessorsInfos{
			"capture_1": {},
			"capture_2": {},
		},
		taskPositions: map[string]*model.TaskPosition{
			"capture_1": {ResolvedTs: 10001},
			"capture_2": {},
		},
		infoWriter: storage.NewOwnerTaskStatusEtcdWriter(s.client),
		ddlHandler: &handlerForDDLTest{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	manager := roles.NewMockManager(uuid.New().String(), cancel)
	owner := &ownerImpl{
		cancelWatchCapture: cancel,
		manager:            manager,
		etcdClient:         s.client,
		cfRWriter:          s.client,
	}
	owner.changeFeeds = map[model.ChangeFeedID]*changeFeed{cfID: sampleCF}
	for cid, pinfo := range sampleCF.taskPositions {
		key := kv.GetEtcdKeyTaskStatus(cfID, cid)
		pinfoStr, err := pinfo.Marshal()
		c.Assert(err, check.IsNil)
		_, err = s.client.Client.Put(ctx, key, pinfoStr)
		c.Assert(err, check.IsNil)
	}
	checkAdminJobLen := func(length int) {
		owner.adminJobsLock.Lock()
		c.Assert(owner.adminJobs, check.HasLen, length)
		owner.adminJobsLock.Unlock()
	}

	err := owner.EnqueueJob(model.AdminJob{CfID: cfID, Type: model.AdminStop})
	c.Assert(errors.Cause(err), check.Equals, concurrency.ErrElectionNotLeader)
	c.Assert(manager.CampaignOwner(ctx), check.IsNil)

	c.Assert(owner.EnqueueJob(model.AdminJob{CfID: cfID, Type: model.AdminStop}), check.IsNil)
	checkAdminJobLen(1)
	c.Assert(owner.handleAdminJob(ctx), check.IsNil)
	checkAdminJobLen(0)
	c.Assert(len(owner.changeFeeds), check.Equals, 0)
	// check changefeed info is set admin job
	info, err := owner.etcdClient.GetChangeFeedInfo(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(info.AdminJobType, check.Equals, model.AdminStop)
	// check processor is set admin job
	for cid := range sampleCF.taskPositions {
		_, subInfo, err := owner.etcdClient.GetTaskStatus(ctx, cfID, cid)
		c.Assert(err, check.IsNil)
		c.Assert(subInfo.AdminJobType, check.Equals, model.AdminStop)
	}
	// check changefeed status is set admin job
	st, err := owner.etcdClient.GetChangeFeedStatus(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminStop)

	c.Assert(owner.EnqueueJob(model.AdminJob{CfID: cfID, Type: model.AdminResume}), check.IsNil)
	c.Assert(owner.handleAdminJob(ctx), check.IsNil)
	checkAdminJobLen(0)
	// check changefeed info is set admin job
	info, err = owner.etcdClient.GetChangeFeedInfo(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(info.AdminJobType, check.Equals, model.AdminResume)
	// check changefeed status is set admin job
	st, err = owner.etcdClient.GetChangeFeedStatus(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminResume)

	owner.changeFeeds[cfID] = sampleCF
	c.Assert(owner.EnqueueJob(model.AdminJob{CfID: cfID, Type: model.AdminRemove}), check.IsNil)
	c.Assert(owner.handleAdminJob(ctx), check.IsNil)
	checkAdminJobLen(0)
	c.Assert(len(owner.changeFeeds), check.Equals, 0)
	// check changefeed info is deleted
	_, err = owner.etcdClient.GetChangeFeedInfo(ctx, cfID)
	c.Assert(errors.Cause(err), check.Equals, model.ErrChangeFeedNotExists)
	// check processor is set admin job
	for cid := range sampleCF.taskPositions {
		_, subInfo, err := owner.etcdClient.GetTaskStatus(ctx, cfID, cid)
		c.Assert(err, check.IsNil)
		c.Assert(subInfo.AdminJobType, check.Equals, model.AdminRemove)
	}
	// check changefeed status is set admin job
	st, err = owner.etcdClient.GetChangeFeedStatus(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminRemove)
}

func (s *ownerSuite) TestChangefeedApplyDDLJob(c *check.C) {
	var (
		jobs = []*timodel.Job{
			{
				ID:       1,
				SchemaID: 1,
				Type:     timodel.ActionCreateSchema,
				State:    timodel.JobStateSynced,
				Query:    "create database test",
				BinlogInfo: &timodel.HistoryInfo{
					SchemaVersion: 1,
					DBInfo: &timodel.DBInfo{
						ID:   1,
						Name: timodel.NewCIStr("test"),
					},
				},
			},
			{
				ID:       2,
				SchemaID: 1,
				Type:     timodel.ActionCreateTable,
				State:    timodel.JobStateSynced,
				Query:    "create table t1 (id int primary key)",
				BinlogInfo: &timodel.HistoryInfo{
					SchemaVersion: 2,
					DBInfo: &timodel.DBInfo{
						ID:   1,
						Name: timodel.NewCIStr("test"),
					},
					TableInfo: &timodel.TableInfo{
						ID:   47,
						Name: timodel.NewCIStr("t1"),
					},
				},
			},
			{
				ID:       2,
				SchemaID: 1,
				Type:     timodel.ActionCreateTable,
				State:    timodel.JobStateSynced,
				Query:    "create table t2 (id int primary key)",
				BinlogInfo: &timodel.HistoryInfo{
					SchemaVersion: 2,
					DBInfo: &timodel.DBInfo{
						ID:   1,
						Name: timodel.NewCIStr("test"),
					},
					TableInfo: &timodel.TableInfo{
						ID:   49,
						Name: timodel.NewCIStr("t2"),
					},
				},
			},
			{
				ID:       2,
				SchemaID: 1,
				TableID:  49,
				Type:     timodel.ActionDropTable,
				State:    timodel.JobStateSynced,
				Query:    "drop table t2",
				BinlogInfo: &timodel.HistoryInfo{
					SchemaVersion: 3,
					DBInfo: &timodel.DBInfo{
						ID:   1,
						Name: timodel.NewCIStr("test"),
					},
					TableInfo: &timodel.TableInfo{
						ID:   49,
						Name: timodel.NewCIStr("t2"),
					},
				},
			},
			{
				ID:       2,
				SchemaID: 1,
				TableID:  47,
				Type:     timodel.ActionTruncateTable,
				State:    timodel.JobStateSynced,
				Query:    "truncate table t1",
				BinlogInfo: &timodel.HistoryInfo{
					SchemaVersion: 4,
					DBInfo: &timodel.DBInfo{
						ID:   1,
						Name: timodel.NewCIStr("test"),
					},
					TableInfo: &timodel.TableInfo{
						ID:   51,
						Name: timodel.NewCIStr("t1"),
					},
				},
			},
			{
				ID:       2,
				SchemaID: 1,
				TableID:  51,
				Type:     timodel.ActionDropTable,
				State:    timodel.JobStateSynced,
				Query:    "drop table t1",
				BinlogInfo: &timodel.HistoryInfo{
					SchemaVersion: 5,
					DBInfo: &timodel.DBInfo{
						ID:   1,
						Name: timodel.NewCIStr("test"),
					},
					TableInfo: &timodel.TableInfo{
						ID:   51,
						Name: timodel.NewCIStr("t1"),
					},
				},
			},
			{
				ID:       2,
				SchemaID: 1,
				Type:     timodel.ActionDropSchema,
				State:    timodel.JobStateSynced,
				Query:    "drop database test",
				BinlogInfo: &timodel.HistoryInfo{
					SchemaVersion: 6,
					DBInfo: &timodel.DBInfo{
						ID:   1,
						Name: timodel.NewCIStr("test"),
					},
				},
			},
		}

		expectSchemas = []map[uint64]tableIDMap{
			{1: make(tableIDMap)},
			{1: {47: struct{}{}}},
			{1: {47: struct{}{}, 49: struct{}{}}},
			{1: {47: struct{}{}}},
			{1: {51: struct{}{}}},
			{1: make(tableIDMap)},
			{},
		}

		expectTables = []map[uint64]entry.TableName{
			{},
			{47: {Schema: "test", Table: "t1"}},
			{47: {Schema: "test", Table: "t1"}, 49: {Schema: "test", Table: "t2"}},
			{47: {Schema: "test", Table: "t1"}},
			{51: {Schema: "test", Table: "t1"}},
			{},
			{},
		}
	)
	schemaStorage, err := entry.NewStorage(nil)
	c.Assert(err, check.IsNil)
	filter, err := newTxnFilter(&model.ReplicaConfig{})
	c.Assert(err, check.IsNil)
	cf := &changeFeed{
		schema:        schemaStorage,
		schemas:       make(map[uint64]map[uint64]struct{}),
		tables:        make(map[uint64]entry.TableName),
		orphanTables:  make(map[uint64]model.ProcessTableInfo),
		toCleanTables: make(map[uint64]struct{}),
		filter:        filter,
	}
	for i, job := range jobs {
		err = cf.applyJob(job)
		c.Assert(err, check.IsNil)
		c.Assert(cf.schemas, check.DeepEquals, expectSchemas[i])
		c.Assert(cf.tables, check.DeepEquals, expectTables[i])
	}
}

type changefeedInfoSuite struct {
}

var _ = check.Suite(&changefeedInfoSuite{})

func (s *changefeedInfoSuite) TestMinimumTables(c *check.C) {
	cf := &changeFeed{
		taskStatus: map[model.CaptureID]*model.TaskStatus{
			"c1": {
				TableInfos: make([]*model.ProcessTableInfo, 2),
			},
			"c2": {
				TableInfos: make([]*model.ProcessTableInfo, 1),
			},
			"c3": {
				TableInfos: make([]*model.ProcessTableInfo, 3),
			},
		},
	}

	captures := map[string]*model.CaptureInfo{
		"c1": {},
		"c2": {},
		"c3": {},
	}

	c.Assert(cf.minimumTablesCapture(captures), check.Equals, "c2")

	captures["c4"] = &model.CaptureInfo{}
	c.Assert(cf.minimumTablesCapture(captures), check.Equals, "c4")
}

// TODO Test watchCapture
func (s *ownerSuite) TestWatchCapture(c *check.C){
	FIXME
}
*/
