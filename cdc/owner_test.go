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

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/store/mockstore"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
	"golang.org/x/sync/errgroup"
)

type ownerSuite struct {
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
	s.client = kv.NewCDCEtcdClient(context.TODO(), client)
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
	s.client.Close() //nolint:errcheck
}

type mockPDClient struct {
	pd.Client
	invokeCounter int
}

func (m *mockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	m.invokeCounter++
	return 0, errors.New("mock error")
}

func (s *ownerSuite) TestOwnerFlushChangeFeedInfos(c *check.C) {
	defer testleak.AfterTest(c)()
	mockPDCli := &mockPDClient{}
	mockOwner := Owner{
		pdClient:              mockPDCli,
		gcSafepointLastUpdate: time.Now(),
	}

	// Owner should ignore UpdateServiceGCSafePoint error.
	err := mockOwner.flushChangeFeedInfos(s.ctx)
	c.Assert(err, check.IsNil)
	c.Assert(mockPDCli.invokeCounter, check.Equals, 1)
	s.TearDownTest(c)
}

/*
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

func (h *handlerForPrueDMLTest) ExecDDL(context.Context, string, map[string]string, model.SingleTableTxn) error {
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
		return nil, cerror.ErrTaskStatusNotExists.GenWithStackByArgs("test_change_feed)
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
		return nil, cerror.ErrTaskStatusNotExists.GenWithStackByArgs("test_change_feed)
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
	return nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(id)
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
		defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	handler := &handlerForPrueDMLTest{
		index:            -1,
		resolvedTs1:      []uint64{10, 22, 64, 92, 99, 120},
		resolvedTs2:      []uint64{8, 36, 53, 88, 103, 108},
		expectResolvedTs: []uint64{8, 22, 53, 88, 99, 100},
		cancel:           cancel,
		c:                c,
	}

	tables := map[uint64]model.TableName{1: {Schema: "any"}}

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

func (h *handlerForDDLTest) ExecDDL(ctx context.Context, sinkURI string, _ map[string]string, txn model.SingleTableTxn) error {
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
		return nil, cerror.ErrTaskStatusNotExists.GenWithStackByArgs("test_change_feed")
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
		return nil, cerror.ErrTaskStatusNotExists.GenWithStackByArgs("test_change_feed")
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
	return nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(id)
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
		defer testleak.AfterTest(c)()
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

	tables := map[uint64]model.TableName{1: {Schema: "any"}}

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
*/

func (s *ownerSuite) TestHandleAdmin(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	cfID := "test_handle_admin"

	ctx, cancel0 := context.WithCancel(context.Background())
	defer cancel0()
	cctx, cancel := context.WithCancel(ctx)
	errg, _ := errgroup.WithContext(cctx)

	replicaConf := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(replicaConf)
	c.Assert(err, check.IsNil)

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
		ddlHandler: &ddlHandler{
			cancel: cancel,
			wg:     errg,
		},
		cancel: cancel,
	}
	errCh := make(chan error, 1)
	sink, err := sink.NewSink(ctx, cfID, "blackhole://", f, replicaConf, map[string]string{}, errCh)
	c.Assert(err, check.IsNil)
	defer sink.Close() //nolint:errcheck
	sampleCF.sink = sink

	capture, err := NewCapture(ctx, []string{s.clientURL.String()}, nil,
		&security.Credential{}, "127.0.0.1:12034", &processorOpts{flushCheckpointInterval: time.Millisecond * 200})
	c.Assert(err, check.IsNil)
	err = capture.Campaign(ctx)
	c.Assert(err, check.IsNil)

	owner, err := NewOwner(ctx, nil, &security.Credential{}, capture.session, DefaultCDCGCSafePointTTL, time.Millisecond*200)
	c.Assert(err, check.IsNil)

	sampleCF.etcdCli = owner.etcdClient
	owner.changeFeeds = map[model.ChangeFeedID]*changeFeed{cfID: sampleCF}
	for cid, pinfo := range sampleCF.taskPositions {
		key := kv.GetEtcdKeyTaskStatus(cfID, cid)
		pinfoStr, err := pinfo.Marshal()
		c.Assert(err, check.IsNil)
		_, err = s.client.Client.Put(ctx, key, pinfoStr)
		c.Assert(err, check.IsNil)
	}
	err = owner.etcdClient.PutChangeFeedStatus(ctx, cfID, &model.ChangeFeedStatus{})
	c.Assert(err, check.IsNil)
	err = owner.etcdClient.SaveChangeFeedInfo(ctx, sampleCF.info, cfID)
	c.Assert(err, check.IsNil)
	checkAdminJobLen := func(length int) {
		owner.adminJobsLock.Lock()
		c.Assert(owner.adminJobs, check.HasLen, length)
		owner.adminJobsLock.Unlock()
	}

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
	st, _, err := owner.etcdClient.GetChangeFeedStatus(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminStop)
	// check changefeed context is canceled
	select {
	case <-cctx.Done():
	default:
		c.Fatal("changefeed context is expected canceled")
	}

	cctx, cancel = context.WithCancel(ctx)
	sampleCF.cancel = cancel

	c.Assert(owner.EnqueueJob(model.AdminJob{CfID: cfID, Type: model.AdminResume}), check.IsNil)
	c.Assert(owner.handleAdminJob(ctx), check.IsNil)
	checkAdminJobLen(0)
	// check changefeed info is set admin job
	info, err = owner.etcdClient.GetChangeFeedInfo(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(info.AdminJobType, check.Equals, model.AdminResume)
	// check changefeed status is set admin job
	st, _, err = owner.etcdClient.GetChangeFeedStatus(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminResume)

	owner.changeFeeds[cfID] = sampleCF
	c.Assert(owner.EnqueueJob(model.AdminJob{CfID: cfID, Type: model.AdminRemove}), check.IsNil)
	c.Assert(owner.handleAdminJob(ctx), check.IsNil)
	checkAdminJobLen(0)
	c.Assert(len(owner.changeFeeds), check.Equals, 0)
	// check changefeed info is deleted
	_, err = owner.etcdClient.GetChangeFeedInfo(ctx, cfID)
	c.Assert(cerror.ErrChangeFeedNotExists.Equal(err), check.IsTrue)
	// check processor is set admin job
	for cid := range sampleCF.taskPositions {
		_, subInfo, err := owner.etcdClient.GetTaskStatus(ctx, cfID, cid)
		c.Assert(err, check.IsNil)
		c.Assert(subInfo.AdminJobType, check.Equals, model.AdminRemove)
	}
	// check changefeed status is set admin job
	st, _, err = owner.etcdClient.GetChangeFeedStatus(ctx, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminRemove)
	// check changefeed context is canceled
	select {
	case <-cctx.Done():
	default:
		c.Fatal("changefeed context is expected canceled")
	}
	owner.etcdClient.Close() //nolint:errcheck
}

func (s *ownerSuite) TestChangefeedApplyDDLJob(c *check.C) {
	defer testleak.AfterTest(c)()
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
						ID:         47,
						Name:       timodel.NewCIStr("t1"),
						PKIsHandle: true,
						Columns: []*timodel.ColumnInfo{
							{ID: 1, FieldType: types.FieldType{Flag: mysql.PriKeyFlag}, State: timodel.StatePublic},
						},
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
						ID:         49,
						Name:       timodel.NewCIStr("t2"),
						PKIsHandle: true,
						Columns: []*timodel.ColumnInfo{
							{ID: 1, FieldType: types.FieldType{Flag: mysql.PriKeyFlag}, State: timodel.StatePublic},
						},
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
						ID:         51,
						Name:       timodel.NewCIStr("t1"),
						PKIsHandle: true,
						Columns: []*timodel.ColumnInfo{
							{ID: 1, FieldType: types.FieldType{Flag: mysql.PriKeyFlag}, State: timodel.StatePublic},
						},
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

		expectSchemas = []map[int64]tableIDMap{
			{1: make(tableIDMap)},
			{1: {47: struct{}{}}},
			{1: {47: struct{}{}, 49: struct{}{}}},
			{1: {47: struct{}{}}},
			{1: {51: struct{}{}}},
			{1: make(tableIDMap)},
			{},
		}

		expectTables = []map[int64]model.TableName{
			{},
			{47: {Schema: "test", Table: "t1"}},
			{47: {Schema: "test", Table: "t1"}, 49: {Schema: "test", Table: "t2"}},
			{47: {Schema: "test", Table: "t1"}},
			{51: {Schema: "test", Table: "t1"}},
			{},
			{},
		}
	)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig())
	c.Assert(err, check.IsNil)

	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)
	defer func() {
		_ = store.Close()
	}()

	txn, err := store.Begin()
	c.Assert(err, check.IsNil)
	defer func() {
		_ = txn.Rollback()
	}()
	t := meta.NewMeta(txn)

	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(t, 0, false)
	c.Assert(err, check.IsNil)

	cf := &changeFeed{
		schema:        schemaSnap,
		schemas:       make(map[model.SchemaID]tableIDMap),
		tables:        make(map[model.TableID]model.TableName),
		partitions:    make(map[model.TableID][]int64),
		orphanTables:  make(map[model.TableID]model.Ts),
		toCleanTables: make(map[model.TableID]model.Ts),
		filter:        f,
		info:          &model.ChangeFeedInfo{Config: config.GetDefaultReplicaConfig()},
	}
	for i, job := range jobs {
		err = cf.schema.HandleDDL(job)
		c.Assert(err, check.IsNil)
		err = cf.schema.FillSchemaName(job)
		c.Assert(err, check.IsNil)
		_, err = cf.applyJob(context.TODO(), job)
		c.Assert(err, check.IsNil)
		c.Assert(cf.schemas, check.DeepEquals, expectSchemas[i])
		c.Assert(cf.tables, check.DeepEquals, expectTables[i])
	}
	s.TearDownTest(c)
}

func (s *ownerSuite) TestWatchCampaignKey(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	capture, err := NewCapture(ctx, []string{s.clientURL.String()}, nil,
		&security.Credential{}, "127.0.0.1:12034", &processorOpts{})
	c.Assert(err, check.IsNil)
	err = capture.Campaign(ctx)
	c.Assert(err, check.IsNil)

	ctx1, cancel1 := context.WithCancel(ctx)
	owner, err := NewOwner(ctx1, nil, &security.Credential{}, capture.session,
		DefaultCDCGCSafePointTTL, time.Millisecond*200)
	c.Assert(err, check.IsNil)

	// check campaign key deleted can be detected
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := owner.watchCampaignKey(ctx1)
		c.Assert(cerror.ErrOwnerCampaignKeyDeleted.Equal(err), check.IsTrue)
		cancel1()
	}()
	// ensure the watch loop has started
	time.Sleep(time.Millisecond * 100)
	etcdCli := owner.etcdClient.Client.Unwrap()
	key := fmt.Sprintf("%s/%x", kv.CaptureOwnerKey, owner.session.Lease())
	_, err = etcdCli.Delete(ctx, key)
	c.Assert(err, check.IsNil)
	wg.Wait()

	// check key is deleted before watch loop starts
	ctx1, cancel1 = context.WithCancel(ctx)
	err = owner.watchCampaignKey(ctx1)
	c.Assert(cerror.ErrOwnerCampaignKeyDeleted.Equal(err), check.IsTrue)

	// check the watch routine can be canceled
	err = capture.Campaign(ctx)
	c.Assert(err, check.IsNil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := owner.watchCampaignKey(ctx1)
		c.Assert(err, check.IsNil)
	}()
	// ensure the watch loop has started
	time.Sleep(time.Millisecond * 100)
	cancel1()
	wg.Wait()

	err = capture.etcdClient.Close()
	c.Assert(err, check.IsNil)
}

func (s *ownerSuite) TestCleanUpStaleTasks(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := "127.0.0.1:12034"
	ctx = util.PutCaptureAddrInCtx(ctx, addr)
	capture, err := NewCapture(ctx, []string{s.clientURL.String()}, nil,
		&security.Credential{}, addr, &processorOpts{})
	c.Assert(err, check.IsNil)
	err = s.client.PutCaptureInfo(ctx, capture.info, capture.session.Lease())
	c.Assert(err, check.IsNil)

	changefeed := "changefeed-name"
	invalidCapture := uuid.New().String()
	for _, captureID := range []string{capture.info.ID, invalidCapture} {
		taskStatus := &model.TaskStatus{}
		if captureID == invalidCapture {
			taskStatus.Tables = map[model.TableID]*model.TableReplicaInfo{
				51: {StartTs: 110},
			}
		}
		err = s.client.PutTaskStatus(ctx, changefeed, captureID, taskStatus)
		c.Assert(err, check.IsNil)
		_, err = s.client.PutTaskPositionOnChange(ctx, changefeed, captureID, &model.TaskPosition{CheckPointTs: 100, ResolvedTs: 120})
		c.Assert(err, check.IsNil)
		err = s.client.PutTaskWorkload(ctx, changefeed, captureID, &model.TaskWorkload{})
		c.Assert(err, check.IsNil)
	}
	err = s.client.SaveChangeFeedInfo(ctx, &model.ChangeFeedInfo{}, changefeed)
	c.Assert(err, check.IsNil)

	_, captureList, err := s.client.GetCaptures(ctx)
	c.Assert(err, check.IsNil)
	captures := make(map[model.CaptureID]*model.CaptureInfo)
	for _, c := range captureList {
		captures[c.ID] = c
	}
	owner, err := NewOwner(ctx, nil, &security.Credential{}, capture.session,
		DefaultCDCGCSafePointTTL, time.Millisecond*200)
	c.Assert(err, check.IsNil)
	// It is better to update changefeed information by `loadChangeFeeds`, however
	// `loadChangeFeeds` is too overweight, just mock enough information here.
	owner.changeFeeds = map[model.ChangeFeedID]*changeFeed{
		changefeed: {
			id:           changefeed,
			orphanTables: make(map[model.TableID]model.Ts),
		},
	}

	// capture information is not built, owner.run does nothing
	err = owner.run(ctx)
	c.Assert(err, check.IsNil)
	statuses, err := s.client.GetAllTaskStatus(ctx, changefeed)
	c.Assert(err, check.IsNil)
	// stale tasks are not cleaned up, since `cleanUpStaleTasks` does not run
	c.Assert(len(statuses), check.Equals, 2)
	c.Assert(len(owner.captures), check.Equals, 0)

	err = owner.rebuildCaptureEvents(ctx, captures)
	c.Assert(err, check.IsNil)
	c.Assert(len(owner.captures), check.Equals, 1)
	c.Assert(owner.captures, check.HasKey, capture.info.ID)
	c.Assert(owner.changeFeeds[changefeed].orphanTables, check.DeepEquals, map[model.TableID]model.Ts{51: 110})
	c.Assert(atomic.LoadInt32(&owner.captureLoaded), check.Equals, int32(1))
	// check stale tasks are cleaned up
	statuses, err = s.client.GetAllTaskStatus(ctx, changefeed)
	c.Assert(err, check.IsNil)
	c.Assert(len(statuses), check.Equals, 1)
	c.Assert(statuses, check.HasKey, capture.info.ID)
	positions, err := s.client.GetAllTaskPositions(ctx, changefeed)
	c.Assert(err, check.IsNil)
	c.Assert(len(positions), check.Equals, 1)
	c.Assert(positions, check.HasKey, capture.info.ID)
	workloads, err := s.client.GetAllTaskWorkloads(ctx, changefeed)
	c.Assert(err, check.IsNil)
	c.Assert(len(workloads), check.Equals, 1)
	c.Assert(workloads, check.HasKey, capture.info.ID)

	err = capture.etcdClient.Close()
	c.Assert(err, check.IsNil)
}

func (s *ownerSuite) TestWatchFeedChange(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := "127.0.0.1:12034"
	ctx = util.PutCaptureAddrInCtx(ctx, addr)
	capture, err := NewCapture(ctx, []string{s.clientURL.String()}, nil,
		&security.Credential{}, addr, &processorOpts{})
	c.Assert(err, check.IsNil)
	owner, err := NewOwner(ctx, nil, &security.Credential{}, capture.session,
		DefaultCDCGCSafePointTTL, time.Millisecond*200)
	c.Assert(err, check.IsNil)

	var (
		wg              sync.WaitGroup
		updateCount     = 0
		recvChangeCount = 0
	)
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		changefeedID := "test-changefeed"
		pos := &model.TaskPosition{CheckPointTs: 100, ResolvedTs: 102}
		for {
			select {
			case <-ctx1.Done():
				return
			default:
			}
			pos.ResolvedTs++
			pos.CheckPointTs++
			updated, err := capture.etcdClient.PutTaskPositionOnChange(ctx1, changefeedID, capture.info.ID, pos)
			if errors.Cause(err) == context.Canceled {
				return
			}
			c.Assert(err, check.IsNil)
			c.Assert(updated, check.IsTrue)
			updateCount++
			// sleep to avoid other goroutine starvation
			time.Sleep(time.Millisecond)
		}
	}()

	feedChangeReceiver, err := owner.feedChangeNotifier.NewReceiver(ownerRunInterval)
	c.Assert(err, check.IsNil)
	defer feedChangeReceiver.Stop()
	owner.watchFeedChange(ctx)
	wg.Add(1)
	go func() {
		defer func() {
			// there could be one message remaining in notification receiver, try to consume it
			select {
			case <-feedChangeReceiver.C:
			default:
			}
			wg.Done()
		}()
		for {
			select {
			case <-ctx1.Done():
				return
			case <-feedChangeReceiver.C:
				recvChangeCount++
				// sleep to simulate some owner work
				time.Sleep(time.Millisecond * 50)
			}
		}
	}()

	time.Sleep(time.Second * 2)
	// use cancel1 to avoid cancel the watchFeedChange
	cancel1()
	wg.Wait()
	c.Assert(recvChangeCount, check.Greater, 0)
	c.Assert(recvChangeCount, check.Less, updateCount)
	select {
	case <-feedChangeReceiver.C:
		c.Error("should not receive message from feed change chan any more")
	default:
	}

	err = capture.etcdClient.Close()
	if err != nil {
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}
}
