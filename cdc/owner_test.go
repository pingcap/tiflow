package cdc

import (
	"context"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/embed"
	"github.com/google/uuid"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/roles"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

type ownerSuite struct {
	owner     *ownerImpl
	e         *embed.Etcd
	clientURL *url.URL
	client    *clientv3.Client
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
	s.client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	c.Assert(err, check.IsNil)
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

func (h *handlerForPrueDMLTest) ExecDDL(context.Context, string, model.Txn) error {
	panic("unreachable")
}

func (h *handlerForPrueDMLTest) Close() error {
	return nil
}

var _ ChangeFeedRWriter = &handlerForPrueDMLTest{}

// Read implements ChangeFeedRWriter interface.
func (h *handlerForPrueDMLTest) Read(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedInfo, map[model.ChangeFeedID]model.ProcessorsInfos, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.index++
	return map[model.ChangeFeedID]*model.ChangeFeedInfo{
			"test_change_feed": {
				TargetTs: 100,
			},
		}, map[model.ChangeFeedID]model.ProcessorsInfos{
			"test_change_feed": {
				"capture_1": {
					ResolvedTs: h.resolvedTs1[h.index],
				},
				"capture_2": {
					ResolvedTs: h.resolvedTs2[h.index],
				},
			},
		}, nil
}

// Read implements ChangeFeedRWriter interface.
func (h *handlerForPrueDMLTest) Write(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedStatus) error {
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

	tables := map[uint64]schema.TableName{1: {Schema: "any"}}

	changeFeeds := map[model.ChangeFeedID]*changeFeed{
		"test_change_feed": {
			tables:                  tables,
			ChangeFeedStatus:        &model.ChangeFeedStatus{},
			processorLastUpdateTime: make(map[string]time.Time),
			TargetTs:                100,
			State:                   model.ChangeFeedSyncDML,
			ProcessorInfos: model.ProcessorsInfos{
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
	expectStatus     []model.ChangeFeedState

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

func (h *handlerForDDLTest) ExecDDL(ctx context.Context, sinkURI string, txn model.Txn) error {
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

func (h *handlerForDDLTest) Read(ctx context.Context) (map[model.CaptureID]*model.ChangeFeedInfo, map[model.ChangeFeedID]model.ProcessorsInfos, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.dmlIndex < len(h.resolvedTs1)-1 {
		h.dmlIndex++
	}
	return map[model.ChangeFeedID]*model.ChangeFeedInfo{
			"test_change_feed": {
				TargetTs: 100,
			},
		}, map[model.ChangeFeedID]model.ProcessorsInfos{
			"test_change_feed": {
				"capture_1": {
					ResolvedTs:   h.resolvedTs1[h.dmlIndex],
					CheckPointTs: h.currentGlobalResolvedTs,
				},
				"capture_2": {
					ResolvedTs:   h.resolvedTs2[h.dmlIndex],
					CheckPointTs: h.currentGlobalResolvedTs,
				},
			},
		}, nil
}

func (h *handlerForDDLTest) Write(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedStatus) error {
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
		expectStatus: []model.ChangeFeedState{
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedSyncDML},

		cancel: cancel,
		c:      c,
	}

	tables := map[uint64]schema.TableName{1: {Schema: "any"}}

	changeFeeds := map[model.ChangeFeedID]*changeFeed{
		"test_change_feed": {
			tables:                  tables,
			info:                    &model.ChangeFeedInfo{},
			ChangeFeedStatus:        &model.ChangeFeedStatus{},
			processorLastUpdateTime: make(map[string]time.Time),
			TargetTs:                100,
			State:                   model.ChangeFeedSyncDML,
			ProcessorInfos: model.ProcessorsInfos{
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

		// ddlHandler: handler,
		cfRWriter: handler,
		manager:   manager,
	}
	s.owner = owner
	err = owner.Run(ctx, 50*time.Millisecond)
	c.Assert(errors.Cause(err), check.DeepEquals, context.Canceled)
}

func (s *ownerSuite) TestHandleAdmin(c *check.C) {
	cfID := "test_handle_admin"
	sampleCF := &changeFeed{
		ID:               cfID,
		info:             &model.ChangeFeedInfo{},
		ChangeFeedStatus: &model.ChangeFeedStatus{},
		State:            model.ChangeFeedSyncDML,
		ProcessorInfos: model.ProcessorsInfos{
			"capture_1": {ResolvedTs: 10001},
			"capture_2": {},
		},
		infoWriter: storage.NewOwnerTaskStatusEtcdWriter(s.client),
	}
	ctx, cancel := context.WithCancel(context.Background())
	manager := roles.NewMockManager(uuid.New().String(), cancel)
	owner := &ownerImpl{
		cancelWatchCapture: cancel,
		manager:            manager,
		etcdClient:         s.client,
		cfRWriter:          storage.NewChangeFeedEtcdRWriter(s.client),
	}
	owner.changeFeeds = map[model.ChangeFeedID]*changeFeed{cfID: sampleCF}
	for cid, pinfo := range sampleCF.ProcessorInfos {
		key := kv.GetEtcdKeyTask(cfID, cid)
		pinfoStr, err := pinfo.Marshal()
		c.Assert(err, check.IsNil)
		_, err = s.client.Put(ctx, key, pinfoStr)
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
	info, err := kv.GetChangeFeedInfo(ctx, owner.etcdClient, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(info.AdminJobType, check.Equals, model.AdminStop)
	// check processor is set admin job
	for cid := range sampleCF.ProcessorInfos {
		_, subInfo, err := kv.GetTaskStatus(ctx, owner.etcdClient, cfID, cid)
		c.Assert(err, check.IsNil)
		c.Assert(subInfo.AdminJobType, check.Equals, model.AdminStop)
	}
	// check changefeed status is set admin job
	st, err := kv.GetChangeFeedStatus(ctx, owner.etcdClient, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminStop)

	c.Assert(owner.EnqueueJob(model.AdminJob{CfID: cfID, Type: model.AdminResume}), check.IsNil)
	c.Assert(owner.handleAdminJob(ctx), check.IsNil)
	checkAdminJobLen(0)
	// check changefeed info is set admin job
	info, err = kv.GetChangeFeedInfo(ctx, owner.etcdClient, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(info.AdminJobType, check.Equals, model.AdminResume)
	// check changefeed status is set admin job
	st, err = kv.GetChangeFeedStatus(ctx, owner.etcdClient, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminResume)

	owner.changeFeeds[cfID] = sampleCF
	c.Assert(owner.EnqueueJob(model.AdminJob{CfID: cfID, Type: model.AdminRemove}), check.IsNil)
	c.Assert(owner.handleAdminJob(ctx), check.IsNil)
	checkAdminJobLen(0)
	c.Assert(len(owner.changeFeeds), check.Equals, 0)
	// check changefeed info is deleted
	_, err = kv.GetChangeFeedInfo(ctx, owner.etcdClient, cfID)
	c.Assert(errors.Cause(err), check.Equals, model.ErrChangeFeedNotExists)
	// check processor is set admin job
	for cid := range sampleCF.ProcessorInfos {
		_, subInfo, err := kv.GetTaskStatus(ctx, owner.etcdClient, cfID, cid)
		c.Assert(err, check.IsNil)
		c.Assert(subInfo.AdminJobType, check.Equals, model.AdminRemove)
	}
	// check changefeed status is set admin job
	st, err = kv.GetChangeFeedStatus(ctx, owner.etcdClient, cfID)
	c.Assert(err, check.IsNil)
	c.Assert(st.AdminJobType, check.Equals, model.AdminRemove)
}

type changefeedInfoSuite struct {
}

var _ = check.Suite(&changefeedInfoSuite{})

func (s *changefeedInfoSuite) TestMinimumTables(c *check.C) {
	cf := &changeFeed{
		ProcessorInfos: map[model.CaptureID]*model.TaskStatus{
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
