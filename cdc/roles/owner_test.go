package roles

import (
	"context"
	"math"
	"net/url"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/google/uuid"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

type ownerSuite struct {
	owner     Owner
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
	s.errg.Wait()
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

func (h *handlerForPrueDMLTest) ExecDDL(context.Context, string, *model.DDL) error {
	panic("unreachable")
}

func (h *handlerForPrueDMLTest) Close() error {
	return nil
}

func (h *handlerForPrueDMLTest) Read(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedDetail, map[model.ChangeFeedID]model.ProcessorsInfos, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.index++
	return map[model.ChangeFeedID]*model.ChangeFeedDetail{
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

func (h *handlerForPrueDMLTest) Write(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedInfo) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	info, exist := infos["test_change_feed"]
	h.c.Assert(exist, check.IsTrue)
	h.c.Assert(info.ResolvedTs, check.Equals, h.expectResolvedTs[h.index])
	h.c.Assert(info.Status, check.Equals, model.ChangeFeedSyncDML)
	if h.index >= len(h.expectResolvedTs)-1 {
		log.Info("cancel")
		h.cancel()
	}
	return nil
}

func (s *ownerSuite) TestPureDML(c *check.C) {
	changeFeedInfos := map[model.ChangeFeedID]*model.ChangeFeedInfo{
		"test_change_feed": {
			TargetTs: 100,
			Status:   model.ChangeFeedSyncDML,
			ProcessorInfos: model.ProcessorsInfos{
				"capture_1": {},
				"capture_2": {},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())

	handler := &handlerForPrueDMLTest{
		index:            -1,
		resolvedTs1:      []uint64{10, 22, 64, 92, 99, 120},
		resolvedTs2:      []uint64{8, 36, 53, 88, 103, 108},
		expectResolvedTs: []uint64{8, 22, 53, 88, 99, 100},
		cancel:           cancel,
		c:                c,
	}

	manager := NewMockManager(uuid.New().String(), cancel)
	err := manager.CampaignOwner(ctx)
	c.Assert(err, check.IsNil)
	owner := &ownerImpl{
		changeFeedInfos: changeFeedInfos,
		ddlHandler:      handler,
		cfRWriter:       handler,
		manager:         manager,
		errCh:           make(chan error),
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
	expectStatus     []model.ChangeFeedStatus

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

func (h *handlerForDDLTest) ExecDDL(ctx context.Context, sinkURI string, ddl *model.DDL) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ddlExpectIndex++
	h.c.Assert(ddl, check.DeepEquals, h.ddls[h.ddlExpectIndex])
	h.c.Assert(ddl.Job.BinlogInfo.FinishedTS, check.Equals, h.currentGlobalResolvedTs)
	return nil
}

func (h *handlerForDDLTest) Close() error {
	return nil
}

func (h *handlerForDDLTest) Read(ctx context.Context) (map[model.CaptureID]*model.ChangeFeedDetail, map[model.ChangeFeedID]model.ProcessorsInfos, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.dmlIndex < len(h.resolvedTs1)-1 {
		h.dmlIndex++
	}
	return map[model.ChangeFeedID]*model.ChangeFeedDetail{
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

func (h *handlerForDDLTest) Write(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedInfo) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dmlExpectIndex++
	info, exist := infos["test_change_feed"]
	h.c.Assert(exist, check.IsTrue)
	h.currentGlobalResolvedTs = info.ResolvedTs
	h.c.Assert(info.ResolvedTs, check.Equals, h.expectResolvedTs[h.dmlExpectIndex])
	h.c.Assert(info.Status, check.Equals, h.expectStatus[h.dmlExpectIndex])
	if h.dmlExpectIndex >= len(h.expectResolvedTs)-1 {
		log.Info("cancel")
		h.cancel()
	}
	return nil
}

func (s *ownerSuite) TestDDL(c *check.C) {

	changeFeedInfos := map[model.ChangeFeedID]*model.ChangeFeedInfo{
		"test_change_feed": {
			TargetTs: 100,
			Status:   model.ChangeFeedSyncDML,
			ProcessorInfos: model.ProcessorsInfos{
				"capture_1": {},
				"capture_2": {},
			},
		},
	}
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
		expectStatus: []model.ChangeFeedStatus{
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedWaitToExecDDL, model.ChangeFeedExecDDL,
			model.ChangeFeedSyncDML},

		cancel: cancel,
		c:      c,
	}

	manager := NewMockManager(uuid.New().String(), cancel)
	err := manager.CampaignOwner(ctx)
	c.Assert(err, check.IsNil)
	owner := &ownerImpl{
		changeFeedInfos: changeFeedInfos,

		ddlHandler: handler,
		cfRWriter:  handler,
		manager:    manager,
		errCh:      make(chan error),
	}
	s.owner = owner
	err = owner.Run(ctx, 50*time.Millisecond)
	c.Assert(errors.Cause(err), check.DeepEquals, context.Canceled)

}

func (s *ownerSuite) TestAssignChangeFeed(c *check.C) {
	var (
		err          error
		changefeedID = "test-assign-changefeed"
		detail       = &model.ChangeFeedDetail{
			TableIDs: []uint64{30, 31, 32, 33},
		}
		captureIDs  = []string{"capture1", "capture2", "capture3"}
		expectedIDs = map[string][]*model.ProcessTableInfo{
			"capture1": {{ID: 30}, {ID: 33}},
			"capture2": {{ID: 31}},
			"capture3": {{ID: 32}},
		}
	)
	err = kv.SaveChangeFeedDetail(context.Background(), s.client, detail, changefeedID)
	c.Assert(err, check.IsNil)
	for _, cid := range captureIDs {
		cinfo := &model.CaptureInfo{ID: cid}
		// TODO: reorginaze etcd operation for capture info
		key := kv.EtcdKeyBase + "/capture/info/" + cid
		data, err := cinfo.Marshal()
		c.Assert(err, check.IsNil)
		_, err = s.client.Put(context.Background(), key, string(data))
		c.Assert(err, check.IsNil)
	}
	owner := &ownerImpl{
		etcdClient: s.client,
		errCh:      make(chan error),
	}
	pinfo, err := owner.assignChangeFeed(context.Background(), changefeedID)
	c.Assert(err, check.IsNil)

	etcdPinfo, err := kv.GetSubChangeFeedInfos(context.Background(), s.client, changefeedID)
	c.Assert(err, check.IsNil)
	c.Assert(pinfo, check.DeepEquals, etcdPinfo)
	for captureID, info := range etcdPinfo {
		c.Assert(expectedIDs[captureID], check.DeepEquals, info.TableInfos)
	}
}
