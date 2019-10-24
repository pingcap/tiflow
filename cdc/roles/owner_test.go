package roles

import (
	"context"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
)

type ownerSuite struct {
	owner Owner
}

var _ = check.Suite(&ownerSuite{})

type handlerForPrueDMLTest struct {
	index            int
	resolvedTS1      []uint64
	resolvedTS2      []uint64
	expectResolvedTS []uint64
	c                *check.C
	cancel           func()
}

func (h *handlerForPrueDMLTest) PullDDL() (resolvedTS uint64, jobs []*model.Job, err error) {
	return uint64(math.MaxUint64), nil, nil
}

func (h *handlerForPrueDMLTest) ExecDDL(*model.Job) error {
	panic("unreachable")
}

func (h *handlerForPrueDMLTest) Read(ctx context.Context) (map[ChangeFeedID]ProcessorsInfos, error) {
	h.index++
	return map[ChangeFeedID]ProcessorsInfos{
		"test_change_feed": {
			"capture_1": {
				ResolvedTS: h.resolvedTS1[h.index],
			},
			"capture_2": {
				ResolvedTS: h.resolvedTS2[h.index],
			},
		},
	}, nil
}

func (h *handlerForPrueDMLTest) Write(ctx context.Context, infos map[ChangeFeedID]*ChangeFeedInfo) error {
	info, exist := infos["test_change_feed"]
	h.c.Assert(exist, check.IsTrue)
	h.c.Assert(info.ResolvedTS(), check.Equals, h.expectResolvedTS[h.index])
	h.c.Assert(info.Status(), check.Equals, ChangeFeedSyncDML)
	if h.index >= len(h.expectResolvedTS)-1 {
		log.Info("cancel")
		h.cancel()
	}
	return nil
}

func (s *ownerSuite) TestPureDML(c *check.C) {
	changeFeedInfos := map[ChangeFeedID]*ChangeFeedInfo{
		"test_change_feed": {
			status: ChangeFeedSyncDML,
			processorInfos: ProcessorsInfos{
				"capture_1": {},
				"capture_2": {},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())

	handler := &handlerForPrueDMLTest{
		index:            -1,
		resolvedTS1:      []uint64{10, 22, 64, 92, 99, 120},
		resolvedTS2:      []uint64{8, 36, 53, 88, 103, 108},
		expectResolvedTS: []uint64{8, 22, 53, 88, 99, 100},
		cancel:           cancel,
		c:                c,
	}

	manager := NewMockManager(uuid.New().String(), cancel)
	err := manager.CampaignOwner(ctx)
	c.Assert(err, check.IsNil)
	owner := &ownerImpl{
		changeFeedInfos: changeFeedInfos,
		targetTS:        100,
		ddlHandler:      handler,
		cfRWriter:       handler,
		manager:         manager,
	}
	s.owner = owner
	err = owner.Run(ctx, 50*time.Millisecond)
	c.Assert(err.Error(), check.Equals, "context canceled")

}

type handlerForDDLTest struct {
	ddlIndex      int
	ddlJobs       []*model.Job
	ddlResolvedTS []uint64

	ddlExpectIndex int

	dmlIndex                int
	resolvedTS1             []uint64
	resolvedTS2             []uint64
	currentGlobalResolvedTS uint64

	dmlExpectIndex   int
	expectResolvedTS []uint64
	expectStatus     []ChangeFeedStatus

	c      *check.C
	cancel func()
}

func (h *handlerForDDLTest) PullDDL() (resolvedTS uint64, jobs []*model.Job, err error) {
	if h.ddlIndex < len(h.ddlJobs)-1 {
		h.ddlIndex++
	}
	return h.ddlResolvedTS[h.ddlIndex], []*model.Job{h.ddlJobs[h.ddlIndex]}, nil
}

func (h *handlerForDDLTest) ExecDDL(job *model.Job) error {
	h.ddlExpectIndex++
	h.c.Assert(job, check.DeepEquals, h.ddlJobs[h.ddlExpectIndex])
	h.c.Assert(job.BinlogInfo.FinishedTS, check.Equals, h.currentGlobalResolvedTS)
	return nil
}

func (h *handlerForDDLTest) Read(ctx context.Context) (map[ChangeFeedID]ProcessorsInfos, error) {
	if h.dmlIndex < len(h.resolvedTS1)-1 {
		h.dmlIndex++
	}
	return map[ChangeFeedID]ProcessorsInfos{
		"test_change_feed": {
			"capture_1": {
				ResolvedTS:   h.resolvedTS1[h.dmlIndex],
				CheckPointTS: h.currentGlobalResolvedTS,
			},
			"capture_2": {
				ResolvedTS:   h.resolvedTS2[h.dmlIndex],
				CheckPointTS: h.currentGlobalResolvedTS,
			},
		},
	}, nil
}

func (h *handlerForDDLTest) Write(ctx context.Context, infos map[ChangeFeedID]*ChangeFeedInfo) error {
	h.dmlExpectIndex++
	info, exist := infos["test_change_feed"]
	h.c.Assert(exist, check.IsTrue)
	h.currentGlobalResolvedTS = info.ResolvedTS()
	h.c.Assert(info.ResolvedTS(), check.Equals, h.expectResolvedTS[h.dmlExpectIndex])
	h.c.Assert(info.Status(), check.Equals, h.expectStatus[h.dmlExpectIndex])
	if h.dmlExpectIndex >= len(h.expectResolvedTS)-1 {
		log.Info("cancel")
		h.cancel()
	}
	return nil
}

func (s *ownerSuite) TestDDL(c *check.C) {

	changeFeedInfos := map[ChangeFeedID]*ChangeFeedInfo{
		"test_change_feed": {
			status: ChangeFeedSyncDML,
			processorInfos: ProcessorsInfos{
				"capture_1": {},
				"capture_2": {},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())

	handler := &handlerForDDLTest{
		ddlIndex:      -1,
		ddlResolvedTS: []uint64{5, 8, 49, 91, 113},
		ddlJobs: []*model.Job{
			{
				ID: 1,
				BinlogInfo: &model.HistoryInfo{
					FinishedTS: 3,
				},
			},
			{
				ID: 2,
				BinlogInfo: &model.HistoryInfo{
					FinishedTS: 7,
				},
			},
			{
				ID: 3,
				BinlogInfo: &model.HistoryInfo{
					FinishedTS: 11,
				},
			},
			{
				ID: 4,
				BinlogInfo: &model.HistoryInfo{
					FinishedTS: 89,
				},
			},
			{
				ID: 5,
				BinlogInfo: &model.HistoryInfo{
					FinishedTS: 111,
				},
			},
		},

		ddlExpectIndex: -1,

		dmlIndex:                -1,
		resolvedTS1:             []uint64{10, 22, 64, 92, 99, 120},
		resolvedTS2:             []uint64{8, 36, 53, 88, 103, 108},
		currentGlobalResolvedTS: 0,

		dmlExpectIndex: -1,
		expectResolvedTS: []uint64{
			3, 3,
			7, 7,
			11, 11,
			89, 89,
			100},
		expectStatus: []ChangeFeedStatus{
			ChangeFeedWaitToExecDDL, ChangeFeedExecDDL,
			ChangeFeedWaitToExecDDL, ChangeFeedExecDDL,
			ChangeFeedWaitToExecDDL, ChangeFeedExecDDL,
			ChangeFeedWaitToExecDDL, ChangeFeedExecDDL,
			ChangeFeedSyncDML},

		cancel: cancel,
		c:      c,
	}

	manager := NewMockManager(uuid.New().String(), cancel)
	err := manager.CampaignOwner(ctx)
	c.Assert(err, check.IsNil)
	owner := &ownerImpl{
		changeFeedInfos: changeFeedInfos,
		targetTS:        100,

		ddlHandler: handler,
		cfRWriter:  handler,
		manager:    manager,
	}
	s.owner = owner
	err = owner.Run(ctx, 50*time.Millisecond)
	c.Assert(errors.Cause(err), check.DeepEquals, context.Canceled)

}
