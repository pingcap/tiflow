package roles

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
)

type ownerSuite struct {
	owner Owner
}

var _ = check.Suite(&ownerSuite{})

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
	index := -1
	resolvedTS1 := []uint64{10, 22, 64, 92, 99, 120}
	resolvedTS2 := []uint64{8, 36, 53, 88, 103, 108}
	expectResolvedTS := []uint64{8, 22, 53, 88, 99, 100}

	ctx, cancel := context.WithCancel(context.Background())
	owner := &ownerImpl{
		changeFeedInfos: changeFeedInfos,
		ddlPullFunc: func() (u uint64, jobs []*model.Job, e error) {
			return uint64(math.MaxUint64), nil, nil
		},
		readChangeFeedInfos: func(ctx context.Context) (infos map[ChangeFeedID]ProcessorsInfos, e error) {

			index++
			return map[ChangeFeedID]ProcessorsInfos{
				"test_change_feed": {
					"capture_1": {
						ResolvedTS: resolvedTS1[index],
					},
					"capture_2": {
						ResolvedTS: resolvedTS2[index],
					},
				},
			}, nil
		},
		writeChangeFeedInfos: func(ctx context.Context, infos map[ChangeFeedID]*ChangeFeedInfo) error {
			info, exist := infos["test_change_feed"]
			c.Assert(exist, check.IsTrue)
			c.Assert(info.ResolvedTS(), check.Equals, expectResolvedTS[index])
			c.Assert(info.Status(), check.Equals, ChangeFeedSyncDML)
			if index >= len(expectResolvedTS)-1 {
				log.Info("cancel")
				cancel()
			}
			return nil
		},
		targetTS: 100,
	}
	s.owner = owner
	err := owner.Run(ctx, 50*time.Millisecond)
	c.Assert(err.Error(), check.Equals, "context canceled")

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
	dmlIndex := -1
	ddlIndex := -1
	resolvedTS1 := []uint64{10, 22, 64, 92, 99, 120}
	resolvedTS2 := []uint64{8, 36, 53, 88, 103, 108}
	ddlResolvedTS := []uint64{5, 8, 49, 91, 113}

	ddlJobs := []model.Job{
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
	}
	currentGlobalResolvedTS := uint64(0)
	expectResolvedTS := []uint64{
		3, 3, 3,
		7, 7, 7,
		11, 11, 11,
		89, 89, 89,
		100}
	expectStatus := []ChangeFeedStatus{
		ChangeFeedWaitToExecDDL, ChangeFeedExecDDL, ChangeFeedSyncDML,
		ChangeFeedWaitToExecDDL, ChangeFeedExecDDL, ChangeFeedSyncDML,
		ChangeFeedWaitToExecDDL, ChangeFeedExecDDL, ChangeFeedSyncDML,
		ChangeFeedWaitToExecDDL, ChangeFeedExecDDL, ChangeFeedSyncDML,
		ChangeFeedSyncDML}
	expectIndex := -1

	ctx, cancel := context.WithCancel(context.Background())
	owner := &ownerImpl{
		changeFeedInfos: changeFeedInfos,
		ddlPullFunc: func() (u uint64, jobs []*model.Job, e error) {
			if ddlIndex < len(ddlJobs)-1 {
				ddlIndex++
			}
			return ddlResolvedTS[ddlIndex], []*model.Job{&ddlJobs[ddlIndex]}, nil
		},
		execDDLFunc: func(job *model.Job) error {
			return nil
		},
		readChangeFeedInfos: func(ctx context.Context) (infos map[ChangeFeedID]ProcessorsInfos, e error) {
			if dmlIndex < len(resolvedTS1)-1 {
				dmlIndex++
			}
			return map[ChangeFeedID]ProcessorsInfos{
				"test_change_feed": {
					"capture_1": {
						ResolvedTS:   resolvedTS1[dmlIndex],
						CheckPointTS: currentGlobalResolvedTS,
					},
					"capture_2": {
						ResolvedTS:   resolvedTS2[dmlIndex],
						CheckPointTS: currentGlobalResolvedTS,
					},
				},
			}, nil
		},
		writeChangeFeedInfos: func(ctx context.Context, infos map[ChangeFeedID]*ChangeFeedInfo) error {
			expectIndex++
			info, exist := infos["test_change_feed"]
			c.Assert(exist, check.IsTrue)
			currentGlobalResolvedTS = info.ResolvedTS()
			c.Assert(info.ResolvedTS(), check.Equals, expectResolvedTS[expectIndex])
			c.Assert(info.Status(), check.Equals, expectStatus[expectIndex])
			if expectIndex >= len(expectResolvedTS)-1 {
				log.Info("cancel")
				cancel()
			}
			return nil
		},
		targetTS:       100,
		finishedDDLJob: make(chan ddlExecResult),
	}
	s.owner = owner
	err := owner.Run(ctx, 50*time.Millisecond)
	c.Assert(err.Error(), check.Equals, "context canceled")

}
