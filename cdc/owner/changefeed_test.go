package owner

import (
	"context"
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/orchestrator"
)

type mockDDLPuller struct {
	DDLPuller
}

type mockAsyncSink struct {
	AsyncSink
}

var _ = check.Suite(&changefeedSuite{})

type changefeedSuite struct {
}

func createChangefeed4Test(ctx cdcContext.Context, c *check.C) (*changefeed, *model.ChangefeedReactorState,
	map[model.CaptureID]*model.CaptureInfo, *orchestrator.ReactorStateTester) {
	ctx.GlobalVars().PDClient = &mockPDClient{updateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		return safePoint, nil
	}}
	gcManager := newGCManager()
	cf := newChangefeed4Test(gcManager, func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{}, nil
	}, func(ctx cdcContext.Context) (AsyncSink, error) {
		return &mockAsyncSink{}, nil
	})
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		info = ctx.ChangefeedVars().Info
		return info, true, nil
	})
	tester.MustApplyPatches()
	captures := map[model.CaptureID]*model.CaptureInfo{ctx.GlobalVars().CaptureInfo.ID: ctx.GlobalVars().CaptureInfo}
	return cf, state, captures, tester
}

func (s *changefeedSuite) TestChangefeed(c *check.C) {
	ctx := cdcContext.NewBackendContext4Test(true)
	cf, state, captures, tester := createChangefeed4Test(ctx, c)
	cf.Tick(ctx, state, captures)
	tester.MustApplyPatches()
}
