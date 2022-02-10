package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestJobManagerSubmitJob(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockMaster := lib.NewMockMasterImpl("", "submit-job-test")
	mockMaster.On("InitImpl", mock.Anything).Return(nil)
	mockMaster.MasterClient().On(
		"ScheduleTask", mock.Anything, mock.Anything, mock.Anything).Return(
		&pb.TaskSchedulerResponse{}, errors.ErrClusterResourceNotEnough.FastGenByArgs(),
	)
	mgr := &JobManagerImplV2{
		BaseMaster: mockMaster.DefaultBaseMaster,
	}
	// set master impl to JobManagerImplV2
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_Benchmark,
		Config: []byte(""),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)
	time.Sleep(time.Millisecond * 10)
	require.Eventually(t, func() bool {
		mgr.workerMu.Lock()
		defer mgr.workerMu.Unlock()
		return len(mgr.workers) == 0
	}, time.Second*2, time.Millisecond*20)
}
