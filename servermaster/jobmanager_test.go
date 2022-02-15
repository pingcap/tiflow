package servermaster

import (
	"context"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/uuid"
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
		jobFsm:     NewJobFsm(),
		uuidGen:    uuid.NewGenerator(),
	}
	// set master impl to JobManagerImplV2
	mockMaster.Impl = mgr
	err := mockMaster.Init(ctx)
	require.Nil(t, err)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_CVSDemo,
		Config: []byte("{\"srcHost\":\"0.0.0.0:1234\", \"dstHost\":\"0.0.0.0:1234\", \"srcDir\":\"data\", \"dstDir\":\"data1\"}"),
	}
	resp := mgr.SubmitJob(ctx, req)
	require.Nil(t, resp.Err)
	time.Sleep(time.Millisecond * 10)
	require.Eventually(t, func() bool {
		return mgr.jobFsm.OnlineJobCount() == 0 &&
			mgr.jobFsm.WaitAckJobCount() == 0 &&
			mgr.jobFsm.PendingJobCount() == 1
	}, time.Second*2, time.Millisecond*20)
}
