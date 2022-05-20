package statusutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type writerTestSuite struct {
	writer        *Writer
	cli           pkgOrm.Client
	messageSender *p2p.MockMessageSender
	masterInfo    *MockMasterInfoProvider
}

func newWriterTestSuite(
	t *testing.T,
	masterID libModel.MasterID,
	masterNode p2p.NodeID,
	masterEpoch libModel.Epoch,
	workerID libModel.WorkerID,
) *writerTestSuite {
	cli, err := pkgOrm.NewMockClient()
	require.NoError(t, err)

	messageSender := p2p.NewMockMessageSender()
	masterInfo := &MockMasterInfoProvider{
		masterID:   masterID,
		masterNode: masterNode,
		epoch:      masterEpoch,
	}
	return &writerTestSuite{
		writer:        NewWriter(cli, messageSender, masterInfo, workerID),
		cli:           cli,
		messageSender: messageSender,
		masterInfo:    masterInfo,
	}
}

func TestWriterUpdate(t *testing.T) {
	suite := newWriterTestSuite(t, "master-1", "executor-1", 1, "worker-1")
	ctx := context.Background()

	st := &libModel.WorkerStatus{
		JobID:        "master-1",
		ID:           "worker-1",
		Code:         libModel.WorkerStatusNormal,
		ErrorMessage: "test",
	}

	err := suite.cli.UpsertWorker(ctx, st)
	require.NoError(t, err)

	st, err = suite.cli.GetWorkerByID(ctx, st.JobID, st.ID)
	require.NoError(t, err)

	err = suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)

	status, err := suite.cli.GetWorkerByID(ctx, st.JobID, st.ID)
	require.NoError(t, err)
	require.Equal(t, status.Code, libModel.WorkerStatusNormal)
	require.Equal(t, status.ErrorMessage, "test")

	rawMsg, ok := suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg := rawMsg.(*WorkerStatusMessage)
	checkWorkerStatusMsg(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)

	// Deletes the persisted status for testing purpose.
	// TODO make a better mock KV that can inspect calls.
	_, err = suite.cli.DeleteWorker(ctx, st.JobID, st.ID)
	require.NoError(t, err)

	// Repeated update. Should have a notification too, but no persistence.
	err = suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)
	_, ok = suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg = rawMsg.(*WorkerStatusMessage)
	checkWorkerStatusMsg(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)
	_, err = suite.cli.GetWorkerByID(ctx, st.JobID, st.ID)
	require.Error(t, err)
}

func TestWriterSendRetry(t *testing.T) {
	suite := newWriterTestSuite(t, "master-1", "executor-1", 1, "worker-1")
	ctx := context.Background()

	st := &libModel.WorkerStatus{
		JobID:        "master-1",
		ID:           "worker-1",
		Code:         libModel.WorkerStatusNormal,
		ErrorMessage: "test",
	}
	err := suite.cli.UpsertWorker(ctx, st)
	require.NoError(t, err)

	st, err = suite.cli.GetWorkerByID(ctx, st.JobID, st.ID)
	require.NoError(t, err)

	require.Equal(t, 0, suite.masterInfo.RefreshCount())
	suite.messageSender.InjectError(derror.ErrExecutorNotFoundForMessage.GenWithStackByArgs())
	err = suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)
	require.Equal(t, 1, suite.masterInfo.RefreshCount())

	rawMsg, ok := suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg := rawMsg.(*WorkerStatusMessage)
	checkWorkerStatusMsg(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)
}

func checkWorkerStatusMsg(t *testing.T, expect, msg *WorkerStatusMessage) {
	require.Equal(t, expect.Worker, msg.Worker)
	require.Equal(t, expect.MasterEpoch, msg.MasterEpoch)
	require.Equal(t, expect.Status.Code, expect.Status.Code)
	require.Equal(t, expect.Status.ErrorMessage, expect.Status.ErrorMessage)
	require.Equal(t, expect.Status.ExtBytes, expect.Status.ExtBytes)
}
