package statusutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/lib/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type writerTestSuite struct {
	writer        *Writer
	kv            *mock.MetaMock
	messageSender *p2p.MockMessageSender
	masterInfo    *MockMasterInfoProvider
}

func newWriterTestSuite(masterID MasterID, masterNode p2p.NodeID, masterEpoch Epoch, workerID WorkerID) *writerTestSuite {
	kv := mock.NewMetaMock()
	messageSender := p2p.NewMockMessageSender()
	masterInfo := &MockMasterInfoProvider{
		masterID:   masterID,
		masterNode: masterNode,
		epoch:      masterEpoch,
	}
	return &writerTestSuite{
		writer:        NewWriter(kv, messageSender, masterInfo, workerID),
		kv:            kv,
		messageSender: messageSender,
		masterInfo:    masterInfo,
	}
}

func TestWriterUpdate(t *testing.T) {
	suite := newWriterTestSuite("master-1", "executor-1", 1, "worker-1")
	ctx := context.Background()

	st := &model.WorkerStatus{
		Code:         model.WorkerStatusNormal,
		ErrorMessage: "test",
	}
	err := suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)

	rawBytes, err := st.Marshal()
	require.NoError(t, err)

	resp, err := suite.kv.Get(ctx, model.EncodeWorkerStatusKey("master-1", "worker-1"))
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, rawBytes, resp.Kvs[0].Value)

	rawMsg, ok := suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg := rawMsg.(*WorkerStatusMessage)
	require.Equal(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)

	// Deletes the persisted status for testing purpose.
	// TODO make a better mock KV that can inspect calls.
	_, err = suite.kv.Delete(ctx, model.EncodeWorkerStatusKey("master-1", "worker-1"))
	require.NoError(t, err)

	// Repeated update. Should have a notification too, but no persistence.
	err = suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)
	_, ok = suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg = rawMsg.(*WorkerStatusMessage)
	require.Equal(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)
	resp, err = suite.kv.Get(ctx, model.EncodeWorkerStatusKey("master-1", "worker-1"))
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 0)
}

func TestWriterSendRetry(t *testing.T) {
	suite := newWriterTestSuite("master-1", "executor-1", 1, "worker-1")
	ctx := context.Background()

	st := &model.WorkerStatus{
		Code:         model.WorkerStatusNormal,
		ErrorMessage: "test",
	}

	suite.messageSender.InjectError(derror.ErrExecutorNotFoundForMessage.GenWithStackByArgs())
	err := suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)

	rawMsg, ok := suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg := rawMsg.(*WorkerStatusMessage)
	require.Equal(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)
}
