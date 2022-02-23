package lib

// This file provides helper function to let the implementation of WorkerImpl
// can finish its unit tests.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

func MockBaseWorker(
	workerID WorkerID,
	masterID MasterID,
	workerImpl WorkerImpl,
) *DefaultBaseWorker {
	ret := NewBaseWorker(
		workerImpl,
		p2p.NewMockMessageHandlerManager(),
		p2p.NewMockMessageSender(),
		metadata.NewMetaMock(),
		workerID,
		masterID)
	return ret.(*DefaultBaseWorker)
}

func MockBaseWorkerCheckSendMessage(
	t *testing.T,
	worker *DefaultBaseWorker,
	topic p2p.Topic,
	message interface{}) {
	masterNode := worker.masterClient.MasterNode()
	got, ok := worker.messageSender.(*p2p.MockMessageSender).TryPop(masterNode, topic)
	require.True(t, ok)
	require.Equal(t, message, got)
}
