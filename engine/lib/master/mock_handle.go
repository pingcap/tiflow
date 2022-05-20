package master

import (
	"context"

	derror "github.com/hanfei1991/microcosm/pkg/errors"

	"go.uber.org/atomic"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// MockHandle implements WorkerHandle, it can work as either a RunningHandle or
// a TombstoneHandle.
type MockHandle struct {
	WorkerID      libModel.WorkerID
	WorkerStatus  *libModel.WorkerStatus
	ExecutorID    model.ExecutorID
	MessageSender *p2p.MockMessageSender
	IsTombstone   bool

	sendMessageCount atomic.Int64
}

// GetTombstone implements WorkerHandle.GetTombstone
func (h *MockHandle) GetTombstone() TombstoneHandle {
	if h.IsTombstone {
		return h
	}
	return nil
}

// Unwrap implements WorkerHandle.Unwrap
func (h *MockHandle) Unwrap() RunningHandle {
	if !h.IsTombstone {
		return h
	}
	return nil
}

// Status implements WorkerHandle.Status
func (h *MockHandle) Status() *libModel.WorkerStatus {
	return h.WorkerStatus
}

// ID implements WorkerHandle.ID
func (h *MockHandle) ID() libModel.WorkerID {
	return h.WorkerID
}

// ToPB implements WorkerHandle.ToPB
func (h *MockHandle) ToPB() (*pb.WorkerInfo, error) {
	statusBytes, err := h.Status().Marshal()
	if err != nil {
		return nil, err
	}

	ret := &pb.WorkerInfo{
		Id:         h.WorkerID,
		ExecutorId: string(h.ExecutorID),
		Status:     statusBytes,
	}
	return ret, nil
}

// SendMessage implements RunningHandle.SendMessage
func (h *MockHandle) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	if h.IsTombstone {
		return derror.ErrSendingMessageToTombstone.GenWithStackByCause(h.WorkerID)
	}

	h.sendMessageCount.Add(1)
	if h.MessageSender == nil {
		return nil
	}

	var err error
	if nonblocking {
		_, err = h.MessageSender.SendToNode(ctx, p2p.NodeID(h.ExecutorID), topic, message)
	} else {
		err = h.MessageSender.SendToNodeB(ctx, p2p.NodeID(h.ExecutorID), topic, message)
	}
	return err
}

// CleanTombstone implements TombstoneHandle.CleanTombstone
func (h *MockHandle) CleanTombstone(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

// SendMessageCount returns the send message count, used in unit test only.
func (h *MockHandle) SendMessageCount() int {
	return int(h.sendMessageCount.Load())
}
