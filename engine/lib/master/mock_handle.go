// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package master

import (
	"context"

	derror "github.com/pingcap/tiflow/engine/pkg/errors"

	"go.uber.org/atomic"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

type MockHandle struct {
	WorkerID      libModel.WorkerID
	WorkerStatus  *libModel.WorkerStatus
	ExecutorID    model.ExecutorID
	MessageSender *p2p.MockMessageSender
	IsTombstone   bool

	sendMessageCount atomic.Int64
}

func (h *MockHandle) GetTombstone() TombstoneHandle {
	if h.IsTombstone {
		return h
	}
	return nil
}

func (h *MockHandle) Unwrap() RunningHandle {
	if !h.IsTombstone {
		return h
	}
	return nil
}

func (h *MockHandle) Status() *libModel.WorkerStatus {
	return h.WorkerStatus
}

func (h *MockHandle) ID() libModel.WorkerID {
	return h.WorkerID
}

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

func (h *MockHandle) CleanTombstone(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (h *MockHandle) SendMessageCount() int {
	return int(h.sendMessageCount.Load())
}
