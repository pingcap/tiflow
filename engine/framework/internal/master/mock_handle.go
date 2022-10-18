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

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
)

// MockHandle implements WorkerHandle, it can work as either a RunningHandle or
// a TombstoneHandle.
type MockHandle struct {
	WorkerID      frameModel.WorkerID
	WorkerStatus  *frameModel.WorkerStatus
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
		return mockRunningHandle{
			BaseHandle: h,
			handler:    h,
		}
	}
	return nil
}

// Status implements WorkerHandle.Status
func (h *MockHandle) Status() *frameModel.WorkerStatus {
	return h.WorkerStatus
}

// ID implements WorkerHandle.ID
func (h *MockHandle) ID() frameModel.WorkerID {
	return h.WorkerID
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

type mockRunningHandle struct {
	BaseHandle
	handler *MockHandle
}

// SendMessage implements RunningHandle.SendMessage
func (rh mockRunningHandle) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	h := rh.handler
	if h.IsTombstone {
		return errors.ErrSendingMessageToTombstone.GenWithStackByCause(h.WorkerID)
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
