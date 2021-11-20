// Copyright 2021 PingCAP, Inc.
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

package scheduler

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

type mockScheduleDispatcherCommunicator struct {
	mock.Mock
	addTableRecords    map[model.CaptureID][]model.TableID
	removeTableRecords map[model.CaptureID][]model.TableID
}

func NewMockScheduleDispatcherCommunicator() *mockScheduleDispatcherCommunicator {
	return &mockScheduleDispatcherCommunicator{
		addTableRecords:    map[model.CaptureID][]model.TableID{},
		removeTableRecords: map[model.CaptureID][]model.TableID{},
	}
}

func (m *mockScheduleDispatcherCommunicator) Reset() {
	m.addTableRecords = map[model.CaptureID][]model.TableID{}
	m.removeTableRecords = map[model.CaptureID][]model.TableID{}
	m.Mock.ExpectedCalls = nil
	m.Mock.Calls = nil
}

func (m *mockScheduleDispatcherCommunicator) DispatchTable(
	ctx cdcContext.Context,
	changeFeedID model.ChangeFeedID,
	tableID model.TableID,
	captureID model.CaptureID,
	isDelete bool,
) (done bool, err error) {
	log.Info("dispatch table called",
		zap.String("changefeed-id", changeFeedID),
		zap.Int64("table-id", tableID),
		zap.String("capture-id", captureID),
		zap.Bool("is-delete", isDelete))
	if !isDelete {
		m.addTableRecords[captureID] = append(m.addTableRecords[captureID], tableID)
	} else {
		m.removeTableRecords[captureID] = append(m.removeTableRecords[captureID], tableID)
	}
	args := m.Called(ctx, changeFeedID, tableID, captureID, isDelete)
	return args.Bool(0), args.Error(1)
}

func (m *mockScheduleDispatcherCommunicator) Announce(
	ctx cdcContext.Context,
	changeFeedID model.ChangeFeedID,
	captureID model.CaptureID,
) (done bool, err error) {
	args := m.Called(ctx, changeFeedID, captureID)
	return args.Bool(0), args.Error(1)
}
