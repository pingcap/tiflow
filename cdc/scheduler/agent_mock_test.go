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
	"github.com/pingcap/ticdc/cdc/model"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/stretchr/testify/mock"
)

type mockProcessorMessenger struct {
	mock.Mock
}

func (m *mockProcessorMessenger) FinishTableOperation(ctx cdcContext.Context, tableID model.TableID) (bool, error) {
	args := m.Called(ctx, tableID)
	return args.Bool(0), args.Error(1)
}

func (m *mockProcessorMessenger) SyncTaskStatuses(ctx cdcContext.Context, running, adding, removing []model.TableID) (bool, error) {
	args := m.Called(ctx, running, adding, removing)
	return args.Bool(0), args.Error(1)
}

func (m *mockProcessorMessenger) SendCheckpoint(ctx cdcContext.Context, checkpointTs model.Ts, resolvedTs model.Ts) (bool, error) {
	args := m.Called(ctx, checkpointTs, resolvedTs)
	return args.Bool(0), args.Error(1)
}

func (m *mockProcessorMessenger) Barrier(ctx cdcContext.Context) (done bool) {
	args := m.Called(ctx)
	return args.Bool(0)
}

func (m *mockProcessorMessenger) OnOwnerChanged(ctx cdcContext.Context, newOwnerCaptureID model.CaptureID) {
	m.Called(ctx, newOwnerCaptureID)
}

func (m *mockProcessorMessenger) Close() error {
	args := m.Called()
	return args.Error(0)
}
