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

type mockTableExecutor struct {
	mock.Mock
}

func (e *mockTableExecutor) AddTable(ctx cdcContext.Context, tableID model.TableID, boundaryTs model.Ts) error {
	args := e.Called(ctx, tableID, boundaryTs)
	return args.Error(0)
}

func (e *mockTableExecutor) RemoveTable(ctx cdcContext.Context, tableID model.TableID, boundaryTs model.Ts) (bool, error) {
	args := e.Called(ctx, tableID, boundaryTs)
	return args.Bool(0), args.Error(1)
}

func (e *mockTableExecutor) IsAddTableFinished(ctx cdcContext.Context, tableID model.TableID) (bool, error) {
	args := e.Called(ctx, tableID)
	return args.Bool(0), args.Error(1)
}

func (e *mockTableExecutor) IsRemoveTableFinished(ctx cdcContext.Context, tableID model.TableID) (bool, error) {
	args := e.Called(ctx, tableID)
	return args.Bool(0), args.Error(1)
}

func (e *mockTableExecutor) GetAllCurrentTables() []model.TableID {
	args := e.Called()
	return args.Get(0).([]model.TableID)
}

func (e *mockTableExecutor) GetCheckpoint() (checkpointTs, resolvedTs model.Ts) {
	args := e.Called()
	return args.Get(0).(model.Ts), args.Get(1).(model.Ts)
}
