//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

// Code generated by mockery v2.13.1. DO NOT EDIT.

package writer

import (
	context "context"

	model "github.com/pingcap/tiflow/cdc/model"
	mock "github.com/stretchr/testify/mock"
)

// MockRedoLogWriter is an autogenerated mock type for the RedoLogWriter type
type MockRedoLogWriter struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *MockRedoLogWriter) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteAllLogs provides a mock function with given fields: ctx
func (_m *MockRedoLogWriter) DeleteAllLogs(ctx context.Context) error {
	ret := _m.Called(ctx)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// EmitCheckpointTs provides a mock function with given fields: ctx, ts
func (_m *MockRedoLogWriter) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	ret := _m.Called(ctx, ts)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uint64) error); ok {
		r0 = rf(ctx, ts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// FlushLog provides a mock function with given fields: ctx, rtsMap
func (_m *MockRedoLogWriter) FlushLog(ctx context.Context, rtsMap map[int64]uint64) error {
	ret := _m.Called(ctx, rtsMap)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, map[int64]uint64) error); ok {
		r0 = rf(ctx, rtsMap)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendDDL provides a mock function with given fields: ctx, ddl
func (_m *MockRedoLogWriter) SendDDL(ctx context.Context, ddl *model.RedoDDLEvent) error {
	ret := _m.Called(ctx, ddl)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, *model.RedoDDLEvent) error); ok {
		r0 = rf(ctx, ddl)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// WriteLog provides a mock function with given fields: ctx, tableID, rows
func (_m *MockRedoLogWriter) WriteLog(ctx context.Context, tableID int64, rows []*model.RedoRowChangedEvent) (uint64, error) {
	ret := _m.Called(ctx, tableID, rows)

	var r0 uint64
	if rf, ok := ret.Get(0).(func(context.Context, int64, []*model.RedoRowChangedEvent) uint64); ok {
		r0 = rf(ctx, tableID, rows)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, int64, []*model.RedoRowChangedEvent) error); ok {
		r1 = rf(ctx, tableID, rows)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewMockRedoLogWriter interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockRedoLogWriter creates a new instance of MockRedoLogWriter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockRedoLogWriter(t mockConstructorTestingTNewMockRedoLogWriter) *MockRedoLogWriter {
	mock := &MockRedoLogWriter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
