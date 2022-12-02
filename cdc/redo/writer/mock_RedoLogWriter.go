// Code generated by mockery v2.14.1. DO NOT EDIT.

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

// FlushLog provides a mock function with given fields: ctx, checkpointTs, resolvedTs
func (_m *MockRedoLogWriter) FlushLog(ctx context.Context, checkpointTs uint64, resolvedTs uint64) error {
	ret := _m.Called(ctx, checkpointTs, resolvedTs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uint64, uint64) error); ok {
		r0 = rf(ctx, checkpointTs, resolvedTs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GC provides a mock function with given fields: ctx, checkpointTs
func (_m *MockRedoLogWriter) GC(ctx context.Context, checkpointTs uint64) error {
	ret := _m.Called(ctx, checkpointTs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uint64) error); ok {
		r0 = rf(ctx, checkpointTs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// GetMeta provides a mock function with given fields:
func (_m *MockRedoLogWriter) GetMeta() (uint64, uint64) {
	ret := _m.Called()

	var r0 uint64
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	var r1 uint64
	if rf, ok := ret.Get(1).(func() uint64); ok {
		r1 = rf()
	} else {
		r1 = ret.Get(1).(uint64)
	}

	return r0, r1
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

// WriteLog provides a mock function with given fields: ctx, rows
func (_m *MockRedoLogWriter) WriteLog(ctx context.Context, rows []*model.RedoRowChangedEvent) error {
	ret := _m.Called(ctx, rows)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []*model.RedoRowChangedEvent) error); ok {
		r0 = rf(ctx, rows)
	} else {
		r0 = ret.Error(0)
	}

	return r0
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
