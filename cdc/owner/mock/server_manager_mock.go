// Code generated by MockGen. DO NOT EDIT.
// Source: cdc/owner/server_manager.go

// Package mock_owner is a generated GoMock package.
package mock_owner

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	orchestrator "github.com/pingcap/tiflow/pkg/orchestrator"
)

// MockServerManager is a mock of ServerManager interface.
type MockServerManager struct {
	ctrl     *gomock.Controller
	recorder *MockServerManagerMockRecorder
}

// MockServerManagerMockRecorder is the mock recorder for MockServerManager.
type MockServerManagerMockRecorder struct {
	mock *MockServerManager
}

// NewMockServerManager creates a new mock instance.
func NewMockServerManager(ctrl *gomock.Controller) *MockServerManager {
	mock := &MockServerManager{ctrl: ctrl}
	mock.recorder = &MockServerManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockServerManager) EXPECT() *MockServerManagerMockRecorder {
	return m.recorder
}

// AsyncStop mocks base method.
func (m *MockServerManager) AsyncStop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AsyncStop")
}

// AsyncStop indicates an expected call of AsyncStop.
func (mr *MockServerManagerMockRecorder) AsyncStop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AsyncStop", reflect.TypeOf((*MockServerManager)(nil).AsyncStop))
}

// Tick mocks base method.
func (m *MockServerManager) Tick(ctx context.Context, state orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Tick", ctx, state)
	ret0, _ := ret[0].(orchestrator.ReactorState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Tick indicates an expected call of Tick.
func (mr *MockServerManagerMockRecorder) Tick(ctx, state interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tick", reflect.TypeOf((*MockServerManager)(nil).Tick), ctx, state)
}
