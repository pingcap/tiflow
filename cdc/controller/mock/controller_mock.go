// Code generated by MockGen. DO NOT EDIT.
// Source: cdc/controller/controller.go

// Package mock_controller is a generated GoMock package.
package mock_controller

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	model "github.com/pingcap/tiflow/cdc/model"
	orchestrator "github.com/pingcap/tiflow/pkg/orchestrator"
)

// MockController is a mock of Controller interface.
type MockController struct {
	ctrl     *gomock.Controller
	recorder *MockControllerMockRecorder
}

// MockControllerMockRecorder is the mock recorder for MockController.
type MockControllerMockRecorder struct {
	mock *MockController
}

// NewMockController creates a new mock instance.
func NewMockController(ctrl *gomock.Controller) *MockController {
	mock := &MockController{ctrl: ctrl}
	mock.recorder = &MockControllerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockController) EXPECT() *MockControllerMockRecorder {
	return m.recorder
}

// AsyncStop mocks base method.
func (m *MockController) AsyncStop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AsyncStop")
}

// AsyncStop indicates an expected call of AsyncStop.
func (mr *MockControllerMockRecorder) AsyncStop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AsyncStop", reflect.TypeOf((*MockController)(nil).AsyncStop))
}

// GetAllChangeFeedCheckpointTs mocks base method.
func (m *MockController) GetAllChangeFeedCheckpointTs(ctx context.Context) (map[model.ChangeFeedID]uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllChangeFeedCheckpointTs", ctx)
	ret0, _ := ret[0].(map[model.ChangeFeedID]uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllChangeFeedCheckpointTs indicates an expected call of GetAllChangeFeedCheckpointTs.
func (mr *MockControllerMockRecorder) GetAllChangeFeedCheckpointTs(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllChangeFeedCheckpointTs", reflect.TypeOf((*MockController)(nil).GetAllChangeFeedCheckpointTs), ctx)
}

// GetAllChangeFeedInfo mocks base method.
func (m *MockController) GetAllChangeFeedInfo(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllChangeFeedInfo", ctx)
	ret0, _ := ret[0].(map[model.ChangeFeedID]*model.ChangeFeedInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllChangeFeedInfo indicates an expected call of GetAllChangeFeedInfo.
func (mr *MockControllerMockRecorder) GetAllChangeFeedInfo(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllChangeFeedInfo", reflect.TypeOf((*MockController)(nil).GetAllChangeFeedInfo), ctx)
}

// GetCaptures mocks base method.
func (m *MockController) GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCaptures", ctx)
	ret0, _ := ret[0].([]*model.CaptureInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCaptures indicates an expected call of GetCaptures.
func (mr *MockControllerMockRecorder) GetCaptures(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCaptures", reflect.TypeOf((*MockController)(nil).GetCaptures), ctx)
}

// GetChangefeedOwnerCaptureInfo mocks base method.
func (m *MockController) GetChangefeedOwnerCaptureInfo(id model.ChangeFeedID) *model.CaptureInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetChangefeedOwnerCaptureInfo", id)
	ret0, _ := ret[0].(*model.CaptureInfo)
	return ret0
}

// GetChangefeedOwnerCaptureInfo indicates an expected call of GetChangefeedOwnerCaptureInfo.
func (mr *MockControllerMockRecorder) GetChangefeedOwnerCaptureInfo(id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetChangefeedOwnerCaptureInfo", reflect.TypeOf((*MockController)(nil).GetChangefeedOwnerCaptureInfo), id)
}

// Tick mocks base method.
func (m *MockController) Tick(ctx context.Context, state orchestrator.ReactorState) (orchestrator.ReactorState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Tick", ctx, state)
	ret0, _ := ret[0].(orchestrator.ReactorState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Tick indicates an expected call of Tick.
func (mr *MockControllerMockRecorder) Tick(ctx, state interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tick", reflect.TypeOf((*MockController)(nil).Tick), ctx, state)
}
