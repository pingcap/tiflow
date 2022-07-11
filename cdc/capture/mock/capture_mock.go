// Code generated by MockGen. DO NOT EDIT.
// Source: cdc/capture/capture.go

// Package mock_capture is a generated GoMock package.
package mock_capture

import (
	context "context"
	io "io"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	model "github.com/pingcap/tiflow/cdc/model"
	owner "github.com/pingcap/tiflow/cdc/owner"
	etcd "github.com/pingcap/tiflow/pkg/etcd"
	upstream "github.com/pingcap/tiflow/pkg/upstream"
)

// MockCapture is a mock of Capture interface.
type MockCapture struct {
	ctrl     *gomock.Controller
	recorder *MockCaptureMockRecorder
}

// MockCaptureMockRecorder is the mock recorder for MockCapture.
type MockCaptureMockRecorder struct {
	mock *MockCapture
}

// NewMockCapture creates a new mock instance.
func NewMockCapture(ctrl *gomock.Controller) *MockCapture {
	mock := &MockCapture{ctrl: ctrl}
	mock.recorder = &MockCaptureMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCapture) EXPECT() *MockCaptureMockRecorder {
	return m.recorder
}

// AsyncClose mocks base method.
func (m *MockCapture) AsyncClose() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "AsyncClose")
}

// AsyncClose indicates an expected call of AsyncClose.
func (mr *MockCaptureMockRecorder) AsyncClose() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AsyncClose", reflect.TypeOf((*MockCapture)(nil).AsyncClose))
}

// Drain mocks base method.
func (m *MockCapture) Drain(ctx context.Context) <-chan struct{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Drain", ctx)
	ret0, _ := ret[0].(<-chan struct{})
	return ret0
}

// Drain indicates an expected call of Drain.
func (mr *MockCaptureMockRecorder) Drain(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Drain", reflect.TypeOf((*MockCapture)(nil).Drain), ctx)
}

// GetEtcdClient mocks base method.
func (m *MockCapture) GetEtcdClient() etcd.CDCEtcdClientForAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetEtcdClient")
	ret0, _ := ret[0].(etcd.CDCEtcdClientForAPI)
	return ret0
}

// GetEtcdClient indicates an expected call of GetEtcdClient.
func (mr *MockCaptureMockRecorder) GetEtcdClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetEtcdClient", reflect.TypeOf((*MockCapture)(nil).GetEtcdClient))
}

// GetOwner mocks base method.
func (m *MockCapture) GetOwner() (owner.Owner, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOwner")
	ret0, _ := ret[0].(owner.Owner)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOwner indicates an expected call of GetOwner.
func (mr *MockCaptureMockRecorder) GetOwner() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOwner", reflect.TypeOf((*MockCapture)(nil).GetOwner))
}

// GetOwnerCaptureInfo mocks base method.
func (m *MockCapture) GetOwnerCaptureInfo(ctx context.Context) (*model.CaptureInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOwnerCaptureInfo", ctx)
	ret0, _ := ret[0].(*model.CaptureInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOwnerCaptureInfo indicates an expected call of GetOwnerCaptureInfo.
func (mr *MockCaptureMockRecorder) GetOwnerCaptureInfo(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOwnerCaptureInfo", reflect.TypeOf((*MockCapture)(nil).GetOwnerCaptureInfo), ctx)
}

// GetUpstreamManager mocks base method.
func (m *MockCapture) GetUpstreamManager() (*upstream.Manager, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUpstreamManager")
	ret0, _ := ret[0].(*upstream.Manager)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetUpstreamManager indicates an expected call of GetUpstreamManager.
func (mr *MockCaptureMockRecorder) GetUpstreamManager() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUpstreamManager", reflect.TypeOf((*MockCapture)(nil).GetUpstreamManager))
}

// Info mocks base method.
func (m *MockCapture) Info() (model.CaptureInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Info")
	ret0, _ := ret[0].(model.CaptureInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Info indicates an expected call of Info.
func (mr *MockCaptureMockRecorder) Info() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Info", reflect.TypeOf((*MockCapture)(nil).Info))
}

// IsOwner mocks base method.
func (m *MockCapture) IsOwner() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsOwner")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsOwner indicates an expected call of IsOwner.
func (mr *MockCaptureMockRecorder) IsOwner() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsOwner", reflect.TypeOf((*MockCapture)(nil).IsOwner))
}

// IsReady mocks base method.
func (m *MockCapture) IsReady() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsReady")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsReady indicates an expected call of IsReady.
func (mr *MockCaptureMockRecorder) IsReady() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsReady", reflect.TypeOf((*MockCapture)(nil).IsReady))
}

// Liveness mocks base method.
func (m *MockCapture) Liveness() model.Liveness {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Liveness")
	ret0, _ := ret[0].(model.Liveness)
	return ret0
}

// Liveness indicates an expected call of Liveness.
func (mr *MockCaptureMockRecorder) Liveness() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Liveness", reflect.TypeOf((*MockCapture)(nil).Liveness))
}

// Run mocks base method.
func (m *MockCapture) Run(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Run", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Run indicates an expected call of Run.
func (mr *MockCaptureMockRecorder) Run(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Run", reflect.TypeOf((*MockCapture)(nil).Run), ctx)
}

// StatusProvider mocks base method.
func (m *MockCapture) StatusProvider() owner.StatusProvider {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StatusProvider")
	ret0, _ := ret[0].(owner.StatusProvider)
	return ret0
}

// StatusProvider indicates an expected call of StatusProvider.
func (mr *MockCaptureMockRecorder) StatusProvider() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StatusProvider", reflect.TypeOf((*MockCapture)(nil).StatusProvider))
}

// WriteDebugInfo mocks base method.
func (m *MockCapture) WriteDebugInfo(ctx context.Context, w io.Writer) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "WriteDebugInfo", ctx, w)
}

// WriteDebugInfo indicates an expected call of WriteDebugInfo.
func (mr *MockCaptureMockRecorder) WriteDebugInfo(ctx, w interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WriteDebugInfo", reflect.TypeOf((*MockCapture)(nil).WriteDebugInfo), ctx, w)
}
