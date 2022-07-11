// Code generated by MockGen. DO NOT EDIT.
// Source: capture.go

// Package mock_v1 is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	model "github.com/pingcap/tiflow/cdc/model"
	v1 "github.com/pingcap/tiflow/pkg/api/v1"
)

// MockCapturesGetter is a mock of CapturesGetter interface.
type MockCapturesGetter struct {
	ctrl     *gomock.Controller
	recorder *MockCapturesGetterMockRecorder
}

// MockCapturesGetterMockRecorder is the mock recorder for MockCapturesGetter.
type MockCapturesGetterMockRecorder struct {
	mock *MockCapturesGetter
}

// NewMockCapturesGetter creates a new mock instance.
func NewMockCapturesGetter(ctrl *gomock.Controller) *MockCapturesGetter {
	mock := &MockCapturesGetter{ctrl: ctrl}
	mock.recorder = &MockCapturesGetterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCapturesGetter) EXPECT() *MockCapturesGetterMockRecorder {
	return m.recorder
}

// Captures mocks base method.
func (m *MockCapturesGetter) Captures() v1.CaptureInterface {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Captures")
	ret0, _ := ret[0].(v1.CaptureInterface)
	return ret0
}

// Captures indicates an expected call of Captures.
func (mr *MockCapturesGetterMockRecorder) Captures() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Captures",
		reflect.TypeOf((*MockCapturesGetter)(nil).Captures))
}

// MockCaptureInterface is a mock of CaptureInterface interface.
type MockCaptureInterface struct {
	ctrl     *gomock.Controller
	recorder *MockCaptureInterfaceMockRecorder
}

// MockCaptureInterfaceMockRecorder is the mock recorder for MockCaptureInterface.
type MockCaptureInterfaceMockRecorder struct {
	mock *MockCaptureInterface
}

// NewMockCaptureInterface creates a new mock instance.
func NewMockCaptureInterface(ctrl *gomock.Controller) *MockCaptureInterface {
	mock := &MockCaptureInterface{ctrl: ctrl}
	mock.recorder = &MockCaptureInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCaptureInterface) EXPECT() *MockCaptureInterfaceMockRecorder {
	return m.recorder
}

// List mocks base method.
func (m *MockCaptureInterface) List(ctx context.Context) (*[]model.Capture, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "List", ctx)
	ret0, _ := ret[0].(*[]model.Capture)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// List indicates an expected call of List.
func (mr *MockCaptureInterfaceMockRecorder) List(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "List",
		reflect.TypeOf((*MockCaptureInterface)(nil).List), ctx)
}
