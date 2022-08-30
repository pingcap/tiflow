// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/tiflow/engine/pkg/client (interfaces: ExecutorClient,ServerMasterClient)

// Package client is a generated GoMock package.
package client

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	enginepb "github.com/pingcap/tiflow/engine/enginepb"
	model "github.com/pingcap/tiflow/engine/model"
)

// MockExecutorClient is a mock of ExecutorClient interface.
type MockExecutorClient struct {
	ctrl     *gomock.Controller
	recorder *MockExecutorClientMockRecorder
}

// MockExecutorClientMockRecorder is the mock recorder for MockExecutorClient.
type MockExecutorClientMockRecorder struct {
	mock *MockExecutorClient
}

// NewMockExecutorClient creates a new mock instance.
func NewMockExecutorClient(ctrl *gomock.Controller) *MockExecutorClient {
	mock := &MockExecutorClient{ctrl: ctrl}
	mock.recorder = &MockExecutorClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockExecutorClient) EXPECT() *MockExecutorClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockExecutorClient) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockExecutorClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockExecutorClient)(nil).Close))
}

// DispatchTask mocks base method.
func (m *MockExecutorClient) DispatchTask(arg0 context.Context, arg1 *DispatchTaskArgs, arg2 func(), arg3 func(error)) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DispatchTask", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// DispatchTask indicates an expected call of DispatchTask.
func (mr *MockExecutorClientMockRecorder) DispatchTask(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DispatchTask", reflect.TypeOf((*MockExecutorClient)(nil).DispatchTask), arg0, arg1, arg2, arg3)
}

// RemoveResource mocks base method.
func (m *MockExecutorClient) RemoveResource(arg0 context.Context, arg1, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveResource", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveResource indicates an expected call of RemoveResource.
func (mr *MockExecutorClientMockRecorder) RemoveResource(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveResource", reflect.TypeOf((*MockExecutorClient)(nil).RemoveResource), arg0, arg1, arg2)
}

// MockServerMasterClient is a mock of ServerMasterClient interface.
type MockServerMasterClient struct {
	ctrl     *gomock.Controller
	recorder *MockServerMasterClientMockRecorder
}

// MockServerMasterClientMockRecorder is the mock recorder for MockServerMasterClient.
type MockServerMasterClientMockRecorder struct {
	mock *MockServerMasterClient
}

// NewMockServerMasterClient creates a new mock instance.
func NewMockServerMasterClient(ctrl *gomock.Controller) *MockServerMasterClient {
	mock := &MockServerMasterClient{ctrl: ctrl}
	mock.recorder = &MockServerMasterClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockServerMasterClient) EXPECT() *MockServerMasterClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockServerMasterClient) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockServerMasterClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockServerMasterClient)(nil).Close))
}

// CreateResource mocks base method.
func (m *MockServerMasterClient) CreateResource(arg0 context.Context, arg1 *enginepb.CreateResourceRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateResource", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateResource indicates an expected call of CreateResource.
func (mr *MockServerMasterClientMockRecorder) CreateResource(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateResource", reflect.TypeOf((*MockServerMasterClient)(nil).CreateResource), arg0, arg1)
}

// Heartbeat mocks base method.
func (m *MockServerMasterClient) Heartbeat(arg0 context.Context, arg1 *enginepb.HeartbeatRequest) (*enginepb.HeartbeatResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Heartbeat", arg0, arg1)
	ret0, _ := ret[0].(*enginepb.HeartbeatResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Heartbeat indicates an expected call of Heartbeat.
func (mr *MockServerMasterClientMockRecorder) Heartbeat(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Heartbeat", reflect.TypeOf((*MockServerMasterClient)(nil).Heartbeat), arg0, arg1)
}

// ListExecutors mocks base method.
func (m *MockServerMasterClient) ListExecutors(arg0 context.Context) ([]*enginepb.Executor, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListExecutors", arg0)
	ret0, _ := ret[0].([]*enginepb.Executor)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListExecutors indicates an expected call of ListExecutors.
func (mr *MockServerMasterClientMockRecorder) ListExecutors(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListExecutors", reflect.TypeOf((*MockServerMasterClient)(nil).ListExecutors), arg0)
}

// ListMasters mocks base method.
func (m *MockServerMasterClient) ListMasters(arg0 context.Context) ([]*enginepb.Master, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListMasters", arg0)
	ret0, _ := ret[0].([]*enginepb.Master)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListMasters indicates an expected call of ListMasters.
func (mr *MockServerMasterClientMockRecorder) ListMasters(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListMasters", reflect.TypeOf((*MockServerMasterClient)(nil).ListMasters), arg0)
}

// QueryMetaStore mocks base method.
func (m *MockServerMasterClient) QueryMetaStore(arg0 context.Context, arg1 *enginepb.QueryMetaStoreRequest) (*enginepb.QueryMetaStoreResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryMetaStore", arg0, arg1)
	ret0, _ := ret[0].(*enginepb.QueryMetaStoreResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryMetaStore indicates an expected call of QueryMetaStore.
func (mr *MockServerMasterClientMockRecorder) QueryMetaStore(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryMetaStore", reflect.TypeOf((*MockServerMasterClient)(nil).QueryMetaStore), arg0, arg1)
}

// QueryResource mocks base method.
func (m *MockServerMasterClient) QueryResource(arg0 context.Context, arg1 *enginepb.QueryResourceRequest) (*enginepb.QueryResourceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryResource", arg0, arg1)
	ret0, _ := ret[0].(*enginepb.QueryResourceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryResource indicates an expected call of QueryResource.
func (mr *MockServerMasterClientMockRecorder) QueryResource(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryResource", reflect.TypeOf((*MockServerMasterClient)(nil).QueryResource), arg0, arg1)
}

// RegisterExecutor mocks base method.
func (m *MockServerMasterClient) RegisterExecutor(arg0 context.Context, arg1 *enginepb.RegisterExecutorRequest) (model.DeployNodeID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterExecutor", arg0, arg1)
	ret0, _ := ret[0].(model.DeployNodeID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterExecutor indicates an expected call of RegisterExecutor.
func (mr *MockServerMasterClientMockRecorder) RegisterExecutor(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterExecutor", reflect.TypeOf((*MockServerMasterClient)(nil).RegisterExecutor), arg0, arg1)
}

// RegisterMetaStore mocks base method.
func (m *MockServerMasterClient) RegisterMetaStore(arg0 context.Context, arg1 *enginepb.RegisterMetaStoreRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterMetaStore", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// RegisterMetaStore indicates an expected call of RegisterMetaStore.
func (mr *MockServerMasterClientMockRecorder) RegisterMetaStore(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterMetaStore", reflect.TypeOf((*MockServerMasterClient)(nil).RegisterMetaStore), arg0, arg1)
}

// RemoveResource mocks base method.
func (m *MockServerMasterClient) RemoveResource(arg0 context.Context, arg1 *enginepb.RemoveResourceRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveResource", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveResource indicates an expected call of RemoveResource.
func (mr *MockServerMasterClientMockRecorder) RemoveResource(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveResource", reflect.TypeOf((*MockServerMasterClient)(nil).RemoveResource), arg0, arg1)
}

// ReportExecutorWorkload mocks base method.
func (m *MockServerMasterClient) ReportExecutorWorkload(arg0 context.Context, arg1 *enginepb.ExecWorkloadRequest) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReportExecutorWorkload", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReportExecutorWorkload indicates an expected call of ReportExecutorWorkload.
func (mr *MockServerMasterClientMockRecorder) ReportExecutorWorkload(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReportExecutorWorkload", reflect.TypeOf((*MockServerMasterClient)(nil).ReportExecutorWorkload), arg0, arg1)
}

// ScheduleTask mocks base method.
func (m *MockServerMasterClient) ScheduleTask(arg0 context.Context, arg1 *enginepb.ScheduleTaskRequest) (*enginepb.ScheduleTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ScheduleTask", arg0, arg1)
	ret0, _ := ret[0].(*enginepb.ScheduleTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ScheduleTask indicates an expected call of ScheduleTask.
func (mr *MockServerMasterClientMockRecorder) ScheduleTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ScheduleTask", reflect.TypeOf((*MockServerMasterClient)(nil).ScheduleTask), arg0, arg1)
}
