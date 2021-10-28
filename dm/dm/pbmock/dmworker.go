// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/ticdc/dm/dm/pb (interfaces: WorkerClient,WorkerServer)

// Package pbmock is a generated GoMock package.
package pbmock

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	pb "github.com/pingcap/ticdc/dm/dm/pb"
	grpc "google.golang.org/grpc"
)

// MockWorkerClient is a mock of WorkerClient interface.
type MockWorkerClient struct {
	ctrl     *gomock.Controller
	recorder *MockWorkerClientMockRecorder
}

// MockWorkerClientMockRecorder is the mock recorder for MockWorkerClient.
type MockWorkerClientMockRecorder struct {
	mock *MockWorkerClient
}

// NewMockWorkerClient creates a new mock instance.
func NewMockWorkerClient(ctrl *gomock.Controller) *MockWorkerClient {
	mock := &MockWorkerClient{ctrl: ctrl}
	mock.recorder = &MockWorkerClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWorkerClient) EXPECT() *MockWorkerClientMockRecorder {
	return m.recorder
}

// GetWorkerCfg mocks base method.
func (m *MockWorkerClient) GetWorkerCfg(arg0 context.Context, arg1 *pb.GetWorkerCfgRequest, arg2 ...grpc.CallOption) (*pb.GetWorkerCfgResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetWorkerCfg", varargs...)
	ret0, _ := ret[0].(*pb.GetWorkerCfgResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerCfg indicates an expected call of GetWorkerCfg.
func (mr *MockWorkerClientMockRecorder) GetWorkerCfg(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerCfg", reflect.TypeOf((*MockWorkerClient)(nil).GetWorkerCfg), varargs...)
}

// HandleError mocks base method.
func (m *MockWorkerClient) HandleError(arg0 context.Context, arg1 *pb.HandleWorkerErrorRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "HandleError", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HandleError indicates an expected call of HandleError.
func (mr *MockWorkerClientMockRecorder) HandleError(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleError", reflect.TypeOf((*MockWorkerClient)(nil).HandleError), varargs...)
}

// OperateSchema mocks base method.
func (m *MockWorkerClient) OperateSchema(arg0 context.Context, arg1 *pb.OperateWorkerSchemaRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateSchema", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateSchema indicates an expected call of OperateSchema.
func (mr *MockWorkerClientMockRecorder) OperateSchema(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateSchema", reflect.TypeOf((*MockWorkerClient)(nil).OperateSchema), varargs...)
}

// OperateV1Meta mocks base method.
func (m *MockWorkerClient) OperateV1Meta(arg0 context.Context, arg1 *pb.OperateV1MetaRequest, arg2 ...grpc.CallOption) (*pb.OperateV1MetaResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateV1Meta", varargs...)
	ret0, _ := ret[0].(*pb.OperateV1MetaResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateV1Meta indicates an expected call of OperateV1Meta.
func (mr *MockWorkerClientMockRecorder) OperateV1Meta(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateV1Meta", reflect.TypeOf((*MockWorkerClient)(nil).OperateV1Meta), varargs...)
}

// PurgeRelay mocks base method.
func (m *MockWorkerClient) PurgeRelay(arg0 context.Context, arg1 *pb.PurgeRelayRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PurgeRelay", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PurgeRelay indicates an expected call of PurgeRelay.
func (mr *MockWorkerClientMockRecorder) PurgeRelay(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PurgeRelay", reflect.TypeOf((*MockWorkerClient)(nil).PurgeRelay), varargs...)
}

// QueryStatus mocks base method.
func (m *MockWorkerClient) QueryStatus(arg0 context.Context, arg1 *pb.QueryStatusRequest, arg2 ...grpc.CallOption) (*pb.QueryStatusResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryStatus", varargs...)
	ret0, _ := ret[0].(*pb.QueryStatusResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryStatus indicates an expected call of QueryStatus.
func (mr *MockWorkerClientMockRecorder) QueryStatus(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryStatus", reflect.TypeOf((*MockWorkerClient)(nil).QueryStatus), varargs...)
}

// MockWorkerServer is a mock of WorkerServer interface.
type MockWorkerServer struct {
	ctrl     *gomock.Controller
	recorder *MockWorkerServerMockRecorder
}

// MockWorkerServerMockRecorder is the mock recorder for MockWorkerServer.
type MockWorkerServerMockRecorder struct {
	mock *MockWorkerServer
}

// NewMockWorkerServer creates a new mock instance.
func NewMockWorkerServer(ctrl *gomock.Controller) *MockWorkerServer {
	mock := &MockWorkerServer{ctrl: ctrl}
	mock.recorder = &MockWorkerServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWorkerServer) EXPECT() *MockWorkerServerMockRecorder {
	return m.recorder
}

// GetWorkerCfg mocks base method.
func (m *MockWorkerServer) GetWorkerCfg(arg0 context.Context, arg1 *pb.GetWorkerCfgRequest) (*pb.GetWorkerCfgResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkerCfg", arg0, arg1)
	ret0, _ := ret[0].(*pb.GetWorkerCfgResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerCfg indicates an expected call of GetWorkerCfg.
func (mr *MockWorkerServerMockRecorder) GetWorkerCfg(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerCfg", reflect.TypeOf((*MockWorkerServer)(nil).GetWorkerCfg), arg0, arg1)
}

// HandleError mocks base method.
func (m *MockWorkerServer) HandleError(arg0 context.Context, arg1 *pb.HandleWorkerErrorRequest) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleError", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HandleError indicates an expected call of HandleError.
func (mr *MockWorkerServerMockRecorder) HandleError(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleError", reflect.TypeOf((*MockWorkerServer)(nil).HandleError), arg0, arg1)
}

// OperateSchema mocks base method.
func (m *MockWorkerServer) OperateSchema(arg0 context.Context, arg1 *pb.OperateWorkerSchemaRequest) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OperateSchema", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateSchema indicates an expected call of OperateSchema.
func (mr *MockWorkerServerMockRecorder) OperateSchema(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateSchema", reflect.TypeOf((*MockWorkerServer)(nil).OperateSchema), arg0, arg1)
}

// OperateV1Meta mocks base method.
func (m *MockWorkerServer) OperateV1Meta(arg0 context.Context, arg1 *pb.OperateV1MetaRequest) (*pb.OperateV1MetaResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OperateV1Meta", arg0, arg1)
	ret0, _ := ret[0].(*pb.OperateV1MetaResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateV1Meta indicates an expected call of OperateV1Meta.
func (mr *MockWorkerServerMockRecorder) OperateV1Meta(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateV1Meta", reflect.TypeOf((*MockWorkerServer)(nil).OperateV1Meta), arg0, arg1)
}

// PurgeRelay mocks base method.
func (m *MockWorkerServer) PurgeRelay(arg0 context.Context, arg1 *pb.PurgeRelayRequest) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PurgeRelay", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PurgeRelay indicates an expected call of PurgeRelay.
func (mr *MockWorkerServerMockRecorder) PurgeRelay(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PurgeRelay", reflect.TypeOf((*MockWorkerServer)(nil).PurgeRelay), arg0, arg1)
}

// QueryStatus mocks base method.
func (m *MockWorkerServer) QueryStatus(arg0 context.Context, arg1 *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryStatus", arg0, arg1)
	ret0, _ := ret[0].(*pb.QueryStatusResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryStatus indicates an expected call of QueryStatus.
func (mr *MockWorkerServerMockRecorder) QueryStatus(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryStatus", reflect.TypeOf((*MockWorkerServer)(nil).QueryStatus), arg0, arg1)
}
