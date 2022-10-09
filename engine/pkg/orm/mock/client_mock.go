// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/tiflow/engine/pkg/orm (interfaces: Client)

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	model "github.com/pingcap/tiflow/engine/framework/model"
	model0 "github.com/pingcap/tiflow/engine/model"
	model1 "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	orm "github.com/pingcap/tiflow/engine/pkg/orm"
	model2 "github.com/pingcap/tiflow/engine/pkg/orm/model"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockClient) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockClient)(nil).Close))
}

// CreateProject mocks base method.
func (m *MockClient) CreateProject(arg0 context.Context, arg1 *model2.ProjectInfo) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateProject", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateProject indicates an expected call of CreateProject.
func (mr *MockClientMockRecorder) CreateProject(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateProject", reflect.TypeOf((*MockClient)(nil).CreateProject), arg0, arg1)
}

// CreateProjectOperation mocks base method.
func (m *MockClient) CreateProjectOperation(arg0 context.Context, arg1 *model2.ProjectOperation) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateProjectOperation", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateProjectOperation indicates an expected call of CreateProjectOperation.
func (mr *MockClientMockRecorder) CreateProjectOperation(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateProjectOperation", reflect.TypeOf((*MockClient)(nil).CreateProjectOperation), arg0, arg1)
}

// CreateResource mocks base method.
func (m *MockClient) CreateResource(arg0 context.Context, arg1 *model1.ResourceMeta) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateResource", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateResource indicates an expected call of CreateResource.
func (mr *MockClientMockRecorder) CreateResource(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateResource", reflect.TypeOf((*MockClient)(nil).CreateResource), arg0, arg1)
}

// DeleteJob mocks base method.
func (m *MockClient) DeleteJob(arg0 context.Context, arg1 string) (orm.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteJob", arg0, arg1)
	ret0, _ := ret[0].(orm.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteJob indicates an expected call of DeleteJob.
func (mr *MockClientMockRecorder) DeleteJob(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteJob", reflect.TypeOf((*MockClient)(nil).DeleteJob), arg0, arg1)
}

// DeleteProject mocks base method.
func (m *MockClient) DeleteProject(arg0 context.Context, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteProject", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteProject indicates an expected call of DeleteProject.
func (mr *MockClientMockRecorder) DeleteProject(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteProject", reflect.TypeOf((*MockClient)(nil).DeleteProject), arg0, arg1)
}

// DeleteResource mocks base method.
func (m *MockClient) DeleteResource(arg0 context.Context, arg1 model1.ResourceKey) (orm.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteResource", arg0, arg1)
	ret0, _ := ret[0].(orm.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteResource indicates an expected call of DeleteResource.
func (mr *MockClientMockRecorder) DeleteResource(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteResource", reflect.TypeOf((*MockClient)(nil).DeleteResource), arg0, arg1)
}

// DeleteResourcesByExecutorID mocks base method.
func (m *MockClient) DeleteResourcesByExecutorID(arg0 context.Context, arg1 model0.DeployNodeID) (orm.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteResourcesByExecutorID", arg0, arg1)
	ret0, _ := ret[0].(orm.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteResourcesByExecutorID indicates an expected call of DeleteResourcesByExecutorID.
func (mr *MockClientMockRecorder) DeleteResourcesByExecutorID(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteResourcesByExecutorID", reflect.TypeOf((*MockClient)(nil).DeleteResourcesByExecutorID), arg0, arg1)
}

// DeleteResourcesByExecutorIDs mocks base method.
func (m *MockClient) DeleteResourcesByExecutorIDs(arg0 context.Context, arg1 []model0.DeployNodeID) (orm.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteResourcesByExecutorIDs", arg0, arg1)
	ret0, _ := ret[0].(orm.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteResourcesByExecutorIDs indicates an expected call of DeleteResourcesByExecutorIDs.
func (mr *MockClientMockRecorder) DeleteResourcesByExecutorIDs(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteResourcesByExecutorIDs", reflect.TypeOf((*MockClient)(nil).DeleteResourcesByExecutorIDs), arg0, arg1)
}

// DeleteWorker mocks base method.
func (m *MockClient) DeleteWorker(arg0 context.Context, arg1, arg2 string) (orm.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteWorker", arg0, arg1, arg2)
	ret0, _ := ret[0].(orm.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteWorker indicates an expected call of DeleteWorker.
func (mr *MockClientMockRecorder) DeleteWorker(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteWorker", reflect.TypeOf((*MockClient)(nil).DeleteWorker), arg0, arg1, arg2)
}

// GenEpoch mocks base method.
func (m *MockClient) GenEpoch(arg0 context.Context) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GenEpoch", arg0)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GenEpoch indicates an expected call of GenEpoch.
func (mr *MockClientMockRecorder) GenEpoch(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GenEpoch", reflect.TypeOf((*MockClient)(nil).GenEpoch), arg0)
}

// GetJobByID mocks base method.
func (m *MockClient) GetJobByID(arg0 context.Context, arg1 string) (*model.MasterMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetJobByID", arg0, arg1)
	ret0, _ := ret[0].(*model.MasterMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetJobByID indicates an expected call of GetJobByID.
func (mr *MockClientMockRecorder) GetJobByID(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetJobByID", reflect.TypeOf((*MockClient)(nil).GetJobByID), arg0, arg1)
}

// GetOneResourceForGC mocks base method.
func (m *MockClient) GetOneResourceForGC(arg0 context.Context) (*model1.ResourceMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetOneResourceForGC", arg0)
	ret0, _ := ret[0].(*model1.ResourceMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetOneResourceForGC indicates an expected call of GetOneResourceForGC.
func (mr *MockClientMockRecorder) GetOneResourceForGC(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetOneResourceForGC", reflect.TypeOf((*MockClient)(nil).GetOneResourceForGC), arg0)
}

// GetProjectByID mocks base method.
func (m *MockClient) GetProjectByID(arg0 context.Context, arg1 string) (*model2.ProjectInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetProjectByID", arg0, arg1)
	ret0, _ := ret[0].(*model2.ProjectInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetProjectByID indicates an expected call of GetProjectByID.
func (mr *MockClientMockRecorder) GetProjectByID(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetProjectByID", reflect.TypeOf((*MockClient)(nil).GetProjectByID), arg0, arg1)
}

// GetResourceByID mocks base method.
func (m *MockClient) GetResourceByID(arg0 context.Context, arg1 model1.ResourceKey) (*model1.ResourceMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetResourceByID", arg0, arg1)
	ret0, _ := ret[0].(*model1.ResourceMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetResourceByID indicates an expected call of GetResourceByID.
func (mr *MockClientMockRecorder) GetResourceByID(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetResourceByID", reflect.TypeOf((*MockClient)(nil).GetResourceByID), arg0, arg1)
}

// GetWorkerByID mocks base method.
func (m *MockClient) GetWorkerByID(arg0 context.Context, arg1, arg2 string) (*model.WorkerStatus, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWorkerByID", arg0, arg1, arg2)
	ret0, _ := ret[0].(*model.WorkerStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetWorkerByID indicates an expected call of GetWorkerByID.
func (mr *MockClientMockRecorder) GetWorkerByID(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWorkerByID", reflect.TypeOf((*MockClient)(nil).GetWorkerByID), arg0, arg1, arg2)
}

// QueryJobOp mocks base method.
func (m *MockClient) QueryJobOp(arg0 context.Context, arg1 string) (*model2.JobOp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryJobOp", arg0, arg1)
	ret0, _ := ret[0].(*model2.JobOp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryJobOp indicates an expected call of QueryJobOp.
func (mr *MockClientMockRecorder) QueryJobOp(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryJobOp", reflect.TypeOf((*MockClient)(nil).QueryJobOp), arg0, arg1)
}

// QueryJobOpsByStatus mocks base method.
func (m *MockClient) QueryJobOpsByStatus(arg0 context.Context, arg1 model2.JobOpStatus) ([]*model2.JobOp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryJobOpsByStatus", arg0, arg1)
	ret0, _ := ret[0].([]*model2.JobOp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryJobOpsByStatus indicates an expected call of QueryJobOpsByStatus.
func (mr *MockClientMockRecorder) QueryJobOpsByStatus(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryJobOpsByStatus", reflect.TypeOf((*MockClient)(nil).QueryJobOpsByStatus), arg0, arg1)
}

// QueryJobs mocks base method.
func (m *MockClient) QueryJobs(arg0 context.Context) ([]*model.MasterMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryJobs", arg0)
	ret0, _ := ret[0].([]*model.MasterMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryJobs indicates an expected call of QueryJobs.
func (mr *MockClientMockRecorder) QueryJobs(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryJobs", reflect.TypeOf((*MockClient)(nil).QueryJobs), arg0)
}

// QueryJobsByProjectID mocks base method.
func (m *MockClient) QueryJobsByProjectID(arg0 context.Context, arg1 string) ([]*model.MasterMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryJobsByProjectID", arg0, arg1)
	ret0, _ := ret[0].([]*model.MasterMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryJobsByProjectID indicates an expected call of QueryJobsByProjectID.
func (mr *MockClientMockRecorder) QueryJobsByProjectID(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryJobsByProjectID", reflect.TypeOf((*MockClient)(nil).QueryJobsByProjectID), arg0, arg1)
}

// QueryJobsByState mocks base method.
func (m *MockClient) QueryJobsByState(arg0 context.Context, arg1 string, arg2 int) ([]*model.MasterMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryJobsByState", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*model.MasterMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryJobsByState indicates an expected call of QueryJobsByState.
func (mr *MockClientMockRecorder) QueryJobsByState(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryJobsByState", reflect.TypeOf((*MockClient)(nil).QueryJobsByState), arg0, arg1, arg2)
}

// QueryProjectOperations mocks base method.
func (m *MockClient) QueryProjectOperations(arg0 context.Context, arg1 string) ([]*model2.ProjectOperation, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryProjectOperations", arg0, arg1)
	ret0, _ := ret[0].([]*model2.ProjectOperation)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryProjectOperations indicates an expected call of QueryProjectOperations.
func (mr *MockClientMockRecorder) QueryProjectOperations(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryProjectOperations", reflect.TypeOf((*MockClient)(nil).QueryProjectOperations), arg0, arg1)
}

// QueryProjectOperationsByTimeRange mocks base method.
func (m *MockClient) QueryProjectOperationsByTimeRange(arg0 context.Context, arg1 string, arg2 orm.TimeRange) ([]*model2.ProjectOperation, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryProjectOperationsByTimeRange", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*model2.ProjectOperation)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryProjectOperationsByTimeRange indicates an expected call of QueryProjectOperationsByTimeRange.
func (mr *MockClientMockRecorder) QueryProjectOperationsByTimeRange(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryProjectOperationsByTimeRange", reflect.TypeOf((*MockClient)(nil).QueryProjectOperationsByTimeRange), arg0, arg1, arg2)
}

// QueryProjects mocks base method.
func (m *MockClient) QueryProjects(arg0 context.Context) ([]*model2.ProjectInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryProjects", arg0)
	ret0, _ := ret[0].([]*model2.ProjectInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryProjects indicates an expected call of QueryProjects.
func (mr *MockClientMockRecorder) QueryProjects(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryProjects", reflect.TypeOf((*MockClient)(nil).QueryProjects), arg0)
}

// QueryResources mocks base method.
func (m *MockClient) QueryResources(arg0 context.Context) ([]*model1.ResourceMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryResources", arg0)
	ret0, _ := ret[0].([]*model1.ResourceMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryResources indicates an expected call of QueryResources.
func (mr *MockClientMockRecorder) QueryResources(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryResources", reflect.TypeOf((*MockClient)(nil).QueryResources), arg0)
}

// QueryResourcesByExecutorID mocks base method.
func (m *MockClient) QueryResourcesByExecutorID(arg0 context.Context, arg1 string) ([]*model1.ResourceMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryResourcesByExecutorID", arg0, arg1)
	ret0, _ := ret[0].([]*model1.ResourceMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryResourcesByExecutorID indicates an expected call of QueryResourcesByExecutorID.
func (mr *MockClientMockRecorder) QueryResourcesByExecutorID(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryResourcesByExecutorID", reflect.TypeOf((*MockClient)(nil).QueryResourcesByExecutorID), arg0, arg1)
}

// QueryResourcesByJobID mocks base method.
func (m *MockClient) QueryResourcesByJobID(arg0 context.Context, arg1 string) ([]*model1.ResourceMeta, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryResourcesByJobID", arg0, arg1)
	ret0, _ := ret[0].([]*model1.ResourceMeta)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryResourcesByJobID indicates an expected call of QueryResourcesByJobID.
func (mr *MockClientMockRecorder) QueryResourcesByJobID(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryResourcesByJobID", reflect.TypeOf((*MockClient)(nil).QueryResourcesByJobID), arg0, arg1)
}

// QueryWorkersByMasterID mocks base method.
func (m *MockClient) QueryWorkersByMasterID(arg0 context.Context, arg1 string) ([]*model.WorkerStatus, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryWorkersByMasterID", arg0, arg1)
	ret0, _ := ret[0].([]*model.WorkerStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkersByMasterID indicates an expected call of QueryWorkersByMasterID.
func (mr *MockClientMockRecorder) QueryWorkersByMasterID(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkersByMasterID", reflect.TypeOf((*MockClient)(nil).QueryWorkersByMasterID), arg0, arg1)
}

// QueryWorkersByState mocks base method.
func (m *MockClient) QueryWorkersByState(arg0 context.Context, arg1 string, arg2 int) ([]*model.WorkerStatus, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryWorkersByState", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*model.WorkerStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkersByState indicates an expected call of QueryWorkersByState.
func (mr *MockClientMockRecorder) QueryWorkersByState(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkersByState", reflect.TypeOf((*MockClient)(nil).QueryWorkersByState), arg0, arg1, arg2)
}

// SetGCPendingByJobs mocks base method.
func (m *MockClient) SetGCPendingByJobs(arg0 context.Context, arg1 []string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetGCPendingByJobs", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetGCPendingByJobs indicates an expected call of SetGCPendingByJobs.
func (mr *MockClientMockRecorder) SetGCPendingByJobs(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetGCPendingByJobs", reflect.TypeOf((*MockClient)(nil).SetGCPendingByJobs), arg0, arg1)
}

// SetJobCanceled mocks base method.
func (m *MockClient) SetJobCanceled(arg0 context.Context, arg1 string) (orm.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetJobCanceled", arg0, arg1)
	ret0, _ := ret[0].(orm.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SetJobCanceled indicates an expected call of SetJobCanceled.
func (mr *MockClientMockRecorder) SetJobCanceled(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetJobCanceled", reflect.TypeOf((*MockClient)(nil).SetJobCanceled), arg0, arg1)
}

// SetJobCanceling mocks base method.
func (m *MockClient) SetJobCanceling(arg0 context.Context, arg1 string) (orm.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetJobCanceling", arg0, arg1)
	ret0, _ := ret[0].(orm.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SetJobCanceling indicates an expected call of SetJobCanceling.
func (mr *MockClientMockRecorder) SetJobCanceling(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetJobCanceling", reflect.TypeOf((*MockClient)(nil).SetJobCanceling), arg0, arg1)
}

// SetJobNoop mocks base method.
func (m *MockClient) SetJobNoop(arg0 context.Context, arg1 string) (orm.Result, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetJobNoop", arg0, arg1)
	ret0, _ := ret[0].(orm.Result)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SetJobNoop indicates an expected call of SetJobNoop.
func (mr *MockClientMockRecorder) SetJobNoop(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetJobNoop", reflect.TypeOf((*MockClient)(nil).SetJobNoop), arg0, arg1)
}

// UpdateJob mocks base method.
func (m *MockClient) UpdateJob(arg0 context.Context, arg1 string, arg2 map[string]interface{}) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateJob", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateJob indicates an expected call of UpdateJob.
func (mr *MockClientMockRecorder) UpdateJob(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateJob", reflect.TypeOf((*MockClient)(nil).UpdateJob), arg0, arg1, arg2)
}

// UpdateResource mocks base method.
func (m *MockClient) UpdateResource(arg0 context.Context, arg1 *model1.ResourceMeta) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateResource", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateResource indicates an expected call of UpdateResource.
func (mr *MockClientMockRecorder) UpdateResource(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateResource", reflect.TypeOf((*MockClient)(nil).UpdateResource), arg0, arg1)
}

// UpdateWorker mocks base method.
func (m *MockClient) UpdateWorker(arg0 context.Context, arg1 *model.WorkerStatus) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorker", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateWorker indicates an expected call of UpdateWorker.
func (mr *MockClientMockRecorder) UpdateWorker(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorker", reflect.TypeOf((*MockClient)(nil).UpdateWorker), arg0, arg1)
}

// UpsertJob mocks base method.
func (m *MockClient) UpsertJob(arg0 context.Context, arg1 *model.MasterMeta) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpsertJob", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpsertJob indicates an expected call of UpsertJob.
func (mr *MockClientMockRecorder) UpsertJob(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpsertJob", reflect.TypeOf((*MockClient)(nil).UpsertJob), arg0, arg1)
}

// UpsertResource mocks base method.
func (m *MockClient) UpsertResource(arg0 context.Context, arg1 *model1.ResourceMeta) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpsertResource", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpsertResource indicates an expected call of UpsertResource.
func (mr *MockClientMockRecorder) UpsertResource(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpsertResource", reflect.TypeOf((*MockClient)(nil).UpsertResource), arg0, arg1)
}

// UpsertWorker mocks base method.
func (m *MockClient) UpsertWorker(arg0 context.Context, arg1 *model.WorkerStatus) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpsertWorker", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpsertWorker indicates an expected call of UpsertWorker.
func (mr *MockClientMockRecorder) UpsertWorker(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpsertWorker", reflect.TypeOf((*MockClient)(nil).UpsertWorker), arg0, arg1)
}
