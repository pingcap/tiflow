// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package enginepb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// DiscoveryClient is the client API for Discovery service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DiscoveryClient interface {
	RegisterExecutor(ctx context.Context, in *RegisterExecutorRequest, opts ...grpc.CallOption) (*Executor, error)
	// ListExecutors lists all executors.
	// Executors will use this API to discover other executors.
	// Currently, we assume that there aren't too many executors.
	// If the number of executors becomes very large in the future,
	// we can consider implement a mechanism to watch the changes of the executors.
	ListExecutors(ctx context.Context, in *ListExecutorsRequest, opts ...grpc.CallOption) (*ListExecutorsResponse, error)
	Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error)
	// RegisterMetaStore is called from backend metastore and
	// registers to server master metastore manager
	RegisterMetaStore(ctx context.Context, in *RegisterMetaStoreRequest, opts ...grpc.CallOption) (*RegisterMetaStoreResponse, error)
	// QueryMetaStore queries metastore manager and returns
	// the information of a matching metastore
	QueryMetaStore(ctx context.Context, in *QueryMetaStoreRequest, opts ...grpc.CallOption) (*QueryMetaStoreResponse, error)
	GetLeader(ctx context.Context, in *GetLeaderRequest, opts ...grpc.CallOption) (*GetLeaderResponse, error)
	ResignLeader(ctx context.Context, in *ResignLeaderRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type discoveryClient struct {
	cc grpc.ClientConnInterface
}

func NewDiscoveryClient(cc grpc.ClientConnInterface) DiscoveryClient {
	return &discoveryClient{cc}
}

func (c *discoveryClient) RegisterExecutor(ctx context.Context, in *RegisterExecutorRequest, opts ...grpc.CallOption) (*Executor, error) {
	out := new(Executor)
	err := c.cc.Invoke(ctx, "/enginepb.Discovery/RegisterExecutor", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *discoveryClient) ListExecutors(ctx context.Context, in *ListExecutorsRequest, opts ...grpc.CallOption) (*ListExecutorsResponse, error) {
	out := new(ListExecutorsResponse)
	err := c.cc.Invoke(ctx, "/enginepb.Discovery/ListExecutors", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *discoveryClient) Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	out := new(HeartbeatResponse)
	err := c.cc.Invoke(ctx, "/enginepb.Discovery/Heartbeat", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *discoveryClient) RegisterMetaStore(ctx context.Context, in *RegisterMetaStoreRequest, opts ...grpc.CallOption) (*RegisterMetaStoreResponse, error) {
	out := new(RegisterMetaStoreResponse)
	err := c.cc.Invoke(ctx, "/enginepb.Discovery/RegisterMetaStore", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *discoveryClient) QueryMetaStore(ctx context.Context, in *QueryMetaStoreRequest, opts ...grpc.CallOption) (*QueryMetaStoreResponse, error) {
	out := new(QueryMetaStoreResponse)
	err := c.cc.Invoke(ctx, "/enginepb.Discovery/QueryMetaStore", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *discoveryClient) GetLeader(ctx context.Context, in *GetLeaderRequest, opts ...grpc.CallOption) (*GetLeaderResponse, error) {
	out := new(GetLeaderResponse)
	err := c.cc.Invoke(ctx, "/enginepb.Discovery/GetLeader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *discoveryClient) ResignLeader(ctx context.Context, in *ResignLeaderRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/enginepb.Discovery/ResignLeader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DiscoveryServer is the server API for Discovery service.
// All implementations should embed UnimplementedDiscoveryServer
// for forward compatibility
type DiscoveryServer interface {
	RegisterExecutor(context.Context, *RegisterExecutorRequest) (*Executor, error)
	// ListExecutors lists all executors.
	// Executors will use this API to discover other executors.
	// Currently, we assume that there aren't too many executors.
	// If the number of executors becomes very large in the future,
	// we can consider implement a mechanism to watch the changes of the executors.
	ListExecutors(context.Context, *ListExecutorsRequest) (*ListExecutorsResponse, error)
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	// RegisterMetaStore is called from backend metastore and
	// registers to server master metastore manager
	RegisterMetaStore(context.Context, *RegisterMetaStoreRequest) (*RegisterMetaStoreResponse, error)
	// QueryMetaStore queries metastore manager and returns
	// the information of a matching metastore
	QueryMetaStore(context.Context, *QueryMetaStoreRequest) (*QueryMetaStoreResponse, error)
	GetLeader(context.Context, *GetLeaderRequest) (*GetLeaderResponse, error)
	ResignLeader(context.Context, *ResignLeaderRequest) (*emptypb.Empty, error)
}

// UnimplementedDiscoveryServer should be embedded to have forward compatible implementations.
type UnimplementedDiscoveryServer struct {
}

func (UnimplementedDiscoveryServer) RegisterExecutor(context.Context, *RegisterExecutorRequest) (*Executor, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterExecutor not implemented")
}
func (UnimplementedDiscoveryServer) ListExecutors(context.Context, *ListExecutorsRequest) (*ListExecutorsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListExecutors not implemented")
}
func (UnimplementedDiscoveryServer) Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Heartbeat not implemented")
}
func (UnimplementedDiscoveryServer) RegisterMetaStore(context.Context, *RegisterMetaStoreRequest) (*RegisterMetaStoreResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterMetaStore not implemented")
}
func (UnimplementedDiscoveryServer) QueryMetaStore(context.Context, *QueryMetaStoreRequest) (*QueryMetaStoreResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryMetaStore not implemented")
}
func (UnimplementedDiscoveryServer) GetLeader(context.Context, *GetLeaderRequest) (*GetLeaderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetLeader not implemented")
}
func (UnimplementedDiscoveryServer) ResignLeader(context.Context, *ResignLeaderRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ResignLeader not implemented")
}

// UnsafeDiscoveryServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DiscoveryServer will
// result in compilation errors.
type UnsafeDiscoveryServer interface {
	mustEmbedUnimplementedDiscoveryServer()
}

func RegisterDiscoveryServer(s grpc.ServiceRegistrar, srv DiscoveryServer) {
	s.RegisterService(&Discovery_ServiceDesc, srv)
}

func _Discovery_RegisterExecutor_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterExecutorRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).RegisterExecutor(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.Discovery/RegisterExecutor",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).RegisterExecutor(ctx, req.(*RegisterExecutorRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_ListExecutors_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListExecutorsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).ListExecutors(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.Discovery/ListExecutors",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).ListExecutors(ctx, req.(*ListExecutorsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_Heartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.Discovery/Heartbeat",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).Heartbeat(ctx, req.(*HeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_RegisterMetaStore_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterMetaStoreRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).RegisterMetaStore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.Discovery/RegisterMetaStore",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).RegisterMetaStore(ctx, req.(*RegisterMetaStoreRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_QueryMetaStore_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryMetaStoreRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).QueryMetaStore(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.Discovery/QueryMetaStore",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).QueryMetaStore(ctx, req.(*QueryMetaStoreRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_GetLeader_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetLeaderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).GetLeader(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.Discovery/GetLeader",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).GetLeader(ctx, req.(*GetLeaderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Discovery_ResignLeader_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ResignLeaderRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DiscoveryServer).ResignLeader(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.Discovery/ResignLeader",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DiscoveryServer).ResignLeader(ctx, req.(*ResignLeaderRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Discovery_ServiceDesc is the grpc.ServiceDesc for Discovery service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Discovery_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "enginepb.Discovery",
	HandlerType: (*DiscoveryServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RegisterExecutor",
			Handler:    _Discovery_RegisterExecutor_Handler,
		},
		{
			MethodName: "ListExecutors",
			Handler:    _Discovery_ListExecutors_Handler,
		},
		{
			MethodName: "Heartbeat",
			Handler:    _Discovery_Heartbeat_Handler,
		},
		{
			MethodName: "RegisterMetaStore",
			Handler:    _Discovery_RegisterMetaStore_Handler,
		},
		{
			MethodName: "QueryMetaStore",
			Handler:    _Discovery_QueryMetaStore_Handler,
		},
		{
			MethodName: "GetLeader",
			Handler:    _Discovery_GetLeader_Handler,
		},
		{
			MethodName: "ResignLeader",
			Handler:    _Discovery_ResignLeader_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "engine/proto/master.proto",
}

// TaskSchedulerClient is the client API for TaskScheduler service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TaskSchedulerClient interface {
	ScheduleTask(ctx context.Context, in *ScheduleTaskRequest, opts ...grpc.CallOption) (*ScheduleTaskResponse, error)
	// ReportExecutorWorkload is called from executor to server master to report
	// resource usage in executor.
	ReportExecutorWorkload(ctx context.Context, in *ExecWorkloadRequest, opts ...grpc.CallOption) (*ExecWorkloadResponse, error)
}

type taskSchedulerClient struct {
	cc grpc.ClientConnInterface
}

func NewTaskSchedulerClient(cc grpc.ClientConnInterface) TaskSchedulerClient {
	return &taskSchedulerClient{cc}
}

func (c *taskSchedulerClient) ScheduleTask(ctx context.Context, in *ScheduleTaskRequest, opts ...grpc.CallOption) (*ScheduleTaskResponse, error) {
	out := new(ScheduleTaskResponse)
	err := c.cc.Invoke(ctx, "/enginepb.TaskScheduler/ScheduleTask", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *taskSchedulerClient) ReportExecutorWorkload(ctx context.Context, in *ExecWorkloadRequest, opts ...grpc.CallOption) (*ExecWorkloadResponse, error) {
	out := new(ExecWorkloadResponse)
	err := c.cc.Invoke(ctx, "/enginepb.TaskScheduler/ReportExecutorWorkload", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TaskSchedulerServer is the server API for TaskScheduler service.
// All implementations should embed UnimplementedTaskSchedulerServer
// for forward compatibility
type TaskSchedulerServer interface {
	ScheduleTask(context.Context, *ScheduleTaskRequest) (*ScheduleTaskResponse, error)
	// ReportExecutorWorkload is called from executor to server master to report
	// resource usage in executor.
	ReportExecutorWorkload(context.Context, *ExecWorkloadRequest) (*ExecWorkloadResponse, error)
}

// UnimplementedTaskSchedulerServer should be embedded to have forward compatible implementations.
type UnimplementedTaskSchedulerServer struct {
}

func (UnimplementedTaskSchedulerServer) ScheduleTask(context.Context, *ScheduleTaskRequest) (*ScheduleTaskResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ScheduleTask not implemented")
}
func (UnimplementedTaskSchedulerServer) ReportExecutorWorkload(context.Context, *ExecWorkloadRequest) (*ExecWorkloadResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ReportExecutorWorkload not implemented")
}

// UnsafeTaskSchedulerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TaskSchedulerServer will
// result in compilation errors.
type UnsafeTaskSchedulerServer interface {
	mustEmbedUnimplementedTaskSchedulerServer()
}

func RegisterTaskSchedulerServer(s grpc.ServiceRegistrar, srv TaskSchedulerServer) {
	s.RegisterService(&TaskScheduler_ServiceDesc, srv)
}

func _TaskScheduler_ScheduleTask_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ScheduleTaskRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TaskSchedulerServer).ScheduleTask(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.TaskScheduler/ScheduleTask",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TaskSchedulerServer).ScheduleTask(ctx, req.(*ScheduleTaskRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TaskScheduler_ReportExecutorWorkload_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecWorkloadRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TaskSchedulerServer).ReportExecutorWorkload(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.TaskScheduler/ReportExecutorWorkload",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TaskSchedulerServer).ReportExecutorWorkload(ctx, req.(*ExecWorkloadRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// TaskScheduler_ServiceDesc is the grpc.ServiceDesc for TaskScheduler service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TaskScheduler_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "enginepb.TaskScheduler",
	HandlerType: (*TaskSchedulerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ScheduleTask",
			Handler:    _TaskScheduler_ScheduleTask_Handler,
		},
		{
			MethodName: "ReportExecutorWorkload",
			Handler:    _TaskScheduler_ReportExecutorWorkload_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "engine/proto/master.proto",
}

// JobManagerClient is the client API for JobManager service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type JobManagerClient interface {
	CreateJob(ctx context.Context, in *CreateJobRequest, opts ...grpc.CallOption) (*Job, error)
	GetJob(ctx context.Context, in *GetJobRequest, opts ...grpc.CallOption) (*Job, error)
	ListJobs(ctx context.Context, in *ListJobsRequest, opts ...grpc.CallOption) (*ListJobsResponse, error)
	CancelJob(ctx context.Context, in *CancelJobRequest, opts ...grpc.CallOption) (*Job, error)
	DeleteJob(ctx context.Context, in *DeleteJobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type jobManagerClient struct {
	cc grpc.ClientConnInterface
}

func NewJobManagerClient(cc grpc.ClientConnInterface) JobManagerClient {
	return &jobManagerClient{cc}
}

func (c *jobManagerClient) CreateJob(ctx context.Context, in *CreateJobRequest, opts ...grpc.CallOption) (*Job, error) {
	out := new(Job)
	err := c.cc.Invoke(ctx, "/enginepb.JobManager/CreateJob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobManagerClient) GetJob(ctx context.Context, in *GetJobRequest, opts ...grpc.CallOption) (*Job, error) {
	out := new(Job)
	err := c.cc.Invoke(ctx, "/enginepb.JobManager/GetJob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobManagerClient) ListJobs(ctx context.Context, in *ListJobsRequest, opts ...grpc.CallOption) (*ListJobsResponse, error) {
	out := new(ListJobsResponse)
	err := c.cc.Invoke(ctx, "/enginepb.JobManager/ListJobs", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobManagerClient) CancelJob(ctx context.Context, in *CancelJobRequest, opts ...grpc.CallOption) (*Job, error) {
	out := new(Job)
	err := c.cc.Invoke(ctx, "/enginepb.JobManager/CancelJob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobManagerClient) DeleteJob(ctx context.Context, in *DeleteJobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/enginepb.JobManager/DeleteJob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// JobManagerServer is the server API for JobManager service.
// All implementations should embed UnimplementedJobManagerServer
// for forward compatibility
type JobManagerServer interface {
	CreateJob(context.Context, *CreateJobRequest) (*Job, error)
	GetJob(context.Context, *GetJobRequest) (*Job, error)
	ListJobs(context.Context, *ListJobsRequest) (*ListJobsResponse, error)
	CancelJob(context.Context, *CancelJobRequest) (*Job, error)
	DeleteJob(context.Context, *DeleteJobRequest) (*emptypb.Empty, error)
}

// UnimplementedJobManagerServer should be embedded to have forward compatible implementations.
type UnimplementedJobManagerServer struct {
}

func (UnimplementedJobManagerServer) CreateJob(context.Context, *CreateJobRequest) (*Job, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateJob not implemented")
}
func (UnimplementedJobManagerServer) GetJob(context.Context, *GetJobRequest) (*Job, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJob not implemented")
}
func (UnimplementedJobManagerServer) ListJobs(context.Context, *ListJobsRequest) (*ListJobsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListJobs not implemented")
}
func (UnimplementedJobManagerServer) CancelJob(context.Context, *CancelJobRequest) (*Job, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CancelJob not implemented")
}
func (UnimplementedJobManagerServer) DeleteJob(context.Context, *DeleteJobRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteJob not implemented")
}

// UnsafeJobManagerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to JobManagerServer will
// result in compilation errors.
type UnsafeJobManagerServer interface {
	mustEmbedUnimplementedJobManagerServer()
}

func RegisterJobManagerServer(s grpc.ServiceRegistrar, srv JobManagerServer) {
	s.RegisterService(&JobManager_ServiceDesc, srv)
}

func _JobManager_CreateJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobManagerServer).CreateJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.JobManager/CreateJob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobManagerServer).CreateJob(ctx, req.(*CreateJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobManager_GetJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobManagerServer).GetJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.JobManager/GetJob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobManagerServer).GetJob(ctx, req.(*GetJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobManager_ListJobs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListJobsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobManagerServer).ListJobs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.JobManager/ListJobs",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobManagerServer).ListJobs(ctx, req.(*ListJobsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobManager_CancelJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CancelJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobManagerServer).CancelJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.JobManager/CancelJob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobManagerServer).CancelJob(ctx, req.(*CancelJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobManager_DeleteJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobManagerServer).DeleteJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/enginepb.JobManager/DeleteJob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobManagerServer).DeleteJob(ctx, req.(*DeleteJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// JobManager_ServiceDesc is the grpc.ServiceDesc for JobManager service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var JobManager_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "enginepb.JobManager",
	HandlerType: (*JobManagerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateJob",
			Handler:    _JobManager_CreateJob_Handler,
		},
		{
			MethodName: "GetJob",
			Handler:    _JobManager_GetJob_Handler,
		},
		{
			MethodName: "ListJobs",
			Handler:    _JobManager_ListJobs_Handler,
		},
		{
			MethodName: "CancelJob",
			Handler:    _JobManager_CancelJob_Handler,
		},
		{
			MethodName: "DeleteJob",
			Handler:    _JobManager_DeleteJob_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "engine/proto/master.proto",
}
