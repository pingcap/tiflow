// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mock

import (
	"context"
	"errors"
	"sync"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/pingcap/tiflow/engine/pb"
)

var container *grpcContainer

func init() {
	ResetGrpcCtx()
}

type grpcContainer struct {
	mu      sync.Mutex
	servers map[string]GrpcServer
}

// GrpcServer implements a mock grpc server.
type GrpcServer interface {
	dial() (Conn, error)
	Stop()
}

type masterServer struct {
	*baseServer
	pb.MasterServer
}

func (s *masterServer) dial() (Conn, error) {
	return &masterServerConn{s}, nil
}

type masterServerConn struct {
	server *masterServer
}

func (s *masterServerConn) Close() error {
	return nil
}

func (s *masterServerConn) sendRequest(ctx context.Context, req interface{}) (interface{}, error) {
	switch x := req.(type) {
	case *pb.RegisterExecutorRequest:
		return s.server.RegisterExecutor(ctx, x)
	case *pb.PauseJobRequest:
		return s.server.PauseJob(ctx, x)
	case *pb.SubmitJobRequest:
		return s.server.SubmitJob(ctx, x)
	case *pb.HeartbeatRequest:
		return s.server.Heartbeat(ctx, x)
	case *pb.CancelJobRequest:
		return s.server.CancelJob(ctx, x)
	}
	return nil, errors.New("unknown request")
}

type masterServerClient struct {
	conn Conn
}

func (c *masterServerClient) ScheduleTask(ctx context.Context, req *pb.ScheduleTaskRequest, opts ...grpc.CallOption) (*pb.ScheduleTaskResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	return resp.(*pb.ScheduleTaskResponse), err
}

func (c *masterServerClient) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest, opts ...grpc.CallOption) (*pb.RegisterExecutorResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	return resp.(*pb.RegisterExecutorResponse), err
}

func (c *masterServerClient) PauseJob(ctx context.Context, req *pb.PauseJobRequest, opts ...grpc.CallOption) (*pb.PauseJobResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	return resp.(*pb.PauseJobResponse), err
}

func (c *masterServerClient) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest, opts ...grpc.CallOption) (*pb.SubmitJobResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	return resp.(*pb.SubmitJobResponse), err
}

func (c *masterServerClient) CancelJob(ctx context.Context, req *pb.CancelJobRequest, opts ...grpc.CallOption) (*pb.CancelJobResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	return resp.(*pb.CancelJobResponse), err
}

func (c *masterServerClient) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest, opts ...grpc.CallOption) (*pb.HeartbeatResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	return resp.(*pb.HeartbeatResponse), err
}

func (c *masterServerClient) RegisterMetaStore(
	ctx context.Context, req *pb.RegisterMetaStoreRequest, opts ...grpc.CallOption,
) (*pb.RegisterMetaStoreResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.RegisterMetaStoreResponse), nil
}

func (c *masterServerClient) QueryMetaStore(
	ctx context.Context, req *pb.QueryMetaStoreRequest, opts ...grpc.CallOption,
) (*pb.QueryMetaStoreResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.QueryMetaStoreResponse), nil
}

func (c *masterServerClient) QueryJob(
	ctx context.Context, req *pb.QueryJobRequest, opts ...grpc.CallOption,
) (*pb.QueryJobResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.QueryJobResponse), nil
}

func (c *masterServerClient) PersistResource(
	ctx context.Context, req *pb.PersistResourceRequest, opts ...grpc.CallOption,
) (*pb.PersistResourceResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.PersistResourceResponse), nil
}

func (c *masterServerClient) ReportExecutorWorkload(
	ctx context.Context, req *pb.ExecWorkloadRequest, opts ...grpc.CallOption,
) (*pb.ExecWorkloadResponse, error) {
	resp, err := c.conn.sendRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ExecWorkloadResponse), nil
}

func NewMasterClient(conn Conn) pb.MasterClient {
	return &masterServerClient{conn}
}

type executorServer struct {
	*baseServer
	pb.ExecutorServer
}

type executorServerConn struct {
	server *executorServer
}

type executorClient struct {
	conn Conn
}

type baseServer struct {
	addr string
}

func (s *baseServer) Stop() {
	container.mu.Lock()
	defer container.mu.Unlock()
	_, ok := container.servers[s.addr]
	log.L().Logger.Info("server is cancelled", zap.String("ip", s.addr))
	if ok {
		delete(container.servers, s.addr)
	}
}

func (s *executorServer) dial() (Conn, error) {
	return &executorServerConn{s}, nil
}

func (c *executorClient) PreDispatchTask(ctx context.Context, in *pb.PreDispatchTaskRequest, opts ...grpc.CallOption) (*pb.PreDispatchTaskResponse, error) {
	panic("implement me")
}

func (c *executorClient) ConfirmDispatchTask(ctx context.Context, in *pb.ConfirmDispatchTaskRequest, opts ...grpc.CallOption) (*pb.ConfirmDispatchTaskResponse, error) {
	panic("implement me")
}

func (s *executorServerConn) Close() error {
	return nil
}

func NewExecutorClient(conn Conn) pb.ExecutorClient {
	return &executorClient{conn}
}

func (s *executorServerConn) sendRequest(ctx context.Context, req interface{}) (interface{}, error) {
	switch x := req.(type) {
	case *pb.PreDispatchTaskRequest:
		return s.server.PreDispatchTask(ctx, x)
	case *pb.ConfirmDispatchTaskRequest:
		return s.server.ConfirmDispatchTask(ctx, x)
	default:
	}
	return nil, errors.New("unknown request")
}

func Dial(addr string) (Conn, error) {
	container.mu.Lock()
	defer container.mu.Unlock()
	server, ok := container.servers[addr]
	if !ok {
		return nil, errors.New("no server found")
	}
	return server.dial()
}

// NewMasterServer creates a master grpc server and listened the address.
// We try to make things simple, so we design the "NewMasterServer" to register only one type of pb server.
func NewMasterServer(addr string, server pb.MasterServer) (GrpcServer, error) {
	container.mu.Lock()
	defer container.mu.Unlock()
	_, ok := container.servers[addr]
	if ok {
		return nil, errors.New("addr " + addr + " has been listened")
	}
	newServer := &masterServer{&baseServer{addr}, server}
	container.servers[addr] = newServer
	return newServer, nil
}

func NewExecutorServer(addr string, server pb.ExecutorServer) (GrpcServer, error) {
	container.mu.Lock()
	defer container.mu.Unlock()
	_, ok := container.servers[addr]
	if ok {
		return nil, errors.New("addr " + addr + " has been listened")
	}
	newServer := &executorServer{&baseServer{addr}, server}
	container.servers[addr] = newServer
	return newServer, nil
}

type Conn interface {
	Close() error
	sendRequest(ctx context.Context, req interface{}) (interface{}, error)
}

func ResetGrpcCtx() {
	container = &grpcContainer{
		servers: make(map[string]GrpcServer),
	}
}
