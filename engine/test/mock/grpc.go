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
	"sync"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

type masterServices interface {
	pb.DiscoveryServer
	pb.TaskSchedulerServer
	pb.JobManagerServer
}

type masterServer struct {
	*baseServer
	masterServices
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
	case *pb.CreateJobRequest:
		return s.server.CreateJob(ctx, x)
	case *pb.HeartbeatRequest:
		return s.server.Heartbeat(ctx, x)
	case *pb.CancelJobRequest:
		return s.server.CancelJob(ctx, x)
	}
	return nil, errors.New("unknown request")
}

type executorServer struct {
	*baseServer
	pb.ExecutorServiceServer
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
	log.Info("server is canceled", zap.String("ip", s.addr))
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

// Close closes executor server conn
func (s *executorServerConn) Close() error {
	return nil
}

// NewExecutorClient returns executor client based on Conn
func NewExecutorClient(conn Conn) pb.ExecutorServiceClient {
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

// Dial dials to gRPC server
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
func NewMasterServer(addr string, server masterServices) (GrpcServer, error) {
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

// NewExecutorServer returns a mock executor gRPC server for given address, if it
// doesn't exist, create a new one
func NewExecutorServer(addr string, server pb.ExecutorServiceServer) (GrpcServer, error) {
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

// Conn is a simple interface that support send gRPC requests and closeable
type Conn interface {
	Close() error
	sendRequest(ctx context.Context, req interface{}) (interface{}, error)
}

// ResetGrpcCtx resets grpc servers
func ResetGrpcCtx() {
	container = &grpcContainer{
		servers: make(map[string]GrpcServer),
	}
}
