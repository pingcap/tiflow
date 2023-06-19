// Copyright 2021 PingCAP, Inc.
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

package p2p

import (
	"context"
	"sync"

	"github.com/modern-go/reflect2"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/proto/p2p"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	gRPCPeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type streamWrapper struct {
	MessageServerStream
	ctx    context.Context
	cancel context.CancelFunc
}

func wrapStream(stream MessageServerStream) *streamWrapper {
	ctx, cancel := context.WithCancel(stream.Context())
	return &streamWrapper{
		MessageServerStream: stream,
		ctx:                 ctx,
		cancel:              cancel,
	}
}

func (w *streamWrapper) Context() context.Context {
	return w.ctx
}

// ServerWrapper implements a CDCPeerToPeerServer, and it
// maintains an inner CDCPeerToPeerServer instance that can
// be replaced as needed.
type ServerWrapper struct {
	rwMu        sync.RWMutex
	innerServer p2p.CDCPeerToPeerServer
	cfg         *MessageServerConfig

	wrappedStreamsMu sync.Mutex
	wrappedStreams   map[*streamWrapper]struct{}
}

// NewServerWrapper creates a new ServerWrapper
func NewServerWrapper(cfg *MessageServerConfig) *ServerWrapper {
	return &ServerWrapper{
		wrappedStreams: map[*streamWrapper]struct{}{},
		cfg:            cfg,
	}
}

// ServerOptions returns server option for creating grpc servers.
func (s *ServerWrapper) ServerOptions() []grpc.ServerOption {
	keepaliveParams := keepalive.ServerParameters{
		Time:    s.cfg.KeepAliveTime,
		Timeout: s.cfg.KeepAliveTimeout,
	}
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(s.cfg.MaxRecvMsgSize),
		grpc.KeepaliveParams(keepaliveParams),
	}
}

// SendMessage implements p2p.CDCPeerToPeerServer
func (s *ServerWrapper) SendMessage(stream p2p.CDCPeerToPeer_SendMessageServer) error {
	s.rwMu.RLock()
	innerServer := s.innerServer
	s.rwMu.RUnlock()

	if innerServer == nil {
		var addr string
		peer, ok := gRPCPeer.FromContext(stream.Context())
		if ok {
			addr = peer.Addr.String()
		}
		log.Debug("gRPC server received request while CDC capture is not running.", zap.String("addr", addr))
		return status.New(codes.Unavailable, "CDC capture is not running").Err()
	}

	wrappedStream := wrapStream(stream)
	s.wrappedStreamsMu.Lock()
	s.wrappedStreams[wrappedStream] = struct{}{}
	s.wrappedStreamsMu.Unlock()
	defer func() {
		s.wrappedStreamsMu.Lock()
		delete(s.wrappedStreams, wrappedStream)
		s.wrappedStreamsMu.Unlock()
		wrappedStream.cancel()
	}()

	// Used in unit tests to simulate a race situation between `SendMessage` and `Reset`.
	// TODO think of another way to make tests parallelizable.
	failpoint.Inject("ServerWrapperSendMessageDelay", func() {})
	return innerServer.SendMessage(wrappedStream)
}

// Reset resets the inner server object in the ServerWrapper
func (s *ServerWrapper) Reset(inner p2p.CDCPeerToPeerServer) {
	s.rwMu.Lock()
	defer s.rwMu.Unlock()

	s.wrappedStreamsMu.Lock()
	defer s.wrappedStreamsMu.Unlock()

	for wrappedStream := range s.wrappedStreams {
		wrappedStream.cancel()
	}

	// reflect2.IsNil handles two cases for us:
	// 1) null value
	// 2) an interface with a null value but a not-null type info.
	if reflect2.IsNil(inner) {
		s.innerServer = nil
		return
	}
	s.innerServer = inner
}
