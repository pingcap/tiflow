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
	"sync"

	"github.com/modern-go/reflect2"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/proto/p2p"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	gRPCPeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ServerWrapper implements a CDCPeerToPeerServer, and it
// maintains an inner CDCPeerToPeerServer instance that can
// be replaced as needed.
type ServerWrapper struct {
	rwMu        sync.RWMutex
	innerServer p2p.CDCPeerToPeerServer
}

func NewResettableServer() *ServerWrapper {
	return &ServerWrapper{}
}

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

	return s.innerServer.SendMessage(stream)
}

func (s *ServerWrapper) Reset(inner p2p.CDCPeerToPeerServer) {
	s.rwMu.Lock()
	defer s.rwMu.Unlock()
	// reflect2.IsNil handles two cases for us:
	// 1) null value
	// 2) an interface with a null value but a not-null type info.
	if reflect2.IsNil(inner) {
		s.innerServer = nil
		return
	}
	s.innerServer = inner
}
