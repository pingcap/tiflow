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

package servermaster

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TODO: we can use UpdateClients, don't need to close and re-create it.
func (s *Server) createLeaderClient(addr string) {
	s.closeLeaderClient()

	// TODO support TLS
	credentials := insecure.NewCredentials()
	conn, err := grpc.Dial(
		addr,
		grpc.WithTransportCredentials(credentials),
	)
	if err != nil {
		log.Error("create server master client failed", zap.String("addr", addr), zap.Error(err))
		return
	}
	cli := newMultiClient(conn)
	s.masterCli.Set(cli, func() {
		if err := conn.Close(); err != nil {
			log.Warn("failed to close clinet", zap.String("addr", addr))
		}
	})
}

func (s *Server) closeLeaderClient() {
	s.masterCli.Close()
}

// watchLeader watches the leader change of the server master,
// then update the leader client and current leader for masterRPCHook.
// TODO: Integrate elector, masterRPCHook, masterCli, s.leader.
func (s *Server) watchLeader(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	var leaderAddr string
	for {
		select {
		case <-ticker.C:
			leader, ok := s.elector.GetLeader()
			if ok {
				s.leader.Store(&rpcutil.Member{
					Name:          leader.ID, // FIXME: rpcutil.Member.Name use id as name which is confusing.
					AdvertiseAddr: leader.Address,
					IsLeader:      true,
				})
				if leader.Address != leaderAddr {
					leaderAddr = leader.Address
					s.createLeaderClient(leaderAddr)
				}
			} else {
				s.leader.Store(&rpcutil.Member{})
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
