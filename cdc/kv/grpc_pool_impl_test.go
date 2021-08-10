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

package kv

import (
	"context"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

// Use etcdSuite for some special reasons, the embed etcd uses zap as the only candidate
// logger and in the logger initializtion it also initializes the grpclog/loggerv2, which
// is not a thread-safe operation and it must be called before any gRPC functions
// ref: https://github.com/grpc/grpc-go/blob/master/grpclog/loggerv2.go#L67-L72
func (s *etcdSuite) TestConnArray(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewGrpcPoolImpl(&security.Credential{})
	defer pool.Close()
	addr := "127.0.0.1:20161"
	tableID := int64(53)
	conn, err := pool.GetConn(ctx, addr, tableID)
	c.Assert(err, check.IsNil)
	c.Assert(conn.active, check.Equals, int64(1))
	pool.ReleaseConn(conn, addr, tableID)
	c.Assert(conn.active, check.Equals, int64(0))

	lastConn := conn
	// First defaultCapacity*2 connections will use initial two connections.
	for i := 0; i < defaultCapacity*2; i++ {
		conn, err := pool.GetConn(ctx, addr, tableID)
		c.Assert(err, check.IsNil)
		c.Assert(lastConn.ClientConn, check.Not(check.Equals), conn.ClientConn)
		c.Assert(conn.active, check.Equals, int64(i)/2+1)
		lastConn = conn
	}
	// The following defaultCapacity*2 connections will trigger resize of connection array.
	for i := 0; i < defaultCapacity*2; i++ {
		conn, err := pool.GetConn(ctx, addr, tableID)
		c.Assert(err, check.IsNil)
		c.Assert(lastConn.ClientConn, check.Not(check.Equals), conn.ClientConn)
		c.Assert(conn.active, check.Equals, int64(i)/2+1)
		lastConn = conn
	}
}
