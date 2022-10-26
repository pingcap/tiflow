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

package rpcutil

import (
	"context"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const defaultDialRetry = 3

// CloseableConnIface defines an interface that supports Close(release resource)
type CloseableConnIface interface {
	Close() error
}

// FailoverRPCClientType should be limited to rpc Client types, but golang can't
// let us do it. So we left an alias to any.
type FailoverRPCClientType any

// DialFunc returns a RPC client and it's underlying connection for closing.
type DialFunc[T FailoverRPCClientType] func(ctx context.Context, addr string) (T, CloseableConnIface, error)

// clientHolder groups a RPC client and it's closing function.
type clientHolder[T FailoverRPCClientType] struct {
	conn   CloseableConnIface
	client T
}

// FailoverRPCClients represent RPC on this type of client can use any client
// to connect to the server.
type FailoverRPCClients[T FailoverRPCClientType] struct {
	leader      string // a key in clients. When clients is empty, this key is ""
	clientsLock sync.RWMutex
	clients     map[string]*clientHolder[T] // addr -> clientHolder
	dialer      DialFunc[T]
}

// NewFailoverRPCClients creates a FailoverRPCClients and initializes it
func NewFailoverRPCClients[T FailoverRPCClientType](
	ctx context.Context,
	urls []string,
	dialer DialFunc[T],
) (*FailoverRPCClients[T], error) {
	ret := &FailoverRPCClients[T]{
		clients: make(map[string]*clientHolder[T]),
		dialer:  dialer,
	}
	err := ret.init(ctx, urls)
	return ret, err
}

func (c *FailoverRPCClients[T]) init(ctx context.Context, urls []string) error {
	// leader will be updated on heartbeat
	c.UpdateClients(ctx, urls, "")
	if len(c.clients) == 0 {
		return errors.ErrGrpcBuildConn.GenWithStack("failed to dial to master, urls: %v", urls)
	}
	return nil
}

// UpdateClients receives a list of rpc server addresses, dials to server that is
// not maintained in current FailoverRPCClients and close redundant clients. If
// the dialing fails, the client will not be added to FailoverRPCClients.
// if leaderURL is empty or fail to dial leader, it will pick any one as leader.
func (c *FailoverRPCClients[T]) UpdateClients(ctx context.Context, urls []string, leaderURL string) {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()

	trimURL := func(url string) string {
		return strings.Replace(url, "http://", "", 1)
	}

	notFound := make(map[string]struct{}, len(c.clients))
	for addr := range c.clients {
		notFound[addr] = struct{}{}
	}

	for _, addr := range urls {
		// TODO: refine address with and without scheme
		addr = trimURL(addr)
		delete(notFound, addr)

		if _, ok := c.clients[addr]; !ok {
			log.Info("add new server master client", zap.String("addr", addr))

			var (
				cli  T
				conn CloseableConnIface
			)
			err := retry.Do(ctx, func() (err error) {
				cli, conn, err = c.dialer(ctx, addr)
				return err
			}, retry.WithMaxTries(defaultDialRetry))
			if err != nil {
				log.Warn("dial to server master failed", zap.String("addr", addr), zap.Error(err))
				continue
			}

			c.clients[addr] = &clientHolder[T]{
				conn:   conn,
				client: cli,
			}
		}
	}

	for k := range notFound {
		if err := c.clients[k].conn.Close(); err != nil {
			log.Warn("close server master client failed", zap.String("addr", k), zap.Error(err))
		}
		delete(c.clients, k)
	}

	c.leader = trimURL(leaderURL)
	// we also handle "" because "" is not in clients map
	if _, ok := c.clients[c.leader]; !ok {
		// reset leader first, note that when c.clients is empty, c.leader is ""
		c.leader = ""
		for k := range c.clients {
			c.leader = k
			break
		}
	}
}

// Endpoints returns a slice of all client endpoints
func (c *FailoverRPCClients[T]) Endpoints() []string {
	c.clientsLock.RLock()
	defer c.clientsLock.RUnlock()
	ret := make([]string, 0, len(c.clients))
	for k := range c.clients {
		ret = append(ret, k)
	}
	return ret
}

// Close closes connection underlying
func (c *FailoverRPCClients[T]) Close() (err error) {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()
	for _, cliH := range c.clients {
		err1 := cliH.conn.Close()
		if err1 != nil {
			err = err1
		}
	}
	return
}

// GetLeaderClient tries to return the client of the leader.
// When leader is unavailable it may return a leader to other server. When all
// servers are unavailable, it will return zero value.
func (c *FailoverRPCClients[T]) GetLeaderClient() T {
	var zeroT T

	c.clientsLock.RLock()
	defer c.clientsLock.RUnlock()
	if c.leader == "" {
		return zeroT
	}

	leader, ok := c.clients[c.leader]
	if !ok {
		log.Panic("leader client not found", zap.String("leader", c.leader))
	}
	return leader.client
}

// DoFailoverRPC calls RPC on given clients one by one until one succeeds.
// It should be a method of FailoverRPCClients, but golang can't let us do it, so
// we use a public function.
func DoFailoverRPC[
	C FailoverRPCClientType,
	Req any,
	Resp any,
	F func(C, context.Context, Req, ...grpc.CallOption) (Resp, error),
](
	ctx context.Context,
	clients *FailoverRPCClients[C],
	req Req,
	rpc F,
) (resp Resp, err error) {
	clients.clientsLock.RLock()
	defer clients.clientsLock.RUnlock()

	if len(clients.clients) == 0 {
		return resp, errors.ErrNoRPCClient.GenWithStack("rpc: %#v, request: %#v", rpc, req)
	}

	// As long as len(clients.clients) > 0, leaderCli is impossible to be nil.
	leaderCli := clients.GetLeaderClient()
	resp, err = rpc(leaderCli, ctx, req)
	if err == nil {
		return resp, nil
	}

	for k, cli := range clients.clients {
		// Skip the leader since we've already tried it.
		if k == clients.leader {
			continue
		}
		resp, err = rpc(cli.client, ctx, req)
		if err == nil {
			return resp, nil
		}
	}
	// return the last error
	return resp, err
}

// NewFailoverRPCClientsForTest creates a FailoverRPCClients for test
func NewFailoverRPCClientsForTest[T FailoverRPCClientType](
	client T,
) *FailoverRPCClients[T] {
	return &FailoverRPCClients[T]{
		leader: "leader",
		clients: map[string]*clientHolder[T]{
			"leader": {
				client: client,
			},
		},
	}
}
