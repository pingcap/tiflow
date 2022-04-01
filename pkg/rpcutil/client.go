package rpcutil

import (
	"context"
	"strings"
	"sync"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

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
	leader      string
	clientsLock sync.RWMutex
	clients     map[string]*clientHolder[T] // addr -> clientHolder
	dialer      DialFunc[T]
}

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
	if err != nil {
		return nil, err
	}
	// leader will be updated on heartbeat
	for k := range ret.clients {
		ret.leader = k
		break
	}
	return ret, nil
}

func (c *FailoverRPCClients[T]) init(ctx context.Context, urls []string) error {
	c.UpdateClients(ctx, urls, "")
	if len(c.clients) == 0 {
		return errors.ErrGrpcBuildConn.GenWithStack("failed to dial to master, urls: %v", urls)
	}
	return nil
}

// UpdateClients receives a list of rpc server addresses, dials to server that is
// not maintained in current FailoverRPCClients and close redundant clients.
func (c *FailoverRPCClients[T]) UpdateClients(ctx context.Context, urls []string, leaderURL string) {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()

	c.leader = leaderURL

	notFound := make(map[string]struct{}, len(c.clients))
	for addr := range c.clients {
		notFound[addr] = struct{}{}
	}

	for _, addr := range urls {
		// TODO: refine address with and without scheme
		addr = strings.Replace(addr, "http://", "", 1)
		delete(notFound, addr)

		if _, ok := c.clients[addr]; !ok {
			log.L().Info("add new server master client", zap.String("addr", addr))
			cli, conn, err := c.dialer(ctx, addr)
			if err != nil {
				log.L().Warn("dial to server master failed", zap.String("addr", addr), zap.Error(err))
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
			log.L().Warn("close server master client failed", zap.String("addr", k), zap.Error(err))
		}
		delete(c.clients, k)
	}
}

func (c *FailoverRPCClients[T]) Endpoints() []string {
	c.clientsLock.RLock()
	defer c.clientsLock.RUnlock()
	ret := make([]string, 0, len(c.clients))
	for k := range c.clients {
		ret = append(ret, k)
	}
	return ret
}

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

func (c *FailoverRPCClients[T]) GetLeaderClient() T {
	c.clientsLock.RLock()
	defer c.clientsLock.RUnlock()
	leader, ok := c.clients[c.leader]
	if !ok {
		log.L().Panic("leader client not found", zap.String("leader", c.leader))
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

	for _, cli := range clients.clients {
		resp, err = rpc(cli.client, ctx, req)
		if err == nil {
			return resp, nil
		}
	}
	return resp, err
}
