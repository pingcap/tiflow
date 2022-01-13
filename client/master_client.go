package client

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const dialTimeout = 5 * time.Second

type clientHolder struct {
	conn   closeableConnIface
	client pb.MasterClient
}

type MasterClient struct {
	urls        []string
	leader      string
	clientsLock sync.RWMutex
	clients     map[string]*clientHolder
	dialer      dialFunc
}

type dialFunc func(ctx context.Context, addr string) (*clientHolder, error)

var dialImpl = func(ctx context.Context, addr string) (*clientHolder, error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrap(errors.ErrGrpcBuildConn, err)
	}
	return &clientHolder{
		conn:   conn,
		client: pb.NewMasterClient(conn),
	}, nil
}

var mockDialImpl = func(ctx context.Context, addr string) (*clientHolder, error) {
	conn, err := mock.Dial(addr)
	if err != nil {
		return nil, errors.Wrap(errors.ErrGrpcBuildConn, err)
	}
	return &clientHolder{
		conn:   conn,
		client: mock.NewMasterClient(conn),
	}, nil
}

// UpdateClients receives a list of server master addresses, dials to server
// master that is not maintained in current MasterClient.
func (c *MasterClient) UpdateClients(ctx context.Context, urls []string) {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()
	for _, addr := range urls {
		// TODO: refine address with and without scheme
		addr = strings.Replace(addr, "http://", "", 1)
		if _, ok := c.clients[addr]; !ok {
			log.L().Info("add new server master client", zap.String("addr", addr))
			cliH, err := c.dialer(ctx, addr)
			if err != nil {
				log.L().Warn("dial to server master failed", zap.String("addr", addr), zap.Error(err))
				continue
			}
			c.urls = append(c.urls, addr)
			c.clients[addr] = cliH
		}
	}
}

func (c *MasterClient) init(ctx context.Context, urls []string) error {
	c.UpdateClients(ctx, urls)
	if len(c.clients) == 0 {
		return errors.ErrGrpcBuildConn.GenWithStack("failed to dial to master, urls: %v", c.urls)
	}
	return nil
}

func NewMasterClient(ctx context.Context, join []string) (*MasterClient, error) {
	client := &MasterClient{
		clients: make(map[string]*clientHolder),
		dialer:  dialImpl,
	}
	if test.GetGlobalTestFlag() {
		client.dialer = mockDialImpl
	}
	err := client.init(ctx, join)
	if err != nil {
		return nil, err
	}
	// TODO: use correct leader
	client.leader = client.urls[0]
	return client, nil
}

// Endpoints returns current server master addresses
func (c *MasterClient) Endpoints() []string {
	return c.urls
}

// rpcWrap calls rpc to server master via pb.MasterClient in clients one by one,
// until one client returns successfully.
func (c *MasterClient) rpcWrap(ctx context.Context, req interface{}, respPointer interface{}) error {
	pc, _, _, _ := runtime.Caller(1)
	fullMethodName := runtime.FuncForPC(pc).Name()
	methodName := fullMethodName[strings.LastIndexByte(fullMethodName, '.')+1:]

	c.clientsLock.RLock()
	defer c.clientsLock.RUnlock()
	var err error
	for addr, cliH := range c.clients {
		params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
		results := reflect.ValueOf(cliH.client).MethodByName(methodName).Call(params)
		// result's inner types should be (*pb.XXResponse, error), which is same as pb.MasterClient.XXRPCMethod
		reflect.ValueOf(respPointer).Elem().Set(results[0])
		errInterface := results[1].Interface()
		// nil can't pass type conversion, so we handle it separately
		if errInterface == nil {
			err = nil
		} else {
			err = errInterface.(error)
		}
		if err != nil {
			log.L().Error("rpc to server master failed",
				zap.Any("payload", req), zap.String("method", methodName),
				zap.String("addr", addr), zap.Error(err),
			)
		} else {
			return nil
		}
	}
	// return the last error returned from rpc call
	return err
}

// Heartbeat wraps Heartbeat rpc to master-server.
func (c *MasterClient) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest, timeout time.Duration) (resp *pb.HeartbeatResponse, err error) {
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err = c.rpcWrap(ctx1, req, &resp)
	return
}

// RegisterExecutor to master-server.
func (c *MasterClient) RegisterExecutor(ctx context.Context, req *pb.RegisterExecutorRequest, timeout time.Duration) (resp *pb.RegisterExecutorResponse, err error) {
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err = c.rpcWrap(ctx1, req, &resp)
	return
}

func (c *MasterClient) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) (resp *pb.SubmitJobResponse, err error) {
	err = c.rpcWrap(ctx, req, &resp)
	return
}

func (c *MasterClient) PauseJob(ctx context.Context, req *pb.PauseJobRequest) (resp *pb.PauseJobResponse, err error) {
	err = c.rpcWrap(ctx, req, &resp)
	return
}

func (c *MasterClient) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (resp *pb.CancelJobResponse, err error) {
	err = c.rpcWrap(ctx, req, &resp)
	return
}

func (c *MasterClient) QueryMetaStore(
	ctx context.Context, req *pb.QueryMetaStoreRequest, timeout time.Duration,
) (resp *pb.QueryMetaStoreResponse, err error) {
	ctx1, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err = c.rpcWrap(ctx1, req, &resp)
	return
}

// ScheduleTask sends TaskSchedulerRequest to server master and master
// will ask resource manager for resource and allocates executors to given tasks
func (c *MasterClient) ScheduleTask(
	ctx context.Context,
	req *pb.TaskSchedulerRequest,
	timeout time.Duration,
) (resp *pb.TaskSchedulerResponse, err error) {
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()
	err = c.rpcWrap(ctx1, req, &resp)
	return
}

func (c *MasterClient) ReportExecutorWorkload(
	ctx context.Context,
	req *pb.ExecWorkloadRequest,
) (resp *pb.ExecWorkloadResponse, err error) {
	err = c.rpcWrap(ctx, req, &resp)
	return
}

// Close closes underlying resources
func (c *MasterClient) Close() (err error) {
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

// GetLeaderClient exposes pb.MasterClient, note this can be used when c.leader
// is up to date.
func (c *MasterClient) GetLeaderClient() pb.MasterClient {
	c.clientsLock.RLock()
	defer c.clientsLock.RUnlock()
	leader, ok := c.clients[c.leader]
	if !ok {
		log.L().Panic("leader client not found", zap.String("leader", c.leader))
	}
	return leader.client
}
