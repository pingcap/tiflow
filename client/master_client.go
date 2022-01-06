package client

import (
	"context"
	"reflect"
	"runtime"
	"strings"
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
	urls    []string
	leader  string
	clients map[string]*clientHolder
}

func (c *MasterClient) init(ctx context.Context) error {
	log.L().Logger.Info("dialing master", zap.Strings("urls", c.urls))
	for _, addr := range c.urls {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.L().Warn("dial to one of server master failed", zap.String("addr", addr))
			cancel()
			continue
		}
		cancel()
		c.clients[addr] = &clientHolder{
			conn:   conn,
			client: pb.NewMasterClient(conn),
		}
	}
	if len(c.clients) == 0 {
		return errors.ErrGrpcBuildConn.GenWithStack("failed to dial to master, urls: %v", c.urls)
	}
	return nil
}

func (c *MasterClient) initForTest(_ context.Context) error {
	log.L().Logger.Info("dialing master", zap.String("leader", c.leader))
	for _, addr := range c.urls {
		conn, err := mock.Dial(addr)
		if err != nil {
			log.L().Warn("mock dial to one of server master failed", zap.String("addr", addr))
			continue
		}
		c.clients[addr] = &clientHolder{
			conn:   conn,
			client: mock.NewMasterClient(conn),
		}
	}
	if len(c.clients) == 0 {
		return errors.ErrGrpcBuildConn.GenWithStack("failed to dial to server master, urls: %v", c.urls)
	}
	return nil
}

func NewMasterClient(ctx context.Context, join []string) (*MasterClient, error) {
	client := &MasterClient{
		urls:    join,
		clients: make(map[string]*clientHolder),
	}
	client.leader = client.urls[0]
	var err error
	if test.GlobalTestFlag {
		err = client.initForTest(ctx)
	} else {
		err = client.init(ctx)
	}
	if err != nil {
		return nil, err
	}
	return client, nil
}

// rpcWrap calls rpc to server master via pb.MasterClient in clients one by one,
// until one client returns successfully.
func (c *MasterClient) rpcWrap(ctx context.Context, req interface{}, respPointer interface{}) error {
	pc, _, _, _ := runtime.Caller(1)
	fullMethodName := runtime.FuncForPC(pc).Name()
	methodName := fullMethodName[strings.LastIndexByte(fullMethodName, '.')+1:]

	var err error
	for _, cliH := range c.clients {
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
				zap.Error(err),
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
	leader, ok := c.clients[c.leader]
	if !ok {
		log.L().Panic("leader client not found", zap.String("leader", c.leader))
	}
	return leader.client
}
