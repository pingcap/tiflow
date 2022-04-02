package rpcutil

import (
	"context"
	"encoding/json"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Member stores server member information
// TODO: make it a protobuf field and can be shared by gRPC API
type Member struct {
	IsServLeader  bool   `json:"is-serv-leader"`
	IsEtcdLeader  bool   `json:"is-etcd-leader"`
	Name          string `json:"name"`
	AdvertiseAddr string `json:"advertise-addr"`
}

// String implements json marshal
func (m *Member) String() (string, error) {
	b, err := json.Marshal(m)
	return string(b), err
}

// Unmarshal unmarshals data into a member
func (m *Member) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// RPCClientType should be limited to rpc Client types, but golang can't
// let us do it. So we left an alias to any.
type RPCClientType any

type LeaderClientWithLock[T RPCClientType] struct {
	sync.RWMutex
	Inner *FailoverRPCClients[T]
}

// PreRPCHooker provides some common functionality that should be executed before
// some RPC, like "forward to leader", "checking rate limit". It should be embedded
// into an RPC server struct and call PreRPCHooker.PreRPC() for every RPC method.
//
// The type parameter T is the type of RPC client that implements "forward to leader".
type PreRPCHooker[T RPCClientType] struct {
	// forward to leader
	id        string        // used to compare with leader.Name, to know if this is the leader
	leader    *atomic.Value // should be a Member
	leaderCli *LeaderClientWithLock[T]

	// check server initialized
	initialized *atomic.Bool

	// rate limiter
	limiter *rate.Limiter
}

func NewPreRPCHooker[T RPCClientType](
	id string,
	leader *atomic.Value,
	leaderCli *LeaderClientWithLock[T],
	initialized *atomic.Bool,
	limiter *rate.Limiter,
) *PreRPCHooker[T] {
	return &PreRPCHooker[T]{
		id:          id,
		leader:      leader,
		leaderCli:   leaderCli,
		initialized: initialized,
		limiter:     limiter,
	}
}

// PreRPC can do these common works:
// - forward to leader
//   the `req` argument must fit with the caller of PreRPC which is an RPC.
//   the `respPointer` argument must be a pointer to the response and the response
//   must fit with the caller of PreRPC which is an RPC.
// - check if the server is initialized
// - rate limit
// TODO: we can build a (req type -> resp type) map at compile time, to avoid passing
// in respPointer.
func (h PreRPCHooker[T]) PreRPC(
	ctx context.Context,
	req interface{},
	respPointer interface{},
) (shouldRet bool, err error) {
	pc, _, _, _ := runtime.Caller(1)
	fullMethodName := runtime.FuncForPC(pc).Name()
	methodName := fullMethodName[strings.LastIndexByte(fullMethodName, '.')+1:]

	h.logRateLimit(methodName, req)

	shouldRet, err = h.forwardToLeader(ctx, methodName, req, respPointer)
	if shouldRet {
		return
	}

	shouldRet = h.checkInitialized(respPointer)
	return
}

func (h PreRPCHooker[T]) logRateLimit(methodName string, req interface{}) {
	// TODO: rate limiter based on different sender
	if h.limiter.Allow() {
		log.L().Info("", zap.Any("payload", req), zap.String("request", methodName))
	}
}

func (h PreRPCHooker[T]) checkInitialized(respPointer interface{}) (shouldRet bool) {
	if !h.initialized.Load() {
		errInRefelct := reflect.ValueOf(&pb.Error{
			Code: pb.ErrorCode_MasterNotReady,
		})
		reflect.ValueOf(respPointer).Elem().FieldByName("Err").Set(errInRefelct)
		return true
	}
	return false
}

func (h PreRPCHooker[T]) forwardToLeader(
	ctx context.Context,
	methodName string,
	req interface{},
	respPointer interface{},
) (shouldRet bool, err error) {
	isLeader, needForward := h.isLeaderAndNeedForward(ctx)
	if isLeader {
		return false, nil
	}
	if needForward {
		h.leaderCli.RLock()
		inner := h.leaderCli.Inner
		h.leaderCli.RUnlock()
		if inner == nil {
			return true, errors.ErrMasterRPCNotForward.GenWithStackByArgs()
		}

		params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
		results := reflect.ValueOf(inner.GetLeaderClient()).
			MethodByName(methodName).
			Call(params)
		// result's inner types should be (*pb.XXResponse, error), which is same as s.leaderClient.XXRPCMethod
		reflect.ValueOf(respPointer).Elem().Set(results[0])
		errInterface := results[1].Interface()
		// nil can't pass type conversion, so we handle it separately
		if errInterface == nil {
			err = nil
		} else {
			err = errInterface.(error)
		}
		return true, err
	}
	return true, errors.ErrMasterRPCNotForward.GenWithStackByArgs()
}

func (h PreRPCHooker[T]) isLeaderAndNeedForward(ctx context.Context) (isLeader, needForward bool) {
	leader, exist := h.CheckLeader()
	// leader is nil, retry for 3 seconds
	if !exist {
		retry := 10
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for !exist {
			if retry == 0 {
				log.L().Error("leader is not found, please retry later")
				return false, false
			}
			select {
			case <-ctx.Done():
				return false, false
			case <-ticker.C:
				retry--
			}
			leader, exist = h.CheckLeader()
		}
	}
	isLeader = leader.Name == h.id
	h.leaderCli.RLock()
	needForward = h.leaderCli.Inner != nil
	h.leaderCli.RUnlock()
	return
}

func (h PreRPCHooker[T]) CheckLeader() (leader *Member, exist bool) {
	lp := h.leader.Load()
	if lp == nil {
		return
	}
	leader = lp.(*Member)
	exist = leader.Name != ""
	return
}
