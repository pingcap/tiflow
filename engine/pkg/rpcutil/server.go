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
	"encoding/json"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Member stores server member information
// TODO: make it a protobuf field and can be shared by gRPC API
type Member struct {
	Name          string `json:"name"`
	AdvertiseAddr string `json:"advertise-addr"`
	IsLeader      bool   `json:"is-leader"`
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

// LeaderClientWithLock encapsulates a thread-safe rpc client
type LeaderClientWithLock[T RPCClientType] struct {
	mu      sync.RWMutex
	inner   *T
	closeFn func()
}

// Get returns internal FailoverRPCClients
func (l *LeaderClientWithLock[T]) Get() (ret T, ok bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.inner == nil {
		ok = false
		return
	}
	return *l.inner, true
}

// Set sets internal FailoverRPCClients to given value
func (l *LeaderClientWithLock[T]) Set(c T, closeFn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inner = &c
	l.closeFn = closeFn
}

// Close closes internal FailoverRPCClients
func (l *LeaderClientWithLock[T]) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inner != nil {
		if l.closeFn != nil {
			l.closeFn()
			l.closeFn = nil
		}
		l.inner = nil
	}
}

// rpcLimiter is a customized rate limiter, which delegates Allow of rate.Limiter,
// and provides an allow list with a higher priority.
type rpcLimiter struct {
	limiter   *rate.Limiter
	allowList []string
}

func newRPCLimiter(limiter *rate.Limiter, allowList []string) *rpcLimiter {
	return &rpcLimiter{
		limiter:   limiter,
		allowList: allowList,
	}
}

func (rl *rpcLimiter) Allow(methodName string) bool {
	for _, name := range rl.allowList {
		if name == methodName {
			return true
		}
	}
	return rl.limiter.Allow()
}

// PreRPCHook provides some common functionality that should be executed before
// some RPC, like "forward to leader", "checking rate limit". It should be embedded
// into an RPC server struct and call PreRPCHook.PreRPC() for every RPC method.
//
// The type parameter T is the type of RPC client that implements "forward to leader".
type PreRPCHook interface {
	PreRPC(
		ctx context.Context,
		req interface{},
		respPointer interface{},
	) (shouldRet bool, err error)

	CheckLeader() (leader *Member, exist bool)
}

// preRPCHookImpl implements PreRPCHook.
type preRPCHookImpl[T RPCClientType] struct {
	// forward to leader
	id        string        // used to compare with leader.Name, to know if this is the leader
	leader    *atomic.Value // should be a Member
	leaderCli *LeaderClientWithLock[T]

	// check server initialized
	initialized *atomic.Bool

	// rate limiter
	limiter *rpcLimiter
}

// NewPreRPCHook creates a new preRPCHookImpl
func NewPreRPCHook[T RPCClientType](
	id string,
	leader *atomic.Value,
	leaderCli *LeaderClientWithLock[T],
	initialized *atomic.Bool,
	limiter *rate.Limiter,
	rpcLimiterAllowList []string,
) PreRPCHook {
	rpcLim := newRPCLimiter(limiter, rpcLimiterAllowList)
	return &preRPCHookImpl[T]{
		id:          id,
		leader:      leader,
		leaderCli:   leaderCli,
		initialized: initialized,
		limiter:     rpcLim,
	}
}

// PreRPC can do these common works:
//   - forward to leader
//     the `req` argument must fit with the caller of PreRPC which is an RPC.
//     the `respPointer` argument must be a pointer to the response and the response
//     must fit with the caller of PreRPC which is an RPC.
//   - check if the server is initialized
//   - rate limit
//
// TODO: we can build a (req type -> resp type) map at compile time, to avoid passing
// in respPointer.
func (h preRPCHookImpl[T]) PreRPC(
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

	shouldRet, err = h.checkInitialized(respPointer)
	return
}

func (h preRPCHookImpl[T]) logRateLimit(methodName string, req interface{}) {
	// TODO: rate limiter based on different sender
	if h.limiter.Allow(methodName) {
		log.Info("", zap.Any("payload", req), zap.String("request", methodName))
	}
}

func (h preRPCHookImpl[T]) checkInitialized(respPointer interface{}) (shouldRet bool, err error) {
	if h.initialized.Load() {
		return false, nil
	}

	respStruct := reflect.ValueOf(respPointer).Elem().Elem()
	errField := respStruct.FieldByName("Err")
	if !errField.IsValid() {
		return true, errors.ErrMasterNotInitialized.GenWithStackByArgs()
	}

	errField.Set(reflect.ValueOf(&pb.Error{
		Code: pb.ErrorCode_MasterNotReady,
	}))
	return true, nil
}

func (h preRPCHookImpl[T]) forwardToLeader(
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
		inner, ok := h.leaderCli.Get()
		if !ok {
			return true, errors.ErrMasterRPCNotForward.GenWithStackByArgs()
		}

		params := []reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(req)}
		results := reflect.ValueOf(inner).
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

func (h preRPCHookImpl[T]) isLeaderAndNeedForward(ctx context.Context) (isLeader, needForward bool) {
	leader, exist := h.CheckLeader()
	// leader is nil, retry for 3 seconds
	if !exist {
		retry := 10
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()

		for !exist {
			if retry == 0 {
				log.Error("leader is not found, please retry later")
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

	isLeader = false
	_, needForward = h.leaderCli.Get()
	if leader == nil {
		return
	}
	isLeader = leader.Name == h.id
	return
}

func (h preRPCHookImpl[T]) CheckLeader() (leader *Member, exist bool) {
	lp := h.leader.Load()
	if lp == nil {
		return
	}
	leader = lp.(*Member)
	exist = leader.Name != ""
	return
}
