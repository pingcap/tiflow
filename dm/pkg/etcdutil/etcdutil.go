// Copyright 2019 PingCAP, Inc.
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

// learn from https://github.com/pingcap/pd/blob/v3.0.5/pkg/etcdutil/etcdutil.go.

package etcdutil

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/pkg/errorutil"
)

const (
	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultRevokeLeaseTimeout is the maximum amount of time waiting for revoke etcd lease.
	DefaultRevokeLeaseTimeout = 3 * time.Second
)

var etcdDefaultTxnRetryParam = retry.Params{
	RetryCount:         5,
	FirstRetryDuration: time.Second,
	BackoffStrategy:    retry.Stable,
	IsRetryableFn: func(retryTime int, err error) bool {
		return errorutil.IsRetryableEtcdError(err)
	},
}

var etcdDefaultTxnStrategy = retry.FiniteRetryStrategy{}

// CreateClient creates an etcd client with some default config items.
func CreateClient(endpoints []string, tlsCfg *tls.Config) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
		DialTimeout:      DefaultDialTimeout,
		AutoSyncInterval: 30 * time.Second,
		TLS:              tlsCfg,
	})
}

// ListMembers returns a list of internal etcd members.
func ListMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.MemberList(ctx)
}

// AddMember adds an etcd member.
func AddMember(client *clientv3.Client, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.MemberAdd(ctx, peerAddrs)
}

// RemoveMember removes an etcd member by the given id.
func RemoveMember(client *clientv3.Client, id uint64) (*clientv3.MemberRemoveResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	defer cancel()
	return client.MemberRemove(ctx, id)
}

type EtcdOpFunc func(*tcontext.Context, *clientv3.Client) (interface{}, error)

// DoTxnWithRepeatable do multiple etcd operations in one txn with repeatable retry.
// There are two situations that this function can be used:
// 1. The operations are all read operations.
// 2. The operations are all write operations, but write operations tolerate being written to etcd ** at least once **.
// TODO: add unit test to test encountered an retryable error first but then recovered.
func DoTxnWithRepeatable(cli *clientv3.Client, opFunc EtcdOpFunc) (*clientv3.TxnResponse, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), DefaultRequestTimeout)
	defer cancel()
	tctx := tcontext.NewContext(ctx, log.L())

	ret, _, err := etcdDefaultTxnStrategy.Apply(tctx, etcdDefaultTxnRetryParam, func(t *tcontext.Context) (interface{}, error) {
		return opFunc(t, cli)
	})
	if err != nil {
		return nil, 0, err
	}
	if resp, ok := ret.(*clientv3.TxnResponse); ok {
		return resp, resp.Header.Revision, nil
	}
	return nil, 0, nil
}

func ThenOpFunc(ops ...clientv3.Op) EtcdOpFunc {
	return func(tctx *tcontext.Context, cli *clientv3.Client) (interface{}, error) {
		resp, err := cli.Txn(tctx.Ctx).Then(ops...).Commit()
		if err != nil {
			return nil, terror.ErrHAFailTxnOperation.Delegate(err, "txn commit failed")
		}
		return resp, nil
	}
}

func FullOpFunc(cmps []clientv3.Cmp, opsThen, opsElse []clientv3.Op) EtcdOpFunc {
	return func(tctx *tcontext.Context, cli *clientv3.Client) (interface{}, error) {
		failpoint.Inject("ErrNoSpace", func() {
			tctx.L().Info("fail to do ops in etcd", zap.String("failpoint", "ErrNoSpace"))
			failpoint.Return(nil, v3rpc.ErrNoSpace)
		})
		resp, err := cli.Txn(tctx.Ctx).If(cmps...).Then(opsThen...).Else(opsElse...).Commit()
		if err != nil {
			return nil, terror.ErrHAFailTxnOperation.Delegate(err, "txn commit failed")
		}
		return resp, nil
	}
}

// IsRetryableError returns true if the etcd error is retryable to write ** repeatable **.
// // https://github.com/etcd-io/etcd/blob/v3.5.2/client/v3/retry.go#L53
func IsRetryableError(err error) bool {
	err = errors.Cause(err)
	switch err {
	case v3rpc.ErrCompacted, v3rpc.ErrNoLeader, v3rpc.ErrNoSpace, context.DeadlineExceeded:
		return true
	}
	eErr := v3rpc.Error(err)
	if serverErr, ok := eErr.(v3rpc.EtcdError); ok && serverErr.Code() != codes.Unavailable {
		return false
	}
	// only retry if unavailable
	return status.Code(err) == codes.Unavailable
}

// IsLimitedRetryableError check whether error is retryable error for etcd to build again in a limited number of times.
func IsLimitedRetryableError(err error) bool {
	switch errors.Cause(err) {
	case v3rpc.ErrNoSpace, context.DeadlineExceeded:
		return true
	default:
		return false
	}
}
