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

package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	derror "github.com/pingcap/tiflow/engine/pkg/errors"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Election is an interface that performs leader elections.
type Election interface {
	// Campaign returns only after being elected.
	// Return values:
	// - leaderCtx: a context that is canceled when the current node is no longer the leader.
	// - resign: a function used to resign the leader.
	// - err: indicates an IRRECOVERABLE error during election.
	Campaign(ctx context.Context, selfID NodeID, timeout time.Duration) (leaderCtx context.Context, resignFn context.CancelFunc, err error)
}

// EtcdElectionConfig defines configurations used in etcd election
type EtcdElectionConfig struct {
	TTL    time.Duration
	Prefix EtcdKeyPrefix
}

type (
	// EtcdKeyPrefix alias the key prefix type used in election
	EtcdKeyPrefix = string
	// NodeID alias the node type used in election
	NodeID = string
)

// EtcdElection implements Election and provides a way to elect leaders via Etcd.
type EtcdElection struct {
	etcdClient *clientv3.Client
	election   *concurrency.Election
	session    *concurrency.Session
	rl         *rate.Limiter
}

// NewEtcdElection creates a new EtcdElection instance.
// `ctx` should not be canceled until the EtcdElection is no longer
// needed, resigned from or aborted.
// It is difficult to bind a context to the execution of creating lease
// alone, so the lifetime of `ctx` is expected to last for the whole session.
func NewEtcdElection(
	ctx context.Context,
	etcdClient *clientv3.Client,
	session *concurrency.Session,
	config EtcdElectionConfig,
) (*EtcdElection, error) {
	var sess *concurrency.Session
	if session == nil {
		var err error
		sess, err = concurrency.NewSession(
			etcdClient,
			concurrency.WithContext(ctx),
			concurrency.WithTTL(int(config.TTL.Seconds())))
		if err != nil {
			return nil, derror.ErrMasterEtcdCreateSessionFail.Wrap(err).GenWithStackByArgs()
		}
	} else {
		sess = session
	}

	election := concurrency.NewElection(sess, config.Prefix)
	return &EtcdElection{
		etcdClient: etcdClient,
		election:   election,
		session:    sess,
		rl:         rate.NewLimiter(rate.Every(time.Second), 1 /* burst */),
	}, nil
}

// Campaign tries to campaign to be a leader. If a leader already exists, it blocks.
func (e *EtcdElection) Campaign(ctx context.Context, selfID NodeID, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, nil, derror.ErrMasterEtcdElectionCampaignFail.Wrap(ctx.Err())
		default:
		}

		err := e.rl.Wait(ctx)
		if err != nil {
			// rl.Wait() can return an unnamed error `rate: Wait(n=%d) exceeds limiter's burst %d` if
			// ctx is canceled. This can be very confusing, so we must wrap it here.
			return nil, nil, derror.ErrMasterEtcdElectionCampaignFail.Wrap(err)
		}

		retCtx, resign, err := e.doCampaign(ctx, selfID, timeout)
		if err != nil {
			if errors.Cause(err) != mvcc.ErrCompacted {
				return nil, nil, derror.ErrMasterEtcdElectionCampaignFail.Wrap(err)
			}
			log.L().Warn("campaign for leader failed", zap.Error(err))
			continue
		}
		return retCtx, resign, nil
	}
}

func (e *EtcdElection) doCampaign(ctx context.Context, selfID NodeID, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := e.election.Campaign(cctx, selfID)
	if err != nil {
		return nil, nil, derror.ErrMasterEtcdElectionCampaignFail.Wrap(err)
	}
	retCtx := newLeaderCtx(ctx, e.session)
	resignFn := func() {
		resignCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		defer retCtx.OnResigned()
		err := e.election.Resign(resignCtx)
		if err != nil {
			log.L().Warn("resign leader failed", zap.Error(err))
		}
	}
	return retCtx, resignFn, nil
}

type leaderCtx struct {
	context.Context
	sess     *concurrency.Session
	closeCh  chan struct{}
	innerErr struct {
		sync.RWMutex
		err error
	}

	createDoneChOnce sync.Once
	doneCh           chan struct{}
	goroutineCount   atomic.Int64 // for testing only
}

func newLeaderCtx(parent context.Context, session *concurrency.Session) *leaderCtx {
	return &leaderCtx{
		Context: parent,
		sess:    session,
		closeCh: make(chan struct{}),
	}
}

func (c *leaderCtx) OnResigned() {
	close(c.closeCh)
}

func (c *leaderCtx) Done() <-chan struct{} {
	c.createDoneChOnce.Do(c.createDoneCh)
	return c.doneCh
}

func (c *leaderCtx) createDoneCh() {
	doneCh := make(chan struct{})
	go func() {
		c.goroutineCount.Add(1)
		defer c.goroutineCount.Sub(1)

		// Handles the three situations where the context needs to be canceled.
		select {
		case <-c.Context.Done():
			// the upstream context is canceled
		case <-c.sess.Done():
			// the session goes out
			c.setError(derror.ErrMasterSessionDone.GenWithStackByArgs())
		case <-c.closeCh:
			// we voluntarily resigned
			c.setError(derror.ErrLeaderCtxCanceled.GenWithStackByArgs())
		}
		close(doneCh)
	}()
	c.doneCh = doneCh
}

func (c *leaderCtx) Err() error {
	c.innerErr.RLock()
	defer c.innerErr.RUnlock()
	if c.innerErr.err != nil {
		return c.innerErr.err
	}
	return c.Context.Err()
}

func (c *leaderCtx) setError(err error) {
	c.innerErr.Lock()
	defer c.innerErr.Unlock()
	c.innerErr.err = err
}
