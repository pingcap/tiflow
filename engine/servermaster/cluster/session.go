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
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	derrors "github.com/pingcap/tiflow/pkg/errors"
)

const (
	defaultSessionTTL = 5 * time.Second
)

// Session is recognized as lease and election manager of a server master node.
type Session interface {
	Campaign(
		ctx context.Context, timeout time.Duration,
	) (context.Context, context.CancelFunc, error)
	Reset(ctx context.Context) error
	CheckNeedReset(err error) bool
}

// EtcdSession implements cluster.Session based on etcd session and election
type EtcdSession struct {
	etcdClient *clientv3.Client
	session    *concurrency.Session
	election   Election
	config     *EtcdSessionConfig
}

// EtcdSessionConfig defines basic config used to create an EtcdSession
type EtcdSessionConfig struct {
	// Member is json encoded string of rpcutil.Member
	Member string

	// Key and Value pair are used in service discovery, and they are stored in etcd
	Key   string
	Value string

	// etcd session ttl
	KeepaliveTTL time.Duration
}

// NewEtcdSession creates a new EtcdSession instance
func NewEtcdSession(
	ctx context.Context, etcdClient *clientv3.Client, config *EtcdSessionConfig,
) (*EtcdSession, error) {
	s := &EtcdSession{
		etcdClient: etcdClient,
		config:     config,
	}
	err := s.Reset(ctx)
	return s, err
}

// Reset implements Session.Reset
func (s *EtcdSession) Reset(ctx context.Context) error {
	session, err := concurrency.NewSession(
		s.etcdClient, concurrency.WithTTL(int(defaultSessionTTL.Seconds())))
	if err != nil {
		return derrors.Wrap(derrors.ErrMasterNewServer, err)
	}

	_, err = s.etcdClient.Put(ctx, s.config.Key, s.config.Value,
		clientv3.WithLease(session.Lease()))
	if err != nil {
		return derrors.Wrap(derrors.ErrEtcdAPIError, err)
	}

	election, err := NewEtcdElection(ctx, s.etcdClient, session, EtcdElectionConfig{
		TTL:    s.config.KeepaliveTTL,
		Prefix: adapter.MasterCampaignKey.Path(),
	})
	if err != nil {
		return err
	}

	s.session = session
	s.election = election
	return nil
}

// Campaign implements Session.Campaign
func (s *EtcdSession) Campaign(ctx context.Context, timeout time.Duration) (
	context.Context, context.CancelFunc, error,
) {
	log.L().Info("start to campaign server master leader",
		zap.String("name", s.config.Member))
	leaderCtx, resignFn, err := s.election.Campaign(ctx, s.config.Member, timeout)
	switch errors.Cause(err) {
	case nil:
	case context.Canceled:
		return nil, nil, ctx.Err()
	default:
		log.L().Warn("campaign leader failed", zap.Error(err))
		return nil, nil, derrors.Wrap(derrors.ErrMasterEtcdElectionCampaignFail, err)
	}
	log.L().Info("campaign leader successfully",
		zap.String("name", s.config.Member))
	return leaderCtx, resignFn, nil
}

// CheckNeedReset implements Session.NeedReset
func (s *EtcdSession) CheckNeedReset(err error) (needReset bool) {
	select {
	case <-s.session.Done():
		// detect the life cycle of session ends by active detection
		needReset = true
	default:
		inErr, ok := errors.Cause(err).(rpctypes.EtcdError)
		// meet error `etcdserver: requested lease not found`
		if ok && inErr.Code() == codes.NotFound {
			needReset = true
		}
	}
	return
}
