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

package roles

import (
	"context"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/ticdc/cdc/kv"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	newSessionRetryInterval = 200 * time.Millisecond
	logIntervalCnt          = int(3 * time.Second / newSessionRetryInterval)
	defaultOpTimeout        = 5 * time.Second
)

// Manager is used to campaign the owner and manage the owner information.
type Manager interface {
	// ID returns the ID of the manager.
	ID() string
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
	// RetireOwner make the manager to be a not owner. It's exported for testing.
	RetireOwner()
	// CampaignOwner campaigns the owner.
	CampaignOwner(ctx context.Context) error
	// RetireNotify returns a channel that can fetch notification when owner is retired
	RetireNotify() <-chan struct{}
	// ResignOwner lets the owner to start a new election
	ResignOwner(ctx context.Context) error
}

const (
	// NewSessionDefaultRetryCnt is the default retry times when create new session.
	NewSessionDefaultRetryCnt = 3
	// NewSessionRetryUnlimited is the unlimited retry times when create new session.
	NewSessionRetryUnlimited = math.MaxInt64
)

// ownerManager represents the structure which is used for electing owner.
type ownerManager struct {
	id       string // id is the ID of the manager.
	key      string
	etcdCli  kv.CDCEtcdClient
	elec     unsafe.Pointer
	logger   *zap.Logger
	retireCh chan struct{}
}

// NewOwnerManager creates a new Manager.
func NewOwnerManager(etcdCli kv.CDCEtcdClient, id, key string) Manager {
	logger := log.L().With(zap.String("id", id))

	return &ownerManager{
		etcdCli:  etcdCli,
		id:       id,
		key:      key,
		logger:   logger,
		retireCh: make(chan struct{}, 1),
	}
}

// ID implements Manager.ID interface.
func (m *ownerManager) ID() string {
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *ownerManager) IsOwner() bool {
	return atomic.LoadPointer(&m.elec) != unsafe.Pointer(nil)
}

// ManagerSessionTTLSeconds is the etcd session's TTL in seconds. It's exported for testing.
var ManagerSessionTTLSeconds = 5

// setManagerSessionTTL sets the ManagerSessionTTLSeconds value, it's used for testing.
func setManagerSessionTTL() error {
	ttlStr := os.Getenv("cdc_manager_ttl")
	if len(ttlStr) == 0 {
		return nil
	}
	ttl, err := strconv.Atoi(ttlStr)
	if err != nil {
		return errors.Trace(err)
	}
	ManagerSessionTTLSeconds = ttl
	return nil
}

// NewSession creates a new etcd session.
func NewSession(ctx context.Context, etcdCli kv.CDCEtcdClient, retryCnt, ttl int) (*concurrency.Session, error) {
	var err error

	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		if err = contextDone(ctx, err); err != nil {
			return etcdSession, errors.Trace(err)
		}

		etcdSession, err = concurrency.NewSession(etcdCli.Client,
			concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		if err == nil {
			break
		}
		if failedCnt%logIntervalCnt == 0 {
			log.Warn("failed to new session to etcd", zap.Error(err))
		}

		time.Sleep(newSessionRetryInterval)
		failedCnt++
	}
	return etcdSession, errors.Trace(err)
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *ownerManager) CampaignOwner(ctx context.Context) error {
	session, err := NewSession(ctx, m.etcdCli, NewSessionDefaultRetryCnt, ManagerSessionTTLSeconds)
	if err != nil {
		return errors.Trace(err)
	}
	go m.campaignLoop(ctx, session)
	return nil
}

// ResignOwner implements Manager.ResignOwner interface.
func (m *ownerManager) ResignOwner(ctx context.Context) error {
	elec := (*concurrency.Election)(atomic.LoadPointer(&m.elec))
	if elec == nil {
		return errors.Trace(concurrency.ErrElectionNotLeader)
	}
	cctx, cancel := context.WithTimeout(ctx, defaultOpTimeout)
	err := elec.Resign(cctx)
	defer cancel()
	if err != nil {
		return errors.Trace(err)
	}
	m.logger.Info("resign owner success")
	return nil
}

func (m *ownerManager) toBeOwner(elec *concurrency.Election) {
	atomic.StorePointer(&m.elec, unsafe.Pointer(elec))
}

// RetireOwner make the manager to be a not owner.
func (m *ownerManager) RetireOwner() {
	atomic.StorePointer(&m.elec, nil)
}

func (m *ownerManager) campaignLoop(ctx context.Context, etcdSession *concurrency.Session) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var err error
	for {
		select {
		case <-etcdSession.Done():
			m.logger.Info("etcd session is done, creates a new one")
			leaseID := etcdSession.Lease()
			etcdSession, err = NewSession(ctx, m.etcdCli, NewSessionRetryUnlimited, ManagerSessionTTLSeconds)
			if err != nil {
				m.logger.Info("break campaign loop, NewSession failed", zap.Error(err))
				m.revokeSession(leaseID)
				return
			}
		case <-ctx.Done():
			m.logger.Info("break campaign loop, context is done")
			m.revokeSession(etcdSession.Lease())
			return
		default:
		}
		// If the etcd server turns clocks forward，the following case may occur.
		// The etcd server deletes this session's lease ID, but etcd session doesn't find it.
		// In this time if we do the campaign operation, the etcd server will return ErrLeaseNotFound.
		if terror.ErrorEqual(err, rpctypes.ErrLeaseNotFound) {
			if etcdSession != nil {
				err = etcdSession.Close()
				m.logger.Info("etcd session encounters the error of lease not found, closes it", zap.Error(err))
			}
			continue
		}

		elec := concurrency.NewElection(etcdSession, m.key)
		err = elec.Campaign(ctx, m.id)
		if err != nil {
			m.logger.Error("failed to campaign", zap.Error(err))
			continue
		}

		ownerKey, err := GetOwnerInfo(ctx, elec, m.id)
		if err != nil {
			continue
		}
		m.logger.Info("campaign to be owner")

		m.toBeOwner(elec)
		m.watchOwner(ctx, etcdSession, ownerKey)
		m.RetireOwner()

		m.logger.Warn("lost owner")
	}
}

func (m *ownerManager) revokeSession(leaseID clientv3.LeaseID) {
	// Revoke the session lease.
	// If revoke takes longer than the ttl, lease is expired anyway.
	cancelCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(ManagerSessionTTLSeconds)*time.Second)
	_, err := m.etcdCli.Client.Revoke(cancelCtx, leaseID)
	cancel()
	m.logger.Info("revoke session", zap.Error(err))
}

// GetOwnerInfo check the owner is id and return the owner key.
func GetOwnerInfo(ctx context.Context, elec *concurrency.Election, id string) (string, error) {
	resp, err := elec.Leader(ctx)
	if err != nil {
		// If no leader elected currently, it returns ErrElectionNoLeader.
		log.Info("failed to get leader", zap.Error(err))
		return "", errors.Trace(err)
	}

	ownerID := string(resp.Kvs[0].Value)
	log.Info("get owner", zap.String("ownerID", ownerID))

	if ownerID != id {
		log.Warn("not owner", zap.String("id", id))
		return "", errors.New("ownerInfoNotMatch")
	}

	return string(resp.Kvs[0].Key), nil
}

// RetireNotify implements Manager.RetireNotify
func (m *ownerManager) RetireNotify() <-chan struct{} {
	return m.retireCh
}

func (m *ownerManager) watchOwner(ctx context.Context, etcdSession *concurrency.Session, key string) {
	log.Debug("watch owner key", zap.String("key", key))

	defer func() {
		select {
		case m.retireCh <- struct{}{}:
			log.Debug("lost owner role, send retire notification")
		default:
		}
	}()

	watchCh := m.etcdCli.Client.Watch(ctx, key)
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				m.logger.Info("watcher is closed, no owner")
				return
			}

			if resp.Canceled {
				m.logger.Info("watch canceled, no owner")
				return
			}

			err := resp.Err()
			if err != nil {
				m.logger.Error("watch owner key error", zap.Error(err))
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					m.logger.Info("watch failed, owner is deleted")
					return
				}
			}
		case <-etcdSession.Done():
			return
		case <-ctx.Done():
			return
		}
	}
}

func init() {
	err := setManagerSessionTTL()
	if err != nil {
		log.Warn("set manager session TTL failed", zap.Error(err))
	}
}

func contextDone(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	// Sometime the ctx isn't closed, but the etcd client is closed,
	// we need to treat it as if context is done.
	// TODO: Make sure ctx is closed with etcd client.
	if terror.ErrorEqual(err, context.Canceled) ||
		terror.ErrorEqual(err, context.DeadlineExceeded) ||
		status.Code(errors.Cause(err)) == codes.Canceled {
		return errors.Trace(err)
	}

	return nil
}

// GetOwnerID is a helper function used to get the owner ID.
func GetOwnerID(ctx context.Context, cli kv.CDCEtcdClient, key string) (string, error) {
	resp, err := cli.Client.Get(ctx, key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return "", concurrency.ErrElectionNoLeader
	}
	return string(resp.Kvs[0].Value), nil
}
