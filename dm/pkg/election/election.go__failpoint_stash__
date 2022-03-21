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

package election

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

const (
	// newSessionDefaultRetryCnt is the default retry times when creating new session.
	newSessionDefaultRetryCnt = 3
	// newSessionRetryUnlimited is the unlimited retry times when creating new session.
	newSessionRetryUnlimited = math.MaxInt64
	// newSessionRetryInterval is the interval time when retrying to create a new session.
	newSessionRetryInterval = 200 * time.Millisecond

	// IsLeader means current compaigner become leader.
	IsLeader = "isLeader"
	// RetireFromLeader means current compaigner is old leader, and retired.
	RetireFromLeader = "retireFromLeader"
	// IsNotLeader means current compaigner is not old leader and current leader.
	IsNotLeader = "isNotLeader"
)

// CampaignerInfo is the campaigner's information.
type CampaignerInfo struct {
	ID string `json:"id"`
	// addr is the campaigner's advertise address
	Addr string `json:"addr"`
}

func (c *CampaignerInfo) String() string {
	infoBytes, err := json.Marshal(c)
	if err != nil {
		// this should never happened
		return fmt.Sprintf("id: %s, addr: %s", c.ID, c.Addr)
	}

	return string(infoBytes)
}

func getCampaignerInfo(infoBytes []byte) (*CampaignerInfo, error) {
	info := &CampaignerInfo{}
	err := json.Unmarshal(infoBytes, info)
	if err != nil {
		return nil, err
	}
	return info, nil
}

// Election implements the leader election based on etcd.
type Election struct {
	// the Election instance does not own the client instance,
	// so do not close it in the methods of Election.
	cli        *clientv3.Client
	sessionTTL int
	key        string

	info    *CampaignerInfo
	infoStr string

	ech      chan error
	leaderCh chan *CampaignerInfo
	isLeader atomic.Bool

	closed atomic.Bool
	cancel context.CancelFunc

	bgWg sync.WaitGroup

	campaignMu sync.RWMutex

	cancelCampaign func()

	// notifyBlockTime is the max block time for notify leader
	notifyBlockTime time.Duration

	// set evictLeader to true if don't hope this member be leader
	evictLeader atomic.Bool

	resignCh chan struct{}

	l log.Logger
}

// NewElection creates a new etcd leader Election instance and starts the campaign loop.
func NewElection(ctx context.Context, cli *clientv3.Client, sessionTTL int, key, id, addr string, notifyBlockTime time.Duration) (*Election, error) {
	ctx2, cancel2 := context.WithCancel(ctx)
	e := &Election{
		cli:        cli,
		sessionTTL: sessionTTL,
		key:        key,
		info: &CampaignerInfo{
			ID:   id,
			Addr: addr,
		},
		notifyBlockTime: notifyBlockTime,
		leaderCh:        make(chan *CampaignerInfo, 1),
		ech:             make(chan error, 1), // size 1 is enough
		cancel:          cancel2,
		l:               log.With(zap.String("component", "election")),
	}
	e.infoStr = e.info.String()

	// try create a session before enter the campaign loop.
	// so we can detect potential error earlier.
	session, err := e.newSession(ctx, newSessionDefaultRetryCnt)
	if err != nil {
		cancel2()
		return nil, terror.ErrElectionCampaignFail.Delegate(err, "create the initial session")
	}

	e.bgWg.Add(1)
	go func() {
		defer e.bgWg.Done()
		e.campaignLoop(ctx2, session)
	}()
	return e, nil
}

// IsLeader returns whether this member is the leader.
func (e *Election) IsLeader() bool {
	return e.isLeader.Load()
}

// ID returns the current member's ID.
func (e *Election) ID() string {
	return e.info.ID
}

// LeaderInfo returns the current leader's key, ID and address.
// it's similar with https://github.com/etcd-io/etcd/blob/v3.4.3/clientv3/concurrency/election.go#L147.
func (e *Election) LeaderInfo(ctx context.Context) (string, string, string, error) {
	resp, err := e.cli.Get(ctx, e.key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", "", "", terror.ErrElectionGetLeaderIDFail.Delegate(err)
	} else if len(resp.Kvs) == 0 {
		// no leader currently elected
		return "", "", "", terror.ErrElectionGetLeaderIDFail.Delegate(concurrency.ErrElectionNoLeader)
	}

	leaderInfo, err := getCampaignerInfo(resp.Kvs[0].Value)
	if err != nil {
		return "", "", "", terror.ErrElectionGetLeaderIDFail.Delegate(err)
	}

	return string(resp.Kvs[0].Key), leaderInfo.ID, leaderInfo.Addr, nil
}

// LeaderNotify returns a channel that can fetch the leader's information when the member become the leader or retire from the leader, or get a new leader.
// leader's information can be nil which means this member is leader and retire.
func (e *Election) LeaderNotify() <-chan *CampaignerInfo {
	return e.leaderCh
}

// ErrorNotify returns a channel that can fetch errors occurred for campaign.
func (e *Election) ErrorNotify() <-chan error {
	return e.ech
}

// Close closes the election instance and release the resources.
func (e *Election) Close() {
	e.l.Info("election is closing", zap.Stringer("current member", e.info))
	if !e.closed.CAS(false, true) {
		e.l.Info("election was already closed", zap.Stringer("current member", e.info))
		return
	}

	e.cancel()
	e.bgWg.Wait()
	e.l.Info("election is closed", zap.Stringer("current member", e.info))
}

func (e *Election) campaignLoop(ctx context.Context, session *concurrency.Session) {
	closeSession := func(se *concurrency.Session) {
		err2 := se.Close() // only log this error
		if err2 != nil {
			e.l.Error("fail to close etcd session", zap.Int64("lease", int64(se.Lease())), zap.Error(err2))
		}
	}
	failpoint.Inject("mockCampaignLoopExitedAbnormally", func(_ failpoint.Value) {
		closeSession = func(_ *concurrency.Session) {
			e.l.Info("skip closeSession", zap.String("failpoint", "mockCampaignLoopExitedAbnormally"))
		}
	})

	var err error
	defer func() {
		if session != nil {
			closeSession(session) // close the latest session.
		}
		if err != nil && errors.Cause(err) != ctx.Err() { // only send non-ctx.Err() error
			e.ech <- err
		}
	}()

	var (
		oldLeaderID string
		campaignWg  sync.WaitGroup
	)
	for {
		// check context canceled/timeout
		select {
		case <-session.Done():
			e.l.Info("etcd session is done, will try to create a new one", zap.Int64("old lease", int64(session.Lease())))
			closeSession(session)
			session, err = e.newSession(ctx, newSessionRetryUnlimited) // retry until context is done
			if err != nil {
				err = terror.ErrElectionCampaignFail.Delegate(err, "create a new session")
				return
			}
		case <-ctx.Done():
			e.l.Info("break campaign loop, context is done", zap.Stringer("current member", e.info), zap.Error(ctx.Err()))
			return
		default:
		}

		// try to campaign
		elec := concurrency.NewElection(session, e.key)
		ctx2, cancel2 := context.WithCancel(ctx)

		e.campaignMu.Lock()
		campaignWg.Add(1)
		e.cancelCampaign = func() {
			cancel2()
			campaignWg.Wait()
		}
		e.campaignMu.Unlock()

		go func() {
			defer campaignWg.Done()

			if e.evictLeader.Load() {
				// skip campaign
				return
			}

			e.l.Debug("begin to campaign", zap.Stringer("current member", e.info))

			err2 := elec.Campaign(ctx2, e.infoStr)
			if err2 != nil {
				// because inner commit may return undetermined error, we try to delete the election key manually
				deleted, err3 := e.ClearSessionIfNeeded(ctx, e.ID())
				if err3 != nil {
					e.l.Warn("failed to clean election key", zap.Error(err3))
				} else if deleted {
					e.l.Info("successful manually clean election key",
						zap.String("campaign error", err2.Error()))
				}

				// err may be ctx.Err(), but this can be handled in `case <-ctx.Done()`
				e.l.Info("fail to campaign", zap.Stringer("current member", e.info), zap.Error(err2))
			}
		}()

		var (
			leaderKey  string
			leaderInfo *CampaignerInfo
		)
		eleObserveCh := elec.Observe(ctx2)

	observeElection:
		for {
			select {
			case <-ctx.Done():
				break observeElection
			case <-session.Done():
				break observeElection
			case resp, ok := <-eleObserveCh:
				if !ok {
					break observeElection
				}

				e.l.Info("get response from election observe", zap.String("key", string(resp.Kvs[0].Key)), zap.String("value", string(resp.Kvs[0].Value)))
				leaderKey = string(resp.Kvs[0].Key)
				leaderInfo, err = getCampaignerInfo(resp.Kvs[0].Value)
				if err != nil {
					// this should never happened
					e.l.Error("fail to get leader information", zap.String("value", string(resp.Kvs[0].Value)), zap.Error(err))
					continue
				}

				if oldLeaderID == leaderInfo.ID {
					continue
				}
				oldLeaderID = leaderInfo.ID
				break observeElection
			}
		}

		if leaderInfo == nil || len(leaderInfo.ID) == 0 {
			cancel2()
			campaignWg.Wait()
			continue
		}

		if leaderInfo.ID != e.info.ID {
			e.l.Info("current member is not the leader", zap.Stringer("current member", e.info), zap.Stringer("leader", leaderInfo))
			e.notifyLeader(ctx, leaderInfo)
			cancel2()
			campaignWg.Wait()
			continue
		}

		e.l.Info("become leader", zap.Stringer("current member", e.info))
		e.notifyLeader(ctx, leaderInfo) // become the leader now
		e.watchLeader(ctx, session, leaderKey, elec)
		e.l.Info("retire from leader", zap.Stringer("current member", e.info))
		e.notifyLeader(ctx, nil) // need to re-campaign
		oldLeaderID = ""

		cancel2()
		campaignWg.Wait()
	}
}

// notifyLeader notify the leader's information.
// leader info can be nil, and this is used when retire from leader.
func (e *Election) notifyLeader(ctx context.Context, leaderInfo *CampaignerInfo) {
	if leaderInfo != nil && leaderInfo.ID == e.info.ID {
		e.isLeader.Store(true)
	} else {
		e.isLeader.Store(false)
	}

	select {
	case e.leaderCh <- leaderInfo:
	case <-time.After(e.notifyBlockTime):
		// this should not happened
		e.l.Error("ignore notify the leader's information after block a period of time", zap.Stringer("current member", e.info), zap.Stringer("leader", leaderInfo))
	case <-ctx.Done():
		e.l.Warn("ignore notify the leader's information because context canceled", zap.Stringer("current member", e.info), zap.Stringer("leader", leaderInfo))
	}
}

func (e *Election) watchLeader(ctx context.Context, session *concurrency.Session, key string, elec *concurrency.Election) {
	e.l.Debug("watch leader key", zap.String("key", key))

	e.campaignMu.Lock()
	e.resignCh = make(chan struct{})
	e.campaignMu.Unlock()

	defer func() {
		e.campaignMu.Lock()
		e.resignCh = nil
		e.campaignMu.Unlock()
	}()

	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	wch := e.cli.Watch(wCtx, key)

	for {
		if e.evictLeader.Load() {
			if err := elec.Resign(ctx); err != nil {
				e.l.Info("fail to resign leader", zap.Stringer("current member", e.info), zap.Error(err))
			}
			return
		}

		select {
		case resp, ok := <-wch:
			if !ok {
				e.l.Info("watch channel is closed")
				return
			}
			if resp.Canceled {
				e.l.Info("watch canceled")
				return
			}

			for _, ev := range resp.Events {
				// user may use some etcd client (like etcdctl) to delete the leader key and trigger a new campaign.
				if ev.Type == mvccpb.DELETE {
					e.l.Info("fail to watch, the leader is deleted", zap.ByteString("key", ev.Kv.Key))
					return
				}
			}
		case <-session.Done():
			return
		case <-ctx.Done():
			return
		case <-e.resignCh:
			if err := elec.Resign(ctx); err != nil {
				e.l.Info("fail to resign leader", zap.Stringer("current member", e.info), zap.Error(err))
			}
			return
		}
	}
}

// EvictLeader set evictLeader to true, and this member can't be leader.
func (e *Election) EvictLeader() {
	if !e.evictLeader.CAS(false, true) {
		return
	}

	e.Resign()
}

// Resign resign the leader.
func (e *Election) Resign() {
	// cancel campaign or current member is leader and then resign
	e.campaignMu.Lock()
	if e.cancelCampaign != nil {
		e.cancelCampaign()
		e.cancelCampaign = nil
	}

	if e.resignCh != nil {
		close(e.resignCh)
	}
	e.campaignMu.Unlock()
}

// CancelEvictLeader set evictLeader to false, and this member can campaign leader again.
func (e *Election) CancelEvictLeader() {
	if !e.evictLeader.CAS(true, false) {
		return
	}

	e.campaignMu.Lock()
	if e.cancelCampaign != nil {
		e.cancelCampaign()
		e.cancelCampaign = nil
	}
	e.campaignMu.Unlock()
}

func (e *Election) newSession(ctx context.Context, retryCnt int) (*concurrency.Session, error) {
	var (
		err     error
		session *concurrency.Session
	)

forLoop:
	for i := 0; i < retryCnt; i++ {
		if i > 0 {
			select {
			case e.ech <- terror.ErrElectionCampaignFail.Delegate(err, "create a new session"):
			default:
			}

			select {
			case <-time.After(newSessionRetryInterval):
			case <-ctx.Done():
				break forLoop
			}
		}

		// add more options if needed.
		// NOTE: I think use the client's context is better than something like `concurrency.WithContext(ctx)`,
		// so we can close the session when the client is still valid.
		session, err = concurrency.NewSession(e.cli, concurrency.WithTTL(e.sessionTTL))
		if err == nil || errors.Cause(err) == e.cli.Ctx().Err() {
			break forLoop
		}
	}
	return session, err
}

// ClearSessionIfNeeded will clear session when deleted master quited abnormally
// returns (triggered deleting session, error).
func (e *Election) ClearSessionIfNeeded(ctx context.Context, id string) (bool, error) {
	resp, err := e.cli.Get(ctx, e.key, clientv3.WithPrefix())
	if err != nil {
		return false, err
	}
	deleteKey := ""
	for _, kv := range resp.Kvs {
		leaderInfo, err2 := getCampaignerInfo(kv.Value)
		if err2 != nil {
			return false, err2
		}
		if leaderInfo.ID == id {
			deleteKey = string(kv.Key)
			break
		}
	}
	if deleteKey == "" {
		// no campaign info left in etcd, no need to trigger re-campaign
		return false, nil
	}
	delResp, err := e.cli.Delete(ctx, deleteKey)
	if err != nil {
		return false, err
	}
	return delResp.Deleted > 0, err
}
