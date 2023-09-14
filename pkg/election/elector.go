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

package election

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Elector is a leader election client.
type Elector interface {
	// RunElection runs the elector to continuously campaign for leadership
	// until the context is canceled.
	RunElection(ctx context.Context) error
	// IsLeader returns true if the current member is the leader
	// and its lease is still valid.
	IsLeader() bool
	// GetLeader returns the last observed leader whose lease is still valid.
	GetLeader() (*Member, bool)
	// GetMembers returns all members.
	GetMembers() []*Member
	// ResignLeader resigns the leadership and let the elector
	// not to try to campaign for leadership during the duration.
	ResignLeader(ctx context.Context, duration time.Duration) error
}

// NewElector creates a new Elector.
func NewElector(config Config) (Elector, error) {
	if err := config.AdjustAndValidate(); err != nil {
		return nil, err
	}
	return &electorImpl{
		config:         config,
		observedRenews: make(map[string]time.Time),
		resignCh:       make(chan *resignReq),
	}, nil
}

// electorImpl is the default implementation of Elector.
type electorImpl struct {
	config Config

	observeLock    sync.RWMutex
	observedRecord Record
	// observedRenews is a map of renew time of each member.
	// Note that the time is not RenewTime recorded in the record,
	// but the time when we observed the renewal. This is because
	// absolute time is not reliable across different machines.
	observedRenews map[string]time.Time

	// resignCh is used to notify the elector to resign leadership.
	resignCh chan *resignReq
	// elector will not be leader until this time.
	resignUntil time.Time

	callbackWg        sync.WaitGroup
	callbackIsRunning atomic.Bool
	callbackCancelFn  context.CancelFunc
}

type resignReq struct {
	ctx      context.Context
	duration time.Duration
	errCh    chan error
}

// RunElection implements Elector.RunElection.
func (e *electorImpl) RunElection(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		if err := e.renew(ctx); err != nil {
			log.Warn("failed to renew lease after renew deadline", zap.Error(err),
				zap.Duration("renewDeadline", e.config.RenewDeadline))
			e.cancelCallback("renew lease failed")
			if e.config.ExitOnRenewFail {
				cancel()
			}
		} else if e.IsLeader() {
			e.ensureCallbackIsRunning(ctx)
		} else {
			e.cancelCallback("not leader")
		}

		select {
		case <-ctx.Done():
			if err := e.release(context.Background(), true /* removeSelf */); err != nil {
				log.Warn("failed to release member lease", zap.Error(err))
			}
			e.cancelCallback(ctx.Err().Error())
			return ctx.Err()
		case req := <-e.resignCh:
			if e.IsLeader() {
				log.Info("try to resign leadership")
				if err := e.release(req.ctx, false /* removeSelf */); err != nil {
					log.Warn("failed to resign leadership", zap.Error(err))
					req.errCh <- err
				} else {
					req.errCh <- nil
					e.resignUntil = time.Now().Add(req.duration)
					e.cancelCallback("leader resigned")
				}
			} else {
				req.errCh <- nil
				e.resignUntil = time.Now().Add(req.duration)
			}
		case <-time.After(e.config.RenewInterval):
		}
	}
}

func (e *electorImpl) renew(ctx context.Context) (err error) {
	start := time.Now()
	defer func() {
		log.Debug("renew", zap.Duration("cost", time.Since(start)), zap.Error(err))
	}()

	ctx, cancel := context.WithTimeout(ctx, e.config.RenewDeadline)
	defer cancel()

	return e.updateRecord(ctx, func(record *Record) (error, bool) {
		var activeMembers []*Member
		var isLeaderChanged bool
		for _, m := range record.Members {
			if e.isLeaseExpired(m.ID) {
				if m.ID == record.LeaderID {
					record.LeaderID = ""
					isLeaderChanged = true
					log.Info(
						"leader lease expired",
						zap.String("leaderID", m.ID),
						zap.String("leaderName", m.Name),
						zap.String("leaderAddress", m.Address),
					)
				} else {
					log.Info(
						"member lease expired",
						zap.String("memberID", m.ID),
						zap.String("memberName", m.Name),
						zap.String("memberAddress", m.Address),
					)
				}
			} else {
				activeMembers = append(activeMembers, m)
			}
		}
		record.Members = activeMembers

		// Add self to the record if not exists.
		if m, ok := record.FindMember(e.config.ID); !ok {
			record.Members = append(record.Members, &Member{
				ID:            e.config.ID,
				Name:          e.config.Name,
				Address:       e.config.Address,
				LeaseDuration: e.config.LeaseDuration,
				RenewTime:     time.Now(),
			})
		} else {
			m.RenewTime = time.Now()
		}

		if time.Now().Before(e.resignUntil) {
			if record.LeaderID == e.config.ID {
				record.LeaderID = ""
				isLeaderChanged = true
				log.Info("try to resign leadership")
			}
		} else if record.LeaderID == "" {
			// Elect a new leader if no leader exists.
			record.LeaderID = e.config.ID
			isLeaderChanged = true
			log.Info(
				"try to elect self as leader",
				zap.String("id", e.config.ID),
				zap.String("name", e.config.Name),
				zap.String("address", e.config.Address),
			)
		}
		return nil, isLeaderChanged
	})
}

func (e *electorImpl) ensureCallbackIsRunning(ctx context.Context) {
	if !e.callbackIsRunning.Load() {
		leaderCallback := e.config.LeaderCallback
		leaderCtx, leaderCancel := context.WithCancel(ctx)
		e.callbackWg.Add(1)
		e.callbackIsRunning.Store(true)
		go func() {
			defer func() {
				e.callbackIsRunning.Store(false)
				e.callbackWg.Done()
				leaderCancel()
			}()
			log.Info("leader callback is called")
			err := leaderCallback(leaderCtx)
			if errors.Cause(err) != context.Canceled {
				log.Warn("leader callback is unexpectedly exited", zap.Error(err))
				if e.IsLeader() {
					log.Info("try to resign leadership")
					if err := e.release(context.Background(), false /* removeSelf */); err != nil {
						log.Warn("failed to resign leadership", zap.Error(err))
					}
				}
			}
		}()
		e.callbackCancelFn = leaderCancel
	}
}

func (e *electorImpl) cancelCallback(reason string) {
	if e.callbackIsRunning.Load() {
		log.Info("cancel leader callback", zap.String("reason", reason))
		start := time.Now()
		e.callbackCancelFn()
		e.callbackWg.Wait()
		log.Info("leader callback is canceled", zap.Duration("took", time.Since(start)))
	}
}

func (e *electorImpl) release(ctx context.Context, removeSelf bool) error {
	ctx, cancel := context.WithTimeout(ctx, defaultReleaseTimeout)
	defer cancel()

	return e.updateRecord(ctx, func(record *Record) (error, bool) {
		var isLeaderChanged bool
		if record.LeaderID == e.config.ID {
			record.LeaderID = ""
			isLeaderChanged = true
		}
		if removeSelf {
			for i, m := range record.Members {
				if m.ID == e.config.ID {
					record.Members = append(record.Members[:i], record.Members[i+1:]...)
					break
				}
			}
		}
		return nil, isLeaderChanged
	})
}

func (e *electorImpl) updateRecord(
	ctx context.Context,
	f func(*Record) (err error, isLeaderChanged bool),
) error {
	// Divide 2 is for more retries.
	backoffBaseDelayInMs := int64(e.config.RenewInterval/time.Millisecond) / 2
	// Make sure the retry delay is less than the deadline, otherwise the retry has no chance to execute.
	backoffMaxDelayInMs := int64(e.config.RenewDeadline/time.Millisecond) / 2
	if deadline, ok := ctx.Deadline(); ok {
		maxDelayForCtx := int64(time.Until(deadline)/time.Millisecond) / 2
		if maxDelayForCtx < backoffMaxDelayInMs {
			backoffMaxDelayInMs = maxDelayForCtx
		}
	}

	return retry.Do(ctx, func() error {
		s := e.config.Storage
		record, err := s.Get(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		e.setObservedRecord(record)

		var isLeaderChanged bool
		if err, isLeaderChanged = f(record); err != nil {
			return errors.Trace(err)
		}

		if err := s.Update(ctx, record, isLeaderChanged); err != nil {
			return errors.Trace(err)
		}
		e.setObservedRecord(record)
		return nil
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithBackoffMaxDelay(backoffMaxDelayInMs),
		retry.WithIsRetryableErr(func(err error) bool {
			if errors.Is(err, errors.ErrElectionRecordConflict) {
				log.Info("conflict encountered while updating record, retrying")
			} else {
				log.Warn("failed to update record, retrying", zap.Error(err))
			}
			return true // For log only, retry doesn't rely on it.
		}),
	)
}

func (e *electorImpl) setObservedRecord(record *Record) {
	e.observeLock.Lock()
	defer e.observeLock.Unlock()

	// Remove members that are not in the new record.
	for id := range e.observedRenews {
		if _, ok := record.FindMember(id); !ok {
			delete(e.observedRenews, id)
		}
	}

	// Update observedRenews for members in the new record.
	for _, m := range record.Members {
		oldMember, ok := e.observedRecord.FindMember(m.ID)
		// If the member is not in the old record, or the RenewTime is
		// changed, update the local observedRenews.
		if !ok || !oldMember.RenewTime.Equal(m.RenewTime) {
			e.observedRenews[m.ID] = time.Now()
		}
	}

	// New leader is elected.
	if record.LeaderID != "" && record.LeaderID != e.observedRecord.LeaderID {
		leader, ok := record.FindMember(record.LeaderID)
		if ok {
			log.Info(
				"new leader elected",
				zap.String("leaderID", leader.ID),
				zap.String("leaderName", leader.Name),
				zap.String("leaderAddress", leader.Address),
			)
		}
	}

	e.observedRecord = *record.Clone()
}

func (e *electorImpl) isLeaseExpired(memberID string) bool {
	e.observeLock.RLock()
	defer e.observeLock.RUnlock()

	return e.isLeaseExpiredLocked(memberID)
}

func (e *electorImpl) isLeaseExpiredLocked(memberID string) bool {
	member, ok := e.observedRecord.FindMember(memberID)
	if !ok {
		return true
	}
	renewTime := e.observedRenews[memberID]
	return renewTime.Add(member.LeaseDuration).Before(time.Now())
}

// IsLeader implements the Elector.IsLeader.
func (e *electorImpl) IsLeader() bool {
	e.observeLock.RLock()
	defer e.observeLock.RUnlock()

	if e.isLeaseExpiredLocked(e.config.ID) {
		return false
	}
	return e.observedRecord.LeaderID == e.config.ID
}

// GetLeader implements the Elector.GetLeader.
func (e *electorImpl) GetLeader() (*Member, bool) {
	e.observeLock.RLock()
	defer e.observeLock.RUnlock()

	leader, ok := e.observedRecord.FindMember(e.observedRecord.LeaderID)
	if ok && !e.isLeaseExpiredLocked(leader.ID) {
		return leader.Clone(), true
	}
	return nil, false
}

// GetMembers implements the Elector.GetMembers.
func (e *electorImpl) GetMembers() []*Member {
	e.observeLock.RLock()
	defer e.observeLock.RUnlock()

	members := make([]*Member, 0, len(e.observedRecord.Members))
	for _, m := range e.observedRecord.Members {
		members = append(members, m.Clone())
	}
	return members
}

// ResignLeader implements the Elector.ResignLeader.
func (e *electorImpl) ResignLeader(ctx context.Context, duration time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, defaultResignTimeout)
	defer cancel()

	req := &resignReq{
		ctx:      ctx,
		duration: duration,
		errCh:    make(chan error, 1),
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.resignCh <- req:
		return <-req.errCh
	}
}
