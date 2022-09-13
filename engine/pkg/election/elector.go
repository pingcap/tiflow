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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Elector is a leader election client.
type Elector struct {
	config Config

	observeLock    sync.RWMutex
	observedRecord Record
	// observedRenews is a map of renew time of each member.
	// Note that the time is not RenewTime recorded in the record,
	// but the time when we observed the renewal. This is because
	// absolute time is not reliable across different machines.
	observedRenews map[string]time.Time

	// resignCh is used to notify the elector to resign leadership.
	// The value is the interval that the elector should not be leader.
	resignCh chan time.Duration
	// Elector will not be leader until this time.
	resignUntil time.Time
}

// NewElector creates a new Elector.
func NewElector(config Config) (*Elector, error) {
	if err := config.AdjustAndValidate(); err != nil {
		return nil, err
	}
	return &Elector{
		config:         config,
		observedRenews: make(map[string]time.Time),
		resignCh:       make(chan time.Duration),
	}, nil
}

// Run runs the elector to continuously campaign for leadership
// until the context is canceled.
func (e *Elector) Run(ctx context.Context) error {
	leaderCallback := e.config.LeaderCallback

	var (
		callbackWg        sync.WaitGroup
		callbackIsRunning atomic.Bool
		cancelCallback    context.CancelFunc
	)

	for {
		var (
			shouldCancelCallback bool
			cancelCallbackReason string
		)

		if err := e.renew(ctx); err != nil {
			log.Warn("failed to renew lease after renew deadline", zap.Error(err),
				zap.Duration("renew-deadline", e.config.RenewDeadline))
			shouldCancelCallback = true
			cancelCallbackReason = "renew lease failed"
		} else if e.IsLeader() {
			if !callbackIsRunning.Load() {
				leaderCtx, leaderCancel := context.WithCancel(ctx)
				callbackWg.Add(1)
				callbackIsRunning.Store(true)
				go func() {
					defer func() {
						callbackIsRunning.Store(false)
						callbackWg.Done()
					}()
					err := leaderCallback(leaderCtx)
					log.Info("leader callback returned", zap.Error(err))
				}()
				cancelCallback = leaderCancel
			}
		} else {
			shouldCancelCallback = true
			cancelCallbackReason = "not leader"
		}

		if shouldCancelCallback && callbackIsRunning.Load() {
			log.Info("cancel leader callback", zap.String("reason", cancelCallbackReason))
			start := time.Now()
			cancelCallback()
			callbackWg.Wait()
			log.Info("leader callback is canceled", zap.Duration("took", time.Since(start)))
		}

		select {
		case <-ctx.Done():
			if err := e.release(context.Background()); err != nil {
				log.Warn("failed to release member lease", zap.Error(err))
			}
			if callbackIsRunning.Load() {
				cancelCallback()
				callbackWg.Wait()
			}
			return ctx.Err()
		case resignInterval := <-e.resignCh:
			e.resignUntil = time.Now().Add(resignInterval)
		case <-time.After(e.config.RenewInterval):
		}
	}
}

func (e *Elector) renew(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, e.config.RenewDeadline)
	defer cancel()

	for {
		err := e.tryAcquireOrRenew(ctx)
		if err == nil {
			return nil
		}
		randDelay := time.Duration(rand.Int63n(int64(e.config.RenewInterval)))
		log.Info("renew lease failed, retry after random delay",
			zap.Duration("delay", randDelay), zap.Error(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(randDelay):
		}
	}
}

func (e *Elector) tryAcquireOrRenew(ctx context.Context) error {
	s := e.config.Storage

	record, err := s.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.setObservedRecord(record.Clone())

	var activeMembers []*Member
	for _, m := range record.Members {
		if e.isLeaseExpired(m.ID) {
			if m.ID == record.LeaderID {
				record.LeaderID = ""
				log.Info(
					"leader lease expired",
					zap.String("leader-id", m.ID),
					zap.String("leader-name", m.Name),
					zap.String("leader-address", m.Address),
				)
			} else {
				log.Info(
					"member lease expired",
					zap.String("member-id", m.ID),
					zap.String("member-name", m.Name),
					zap.String("member-address", m.Address),
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
			log.Info("resign the leadership")
		}
	} else if record.LeaderID == "" {
		// Elect a new leader if no leader exists.
		record.LeaderID = e.config.ID
		log.Info(
			"try to elect self as leader",
			zap.String("id", e.config.ID),
			zap.String("name", e.config.Name),
			zap.String("address", e.config.Address),
		)
	}

	if err := s.Update(ctx, record); err != nil {
		return errors.Trace(err)
	}
	e.setObservedRecord(record)
	return nil
}

func (e *Elector) release(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, defaultReleaseTimeout)
	defer cancel()

	s := e.config.Storage

	record, err := s.Get(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.setObservedRecord(record.Clone())

	for i, m := range record.Members {
		if m.ID == e.config.ID {
			record.Members = append(record.Members[:i], record.Members[i+1:]...)
			break
		}
	}
	if record.LeaderID == e.config.ID {
		record.LeaderID = ""
	}

	if err := s.Update(ctx, record); err != nil {
		return errors.Trace(err)
	}
	e.setObservedRecord(record)
	return nil
}

func (e *Elector) setObservedRecord(record *Record) {
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
				zap.String("leader-id", leader.ID),
				zap.String("leader-name", leader.Name),
				zap.String("leader-address", leader.Address),
			)
		}
	}

	e.observedRecord = *record
}

func (e *Elector) isLeaseExpired(memberID string) bool {
	e.observeLock.RLock()
	defer e.observeLock.RUnlock()

	member, ok := e.observedRecord.FindMember(memberID)
	if !ok {
		return true
	}
	renewTime := e.observedRenews[memberID]
	return renewTime.Add(member.LeaseDuration).Before(time.Now())
}

// IsLeader returns true if the current member is the leader.
func (e *Elector) IsLeader() bool {
	e.observeLock.RLock()
	defer e.observeLock.RUnlock()

	return e.observedRecord.LeaderID == e.config.ID
}

// GetLeader returns the leader member.
func (e *Elector) GetLeader() (*Member, bool) {
	e.observeLock.RLock()
	defer e.observeLock.RUnlock()

	for _, m := range e.observedRecord.Members {
		if m.ID == e.observedRecord.LeaderID {
			return m.Clone(), true
		}
	}
	return nil, false
}

// GetMembers returns all members.
func (e *Elector) GetMembers() []*Member {
	e.observeLock.RLock()
	defer e.observeLock.RUnlock()

	var members []*Member
	for _, m := range e.observedRecord.Members {
		members = append(members, m.Clone())
	}
	return members
}

// ResignLeader resigns the leadership and let the elector
// not to try to campaign for leadership during the resignInterval.
func (e *Elector) ResignLeader(ctx context.Context, resignInterval time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.resignCh <- resignInterval:
		return nil
	}
}
