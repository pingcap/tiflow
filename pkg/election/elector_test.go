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

package election_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/pkg/election"
	"github.com/pingcap/tiflow/pkg/election/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestElectorBasic(t *testing.T) {
	t.Parallel()

	s := mock.NewMockStorage(gomock.NewController(t))

	var recordLock sync.RWMutex
	record := &election.Record{}

	s.EXPECT().Get(gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context) (*election.Record, error) {
			recordLock.RLock()
			defer recordLock.RUnlock()

			return record.Clone(), nil
		})

	s.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context, r *election.Record, _ bool) error {
			recordLock.Lock()
			defer recordLock.Unlock()

			if r.Version != record.Version {
				return errors.ErrElectionRecordConflict.GenWithStackByArgs()
			}
			record = r.Clone()
			record.Version++
			return nil
		})

	var (
		electors  []election.Elector
		configs   []election.Config
		cancelFns []context.CancelFunc
		wg        sync.WaitGroup
	)
	firstLeaderID := make(chan string, 1)
	const electorCount = 5
	for i := 0; i < electorCount; i++ {
		id := fmt.Sprintf("elector-%d", i)
		config := election.Config{
			ID:      id,
			Name:    id,
			Address: fmt.Sprintf("127.0.0.1:1024%d", i),
			Storage: s,
			LeaderCallback: func(ctx context.Context) error {
				select {
				case firstLeaderID <- id:
				default:
				}
				<-ctx.Done()
				return ctx.Err()
			},
			LeaseDuration: time.Second,
			RenewInterval: time.Millisecond * 100,
			RenewDeadline: time.Millisecond * 900,
		}
		elector, err := election.NewElector(config)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := elector.RunElection(ctx)
			require.Error(t, err)
			require.Equal(t, context.Canceled, errors.Cause(err))
		}()

		electors = append(electors, elector)
		configs = append(configs, config)
		cancelFns = append(cancelFns, cancel)
	}

	// Wait for first leader to be elected.
	var leader *election.Member
	require.Eventually(t, func() bool {
		var ok bool
		leader, ok = electors[0].GetLeader()
		return ok
	}, time.Second, time.Millisecond*100, "leader not elected")
	require.NotNil(t, leader)
	select {
	case leaderID := <-firstLeaderID:
		require.Equal(t, leaderID, leader.ID)
	case <-time.After(time.Second):
		require.Fail(t, "leader callback not called")
	}

	// Wait for all elector members to join.
	var members []*election.Member
	require.Eventually(t, func() bool {
		members = electors[0].GetMembers()
		return len(members) == electorCount
	}, time.Second, time.Millisecond*100, "not all members joined")

	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	for i, m := range members {
		require.Equal(t, configs[i].ID, m.ID)
		require.Equal(t, configs[i].Name, m.Name)
		require.Equal(t, configs[i].Address, m.Address)
	}

	// All electors should have the same leader.
	for _, e := range electors {
		leader1, ok := e.GetLeader()
		require.True(t, ok)
		require.Equal(t, leader.ID, leader1.ID)
	}

	// Test resign leader.
	leaderIdx, err := strconv.Atoi(strings.TrimPrefix(leader.ID, "elector-"))
	require.NoError(t, err)
	leaderElector := electors[leaderIdx]
	require.True(t, leaderElector.IsLeader())
	err = leaderElector.ResignLeader(context.Background(), time.Second)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		newLeader, ok := leaderElector.GetLeader()
		return ok && newLeader.ID != leader.ID
	}, time.Second, time.Millisecond*100, "leader not changed")

	// Test cancel elector.
	for i := electorCount - 1; i > 0; i-- {
		cancelFns[i]()
		require.Eventually(t, func() bool {
			_, ok := electors[0].GetLeader()
			if !ok {
				return false
			}
			members := electors[0].GetMembers()
			return len(members) == i
		}, time.Second*3, time.Millisecond*100, "member not removed")
	}
	cancelFns[0]()
	wg.Wait()
}

func TestElectorRenewFailure(t *testing.T) {
	t.Parallel()

	var recordLock sync.RWMutex
	record := &election.Record{}

	getRecord := func(_ context.Context) (*election.Record, error) { //nolint:unparam
		recordLock.RLock()
		defer recordLock.RUnlock()

		return record.Clone(), nil
	}

	updateRecord := func(_ context.Context, r *election.Record, _ bool) error {
		recordLock.Lock()
		defer recordLock.Unlock()

		if r.Version != record.Version {
			return errors.ErrElectionRecordConflict.GenWithStackByArgs()
		}
		record = r.Clone()
		record.Version++
		return nil
	}

	var (
		s1Err       atomic.Error
		s1LastRenew time.Time
	)
	s1 := mock.NewMockStorage(gomock.NewController(t))
	s1.EXPECT().Get(gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context) (*election.Record, error) {
			if err := s1Err.Load(); err != nil {
				return nil, err
			}
			return getRecord(ctx)
		})
	s1.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context, r *election.Record, isLeaderChanged bool) error {
			if err := s1Err.Load(); err != nil {
				return err
			}
			if err := updateRecord(ctx, r, isLeaderChanged); err != nil {
				return err
			}
			s1LastRenew = time.Now()
			return nil
		})

	s2 := mock.NewMockStorage(gomock.NewController(t))
	s2.EXPECT().Get(gomock.Any()).AnyTimes().DoAndReturn(getRecord)
	s2.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(updateRecord)

	const (
		leaseDuration = time.Second * 1
		renewInterval = time.Millisecond * 100
		renewDeadline = leaseDuration - renewInterval
	)

	ctx, cancel := context.WithCancel(context.Background())

	e1, err := election.NewElector(election.Config{
		ID:      "e1",
		Name:    "e1",
		Address: "127.0.0.1:10241",
		Storage: s1,
		LeaderCallback: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		LeaseDuration: leaseDuration,
		RenewInterval: renewInterval,
		RenewDeadline: renewDeadline,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := e1.RunElection(ctx)
		require.Error(t, err)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// Wait for leader to be elected.
	require.Eventually(t, func() bool {
		_, ok := e1.GetLeader()
		return ok
	}, time.Second, time.Millisecond*100, "leader not elected")

	e2, err := election.NewElector(election.Config{
		ID:      "e2",
		Name:    "e2",
		Address: "127.0.0.1:10242",
		Storage: s2,
		LeaderCallback: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		LeaseDuration: leaseDuration,
		RenewInterval: renewInterval,
		RenewDeadline: renewDeadline,
	})
	require.NoError(t, err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := e2.RunElection(ctx)
		require.Error(t, err)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// Make s1 fail and wait for s2 to be elected.
	s1Err.Store(errors.New("connection error"))
	require.Eventually(t, func() bool {
		leader, ok := e2.GetLeader()
		if ok && leader.ID == "e2" {
			require.GreaterOrEqual(t, time.Since(s1LastRenew), leaseDuration,
				"elector 2 shouldn't elect itself as leader when elector 1 lease is not expired")
			return true
		}
		return false
	}, time.Second*3, time.Millisecond*100, "elector 2 not elected")

	require.False(t, e1.IsLeader())
	_, ok := e1.GetLeader()
	require.False(t, ok)

	cancel()
	wg.Wait()
}

func TestLeaderCallbackUnexpectedExit(t *testing.T) {
	t.Parallel()

	s := mock.NewMockStorage(gomock.NewController(t))

	var recordLock sync.RWMutex
	record := &election.Record{}

	s.EXPECT().Get(gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context) (*election.Record, error) {
			recordLock.RLock()
			defer recordLock.RUnlock()

			return record.Clone(), nil
		})

	s.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context, r *election.Record, _ bool) error {
			recordLock.Lock()
			defer recordLock.Unlock()

			if r.Version != record.Version {
				return errors.ErrElectionRecordConflict.GenWithStackByArgs()
			}
			record = r.Clone()
			record.Version++
			return nil
		})

	const (
		leaseDuration = time.Second * 1
		renewInterval = time.Millisecond * 100
		renewDeadline = leaseDuration - renewInterval
	)

	ctx, cancel := context.WithCancel(context.Background())

	var e1CallbackErr atomic.Error

	e1, err := election.NewElector(election.Config{
		ID:      "e1",
		Name:    "e1",
		Address: "127.0.0.1:10241",
		Storage: s,
		LeaderCallback: func(ctx context.Context) error {
			ticker := time.NewTicker(time.Millisecond)
			for {
				select {
				case <-ticker.C:
					if err := e1CallbackErr.Load(); err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		},
		LeaseDuration: leaseDuration,
		RenewInterval: renewInterval,
		RenewDeadline: renewDeadline,
	})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := e1.RunElection(ctx)
		require.Error(t, err)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// Wait for leader to be elected.
	require.Eventually(t, func() bool {
		_, ok := e1.GetLeader()
		return ok
	}, time.Second, time.Millisecond*100, "leader not elected")

	e2, err := election.NewElector(election.Config{
		ID:      "e2",
		Name:    "e2",
		Address: "127.0.0.1:10242",
		Storage: s,
		LeaderCallback: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		LeaseDuration: leaseDuration,
		RenewInterval: renewInterval,
		RenewDeadline: renewDeadline,
	})
	require.NoError(t, err)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := e2.RunElection(ctx)
		require.Error(t, err)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// Make elector 1 leader callback return error.
	e1CallbackErr.Store(errors.New("callback error"))

	require.Eventually(t, func() bool {
		leader, ok := e1.GetLeader()
		return ok && leader.ID == "e2"
	}, time.Second*3, time.Millisecond*100, "e2 not elected")

	cancel()
	wg.Wait()
}
