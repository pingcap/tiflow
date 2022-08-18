// Copyright 2021 PingCAP, Inc.
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

package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/sorter/db/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	"github.com/stretchr/testify/require"
)

type mockCompactDB struct {
	db.DB
	compact chan struct{}
}

func (m *mockCompactDB) Compact(_, _ []byte) error {
	m.compact <- struct{}{}
	return nil
}

func TestCompactorPoll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenPebble(ctx, 1, t.TempDir(), cfg, db.WithTableCRTsCollectors())
	require.Nil(t, err)
	mockDB := mockCompactDB{DB: db, compact: make(chan struct{}, 1)}
	closedWg := new(sync.WaitGroup)
	cfg.CompactionDeletionThreshold = 2
	cfg.CompactionPeriod = 1
	compactor, _, err := NewCompactActor(1, &mockDB, closedWg, cfg)
	require.Nil(t, err)

	// Must not trigger compact.
	task := message.Task{DeleteReq: &message.DeleteRequest{}}
	task.DeleteReq.Count = 0
	closed := !compactor.Poll(ctx, []actormsg.Message[message.Task]{actormsg.ValueMessage(task)})
	require.False(t, closed)
	select {
	case <-mockDB.compact:
		t.Fatal("Must trigger compact")
	case <-time.After(500 * time.Millisecond):
	}

	// Must trigger compact.
	task.DeleteReq.Count = 2 * cfg.CompactionDeletionThreshold
	closed = !compactor.Poll(ctx, []actormsg.Message[message.Task]{actormsg.ValueMessage(task)})
	require.False(t, closed)
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Must trigger compact")
	case <-mockDB.compact:
	}

	// Must trigger compact.
	time.Sleep(time.Duration(cfg.CompactionPeriod) * time.Second * 2)
	task.DeleteReq.Count = cfg.CompactionDeletionThreshold / 2
	closed = !compactor.Poll(ctx, []actormsg.Message[message.Task]{actormsg.ValueMessage(task)})
	require.False(t, closed)
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Must trigger compact")
	case <-mockDB.compact:
	}

	// Close db.
	stopMsg := actormsg.StopMessage[message.Task]()
	closed = !compactor.Poll(ctx, []actormsg.Message[message.Task]{stopMsg})
	require.True(t, closed)
	compactor.OnClose()
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestComactorContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenPebble(ctx, 1, t.TempDir(), cfg, db.WithTableCRTsCollectors())
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	ldb, _, err := NewCompactActor(0, db, closedWg, cfg)
	require.Nil(t, err)

	cancel()
	closed := !ldb.Poll(
		ctx, []actormsg.Message[message.Task]{actormsg.ValueMessage(message.Task{})})
	require.True(t, closed)
	ldb.OnClose()
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestScheduleCompact(t *testing.T) {
	t.Parallel()
	router := actor.NewRouter[message.Task](t.Name())
	mb := actor.NewMailbox[message.Task](actor.ID(1), 1)
	router.InsertMailbox4Test(mb.ID(), mb)
	compact := NewCompactScheduler(router)

	// Must schedule successfully.
	require.True(t, compact.tryScheduleCompact(mb.ID(), 3))
	msg, ok := mb.Receive()
	require.True(t, ok)
	task := message.Task{DeleteReq: &message.DeleteRequest{}}
	task.DeleteReq.Count = 3
	require.EqualValues(t, actormsg.ValueMessage(task), msg)

	// Skip sending unnecessary tasks.
	require.True(t, compact.tryScheduleCompact(mb.ID(), 3))
	require.True(t, compact.tryScheduleCompact(mb.ID(), 3))
	msg, ok = mb.Receive()
	require.True(t, ok)
	require.EqualValues(t, actormsg.ValueMessage(task), msg)
	_, ok = mb.Receive()
	require.False(t, ok)
}

func TestDeleteThrottle(t *testing.T) {
	t.Parallel()
	dt := deleteThrottle{
		countThreshold: 2,
		period:         1 * time.Second,
	}

	require.False(t, dt.trigger(1, time.Now()))
	require.True(t, dt.trigger(3, time.Now()))
	time.Sleep(2 * dt.period)
	require.True(t, dt.trigger(0, time.Now()))
}
