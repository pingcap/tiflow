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

package leveldb

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sorter/encoding"
	"github.com/pingcap/ticdc/cdc/sorter/leveldb/message"
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/db"
	"github.com/stretchr/testify/require"
)

func makeCleanTask(uid uint32, tableID uint64) []actormsg.Message {
	return []actormsg.Message{actormsg.SorterMessage(message.Task{
		UID:     uid,
		TableID: tableID,
		Cleanup: true,
	})}
}

func prepareData(t *testing.T, db db.DB, data [][]int) {
	wb := db.Batch(0)
	for _, d := range data {
		count, uid, tableID := d[0], d[1], d[2]
		for k := 0; k < count; k++ {
			key := encoding.EncodeKey(
				uint32(uid), uint64(tableID),
				model.NewPolymorphicEvent(&model.RawKVEntry{
					OpType:  model.OpTypeDelete,
					Key:     []byte{byte(k)},
					StartTs: 1,
					CRTs:    2,
				}))
			wb.Put(key, key)
		}
	}
	require.Nil(t, wb.Commit())
}

func TestCleanerPoll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter(t.Name()), cfg)
	clean, _, err := NewCleanerActor(1, db, nil, compact, cfg, closedWg)
	require.Nil(t, err)

	// Put data to db.
	// * 1 key of uid1 table1
	// * 3 key of uid2 table1
	// * 2 key of uid3 table2
	// * 1 key of uid4 table2
	data := [][]int{
		{1, 1, 1},
		{3, 2, 1},
		{2, 3, 2},
		{1, 4, 2},
	}
	prepareData(t, db, data)

	// Ensure there are some key/values belongs to uid2 table1.
	start := encoding.EncodeTsKey(2, 1, 0)
	limit := encoding.EncodeTsKey(2, 2, 0)
	snap, err := db.Snapshot()
	require.Nil(t, err)
	iter := snap.Iterator(start, limit)
	require.True(t, iter.First())
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Clean up uid2 table1
	closed := !clean.Poll(ctx, makeCleanTask(2, 1))
	require.False(t, closed)

	// Ensure no key/values belongs to uid2 table1
	snap, err = db.Snapshot()
	require.Nil(t, err)
	iter = snap.Iterator(start, limit)
	require.False(t, iter.First())
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Ensure uid1 table1 is untouched.
	start = encoding.EncodeTsKey(1, 1, 0)
	limit = encoding.EncodeTsKey(1, 2, 0)
	snap, err = db.Snapshot()
	require.Nil(t, err)
	iter = snap.Iterator(start, limit)
	require.True(t, iter.First())
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Ensure uid3 table2 is untouched.
	start = encoding.EncodeTsKey(3, 2, 0)
	limit = encoding.EncodeTsKey(3, 3, 0)
	snap, err = db.Snapshot()
	require.Nil(t, err)
	iter = snap.Iterator(start, limit)
	require.True(t, iter.First())
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Clean up uid3 table2
	closed = !clean.Poll(ctx, makeCleanTask(3, 2))
	require.False(t, closed)

	// Ensure no key/values belongs to uid3 table2
	snap, err = db.Snapshot()
	require.Nil(t, err)
	iter = snap.Iterator(start, limit)
	require.False(t, iter.First())
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Ensure uid4 table2 is untouched.
	start = encoding.EncodeTsKey(4, 2, 0)
	limit = encoding.EncodeTsKey(4, 3, 0)
	snap, err = db.Snapshot()
	require.Nil(t, err)
	iter = snap.Iterator(start, limit)
	require.True(t, iter.First())
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Close leveldb.
	closed = !clean.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestCleanerContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter(t.Name()), cfg)
	clean, _, err := NewCleanerActor(1, db, nil, compact, cfg, closedWg)
	require.Nil(t, err)

	cancel()
	tasks := makeCleanTask(1, 1)
	closed := !clean.Poll(ctx, tasks)
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestCleanerWriteRateLimited(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1
	cfg.CleanupSpeedLimit = 4
	// wbSize = cleanup speed limit / 2

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter(t.Name()), cfg)
	clean, _, err := NewCleanerActor(1, db, nil, compact, cfg, closedWg)
	require.Nil(t, err)

	// Put data to db.
	// * 1 key of uid1 table1
	// * 3 key of uid2 table1
	// * 2 key of uid3 table2
	// * 1 key of uid4 table2
	data := [][]int{
		{1, 1, 1},
		{3, 2, 1},
		{2, 3, 2},
		{1, 4, 2},
	}
	prepareData(t, db, data)

	keys := [][]byte{}
	start := encoding.EncodeTsKey(0, 0, 0)
	limit := encoding.EncodeTsKey(5, 0, 0)
	snap, err := db.Snapshot()
	require.Nil(t, err)
	iter := snap.Iterator(start, limit)
	for iter.Next() {
		key := append([]byte{}, iter.Key()...)
		keys = append(keys, key)
	}
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())
	require.Equal(t, 7, len(keys), "%v", keys)

	// Must speed limited.
	wb := db.Batch(0)
	var delay time.Duration
	var count int
	for {
		for i := 0; i < cfg.CleanupSpeedLimit/2; i++ {
			wb.Delete(keys[i])
		}
		delay, err = clean.writeRateLimited(wb, false)
		require.Nil(t, err)
		if delay != 0 {
			break
		}
		count++
	}

	// Sleep and write again.
	time.Sleep(delay * 4)
	delay, err = clean.writeRateLimited(wb, false)
	require.EqualValues(t, 0, delay)
	require.Nil(t, err)

	// Force write ignores speed limit.
	for i := 0; i < count*2; i++ {
		delay, err = clean.writeRateLimited(wb, true)
		require.EqualValues(t, 0, delay)
		require.Nil(t, err)
	}

	// Close leveldb.
	closed := !clean.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestCleanerTaskRescheduled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1
	cfg.CleanupSpeedLimit = 4
	// wbSize = cleanup speed limit / 2

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	router := actor.NewRouter(t.Name())
	compact := NewCompactScheduler(actor.NewRouter(t.Name()), cfg)
	clean, mb, err := NewCleanerActor(1, db, router, compact, cfg, closedWg)
	require.Nil(t, err)
	router.InsertMailbox4Test(actor.ID(1), mb)
	require.Nil(t, router.SendB(ctx, actor.ID(1), actormsg.TickMessage()))
	receiveTimeout := func() (actormsg.Message, bool) {
		for i := 0; i < 10; i++ { // 2s
			time.Sleep(200 * time.Millisecond)
			task, ok := mb.Receive()
			if ok {
				return task, ok
			}
		}
		return mb.Receive()
	}
	mustReceive := func() actormsg.Message {
		task, ok := receiveTimeout()
		if !ok {
			t.Fatal("timeout")
		}
		return task
	}
	_ = mustReceive()

	// Put data to db.
	// * 8 key of uid1 table1
	// * 2 key of uid2 table1
	// * 2 key of uid3 table2
	data := [][]int{
		{8, 1, 1},
		{2, 2, 1},
		{2, 3, 2},
	}
	prepareData(t, db, data)

	tasks := makeCleanTask(1, 1)
	tasks = append(tasks, makeCleanTask(2, 1)...)
	tasks = append(tasks, makeCleanTask(3, 2)...)

	// All tasks must be rescheduled.
	closed := !clean.Poll(ctx, tasks)
	require.False(t, closed)
	// uid1 table1
	task := mustReceive()
	msg := makeCleanTask(1, 1)
	msg[0].SorterTask.CleanupRatelimited = true
	require.EqualValues(t, msg[0], task)
	tasks[0] = task
	// uid2 tabl2
	task = mustReceive()
	msg = makeCleanTask(2, 1)
	require.EqualValues(t, msg[0], task)
	tasks[1] = task
	// uid3 tabl2
	task = mustReceive()
	msg = makeCleanTask(3, 2)
	require.EqualValues(t, msg[0], task)
	tasks[2] = task

	// Reschedule tasks.
	// All tasks can finish eventually.
	closed = !clean.Poll(ctx, tasks)
	require.False(t, closed)
	for {
		task, ok := receiveTimeout()
		if !ok {
			break
		}
		closed := !clean.Poll(ctx, []actormsg.Message{task})
		require.False(t, closed)
	}

	// Ensure all data are deleted.
	start := encoding.EncodeTsKey(0, 0, 0)
	limit := encoding.EncodeTsKey(4, 0, 0)
	snap, err := db.Snapshot()
	require.Nil(t, err)
	iter := snap.Iterator(start, limit)
	require.False(t, iter.First(), fmt.Sprintln(hex.EncodeToString(iter.Key())))
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Close leveldb.
	closed = !clean.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	closedWg.Wait()
	// TODO: find a better to test if iterators are leaked.
	// stats := leveldb.DBStats{}
	// require.Nil(t, db.Stats(&stats))
	// require.Zero(t, stats.AliveIterators)
	require.Nil(t, db.Close())
}

func TestCleanerCompact(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	id := 1
	db, err := db.OpenLevelDB(ctx, id, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compactRouter := actor.NewRouter(t.Name())
	compactMB := actor.NewMailbox(actor.ID(id), 1)
	compactRouter.InsertMailbox4Test(compactMB.ID(), compactMB)
	compact := NewCompactScheduler(compactRouter, cfg)
	cleaner, _, err := NewCleanerActor(id, db, nil, compact, cfg, closedWg)
	require.Nil(t, err)

	// Lower compactThreshold to speed up tests.
	compact.compactThreshold = 2
	cleaner.wbSize = 1

	// Put data to db.
	// * 1 key of uid1 table1
	// * 2 key of uid2 table1
	data := [][]int{
		{1, 1, 1},
		{2, 2, 1},
	}
	prepareData(t, db, data)

	// Empty task must not trigger compact.
	closed := !cleaner.Poll(ctx, makeCleanTask(0, 0))
	require.False(t, closed)
	_, ok := compactMB.Receive()
	require.False(t, ok)

	// Delete 2 keys must trigger compact.
	closed = !cleaner.Poll(ctx, makeCleanTask(2, 1))
	require.False(t, closed)
	_, ok = compactMB.Receive()
	require.True(t, ok)

	// Delete 1 key must not trigger compact.
	closed = !cleaner.Poll(ctx, makeCleanTask(1, 1))
	require.False(t, closed)
	_, ok = compactMB.Receive()
	require.False(t, ok)

	// Close db.
	closed = !cleaner.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}
