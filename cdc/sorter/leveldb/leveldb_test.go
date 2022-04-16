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
	"bytes"
	"context"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	"github.com/pingcap/tiflow/pkg/leakutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"))
}

func TestMaybeWrite(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenPebble(ctx, 1, t.TempDir(), 0, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter[message.Task](t.Name()))
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg)
	require.Nil(t, err)

	// Empty batch
	err = ldb.maybeWrite(false)
	require.Nil(t, err)

	// None empty batch
	ldb.wb.Put([]byte("abc"), []byte("abc"))
	err = ldb.maybeWrite(false)
	require.Nil(t, err)
	require.EqualValues(t, ldb.wb.Count(), 1)

	// None empty batch
	err = ldb.maybeWrite(true)
	require.Nil(t, err)
	require.EqualValues(t, ldb.wb.Count(), 0)

	ldb.wb.Put([]byte("abc"), []byte("abc"))
	ldb.wbSize = 1
	require.Greater(t, len(ldb.wb.Repr()), ldb.wbSize)
	err = ldb.maybeWrite(false)
	require.Nil(t, err)
	require.EqualValues(t, ldb.wb.Count(), 0)

	// Close db.
	closed := !ldb.Poll(ctx, []actormsg.Message[message.Task]{actormsg.StopMessage[message.Task]()})
	require.True(t, closed)
	ldb.OnClose()
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestCompact(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	id := 1
	db, err := db.OpenPebble(ctx, id, t.TempDir(), 0, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compactRouter := actor.NewRouter[message.Task](t.Name())
	compactMB := actor.NewMailbox[message.Task](actor.ID(id), 1)
	compactRouter.InsertMailbox4Test(compactMB.ID(), compactMB)
	compact := NewCompactScheduler(compactRouter)
	ldb, _, err := NewDBActor(id, db, cfg, compact, closedWg)
	require.Nil(t, err)

	// Empty task must not trigger compact.
	task, iterCh := makeTask(make(map[message.Key][]byte), [][]byte{{0x00}, {0xff}})
	require.True(t, ldb.Poll(ctx, task))
	iter := <-iterCh
	iter.Release()
	_, ok := compactMB.Receive()
	require.False(t, ok)

	// Empty delete range task must not trigger compact.
	task = makeDelTask([2][]byte{}, 0)
	require.True(t, ldb.Poll(ctx, task))
	_, ok = compactMB.Receive()
	require.False(t, ok)

	// A valid delete range task must trigger compact.
	task = makeDelTask([2][]byte{{0x00}, {0xff}}, 3)
	require.True(t, ldb.Poll(ctx, task))
	_, ok = compactMB.Receive()
	require.True(t, ok)

	// Close db.
	closed := !ldb.Poll(ctx, []actormsg.Message[message.Task]{actormsg.StopMessage[message.Task]()})
	require.True(t, closed)
	ldb.OnClose()
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func makeDelTask(delRange [2][]byte, count int) []actormsg.Message[message.Task] {
	return []actormsg.Message[message.Task]{actormsg.ValueMessage(message.Task{
		DeleteReq: &message.DeleteRequest{
			Range: delRange,
			Count: count,
		},
	})}
}

func makeTask(
	writes map[message.Key][]byte, rg [][]byte,
) (
	[]actormsg.Message[message.Task], chan *message.LimitedIterator,
) {
	var iterReq *message.IterRequest
	var iterCh chan *message.LimitedIterator
	if len(rg) != 0 {
		iterCh = make(chan *message.LimitedIterator, 1)
		iterReq = &message.IterRequest{
			Range: [2][]byte{rg[0], rg[1]},
			IterCallback: func(iter *message.LimitedIterator) {
				iterCh <- iter
				close(iterCh)
			},
		}
	}
	return []actormsg.Message[message.Task]{actormsg.ValueMessage(message.Task{
		WriteReq: writes,
		IterReq:  iterReq,
	})}, iterCh
}

func TestPutReadDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenPebble(ctx, 1, t.TempDir(), 0, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter[message.Task](t.Name()))
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg)
	require.Nil(t, err)

	// Put only.
	tasks, iterCh := makeTask(map[message.Key][]byte{"key": []byte("value")}, nil)
	require.Nil(t, iterCh)
	closed := !ldb.Poll(ctx, tasks)
	require.False(t, closed)

	// Put and read.
	tasks, iterCh = makeTask(map[message.Key][]byte{"key": []byte("value")},
		[][]byte{{0x00}, {0xff}})
	closed = !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	iter, ok := <-iterCh
	require.True(t, ok)
	require.NotNil(t, iter)
	ok = iter.Seek([]byte(""))
	require.True(t, ok)
	require.EqualValues(t, iter.Key(), "key")
	ok = iter.Next()
	require.False(t, ok)
	require.Nil(t, iter.Release())

	// Read only.
	tasks, iterCh = makeTask(make(map[message.Key][]byte), [][]byte{{0x00}, {0xff}})
	closed = !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	iter, ok = <-iterCh
	require.True(t, ok)
	require.NotNil(t, iter)
	ok = iter.Seek([]byte(""))
	require.True(t, ok)
	require.EqualValues(t, iter.Key(), "key")
	ok = iter.Next()
	require.False(t, ok)
	require.Nil(t, iter.Release())

	// Delete and read.
	tasks = makeDelTask([2][]byte{{0x00}, {0xff}}, 0)
	iterTasks, iterCh := makeTask(make(map[message.Key][]byte), [][]byte{{0x00}, {0xff}})
	tasks = append(tasks, iterTasks...)
	closed = !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	iter, ok = <-iterCh
	require.True(t, ok)
	require.NotNil(t, iter)
	ok = iter.Seek([]byte(""))
	require.False(t, ok, string(iter.Key()))
	require.Nil(t, iter.Release())

	// Close db.
	closed = !ldb.Poll(ctx, []actormsg.Message[message.Task]{actormsg.StopMessage[message.Task]()})
	require.True(t, closed)
	ldb.OnClose()
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestAcquireIterators(t *testing.T) {
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenPebble(ctx, 1, t.TempDir(), 0, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)

	// Set max iterator count to 1.
	cfg.Concurrency = 1
	compact := NewCompactScheduler(actor.NewRouter[message.Task](t.Name()))
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg)
	require.Nil(t, err)

	// Poll two tasks.
	tasks, iterCh1 := makeTask(make(map[message.Key][]byte), [][]byte{{0x00}, {0xff}})
	tasks[0].Value.TableID = 1
	tasks2, iterCh2 := makeTask(make(map[message.Key][]byte), [][]byte{{0x00}, {0xff}})
	tasks2[0].Value.TableID = 2
	tasks = append(tasks, tasks2...)
	closed := !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	iter, ok := <-iterCh1
	require.True(t, ok)
	require.NotNil(t, iter)

	// Require iterator is not allow for now.
	closed = !ldb.Poll(ctx, []actormsg.Message[message.Task]{actormsg.ValueMessage(message.Task{})})
	require.False(t, closed)
	select {
	case <-iterCh2:
		require.FailNow(t, "should not acquire an iterator")
	default:
	}

	// Release iter and iterCh2 should be able to receive an iterator.
	require.Nil(t, iter.Release())
	closed = !ldb.Poll(ctx, []actormsg.Message[message.Task]{actormsg.ValueMessage(message.Task{})})
	require.False(t, closed)
	iter, ok = <-iterCh2
	require.True(t, ok)
	require.Nil(t, iter.Release())

	// Close db.
	closed = !ldb.Poll(ctx, []actormsg.Message[message.Task]{actormsg.StopMessage[message.Task]()})
	require.True(t, closed)
	ldb.OnClose()
	closedWg.Wait()
	require.Nil(t, db.Close())
}

type sortedMap struct {
	// sorted keys
	kvs map[message.Key][]byte
}

func (s *sortedMap) put(k message.Key, v []byte) {
	s.kvs[k] = v
}

func (s *sortedMap) delete(k message.Key) {
	delete(s.kvs, k)
}

func (s *sortedMap) iter(start, end message.Key) []message.Key {
	keys := make([]message.Key, 0)
	for k := range s.kvs {
		key := k
		// [start, end)
		if bytes.Compare([]byte(key), []byte(start)) >= 0 &&
			bytes.Compare([]byte(key), []byte(end)) < 0 {
			keys = append(keys, key)
		}
	}
	sort.Sort(sortableKeys(keys))
	return keys
}

type sortableKeys []message.Key

func (x sortableKeys) Len() int           { return len(x) }
func (x sortableKeys) Less(i, j int) bool { return bytes.Compare([]byte(x[i]), []byte(x[j])) < 0 }
func (x sortableKeys) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func TestModelChecking(t *testing.T) {
	t.Parallel()

	seed := time.Now().Unix()
	rd := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenPebble(ctx, 1, t.TempDir(), 0, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter[message.Task](t.Name()))
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg)
	require.Nil(t, err)

	minKey := message.Key("")
	maxKey := message.Key(bytes.Repeat([]byte{0xff}, 100))
	randKey := func() []byte {
		// At least 10 bytes.
		key := make([]byte, rd.Intn(90)+10)
		n, err := rd.Read(key)
		require.Greater(t, n, 0)
		require.Nil(t, err)
		return key
	}
	model := sortedMap{kvs: make(map[message.Key][]byte)}
	// Prepare 100 key value pairs.
	for i := 0; i < 100; i++ {
		key := randKey()
		value := key

		// Put to model.
		model.put(message.Key(key), value)
		// Put to db.
		tasks, _ := makeTask(map[message.Key][]byte{message.Key(key): value}, nil)
		closed := !ldb.Poll(ctx, tasks)
		require.False(t, closed)
	}

	// 100 random tests.
	for i := 0; i < 100; i++ {
		// [1, 4] ops
		ops := rd.Intn(4) + 1
		for j := 0; j < ops; j++ {
			switch rd.Intn(2) {
			// 0 for put.
			case 0:
				key := randKey()
				value := key

				model.put(message.Key(key), value)
				tasks, _ := makeTask(map[message.Key][]byte{message.Key(key): value}, nil)
				closed := !ldb.Poll(ctx, tasks)
				require.False(t, closed)

			// 1 for delete.
			case 1:
				keys := model.iter(minKey, maxKey)
				delKey := keys[rd.Intn(len(keys))]
				model.delete(delKey)
				tasks, _ := makeTask(map[message.Key][]byte{delKey: {}}, nil)
				closed := !ldb.Poll(ctx, tasks)
				require.False(t, closed)
			}
		}

		tasks, iterCh := makeTask(map[message.Key][]byte{},
			[][]byte{[]byte(minKey), []byte(maxKey)})
		closed := !ldb.Poll(ctx, tasks)
		require.False(t, closed)
		iter := <-iterCh
		iter.Seek([]byte(minKey))
		keys := model.iter(minKey, maxKey)
		for idx, key := range keys {
			require.EqualValues(t, key, iter.Key())
			require.EqualValues(t, model.kvs[key], iter.Value())
			ok := iter.Next()
			require.Equal(t, ok, idx != len(keys)-1,
				"index %d, len(model): %d, seed: %d", idx, len(model.kvs), seed)
		}
		require.Nil(t, iter.Release())
	}

	// Close db.
	closed := !ldb.Poll(ctx, []actormsg.Message[message.Task]{actormsg.StopMessage[message.Task]()})
	require.True(t, closed)
	require.Nil(t, db.Close())
}

func TestContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenPebble(ctx, 1, t.TempDir(), 0, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter[message.Task](t.Name()))
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg)
	require.Nil(t, err)

	cancel()
	tasks, _ := makeTask(map[message.Key][]byte{"key": {}}, [][]byte{{0x00}, {0xff}})
	closed := !ldb.Poll(ctx, tasks)
	require.True(t, closed)
	ldb.OnClose()
	closedWg.Wait()
	require.Nil(t, db.Close())
}
