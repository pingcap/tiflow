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

	"github.com/pingcap/ticdc/cdc/sorter/leveldb/message"
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/db"
	"github.com/pingcap/ticdc/pkg/leakutil"
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

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter(t.Name()), cfg)
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg, "")
	require.Nil(t, err)

	var wrote bool
	// Empty batch
	wrote, err = ldb.maybeWrite(false)
	require.Nil(t, err)
	require.False(t, wrote)

	// None empty batch
	ldb.wb.Put([]byte("abc"), []byte("abc"))
	wrote, err = ldb.maybeWrite(false)
	require.Nil(t, err)
	require.False(t, wrote)
	require.EqualValues(t, ldb.wb.Count(), 1)

	// None empty batch
	wrote, err = ldb.maybeWrite(true)
	require.Nil(t, err)
	require.True(t, wrote)
	require.EqualValues(t, ldb.wb.Count(), 0)

	ldb.wb.Put([]byte("abc"), []byte("abc"))
	ldb.wbSize = 1
	require.Greater(t, len(ldb.wb.Repr()), ldb.wbSize)
	wrote, err = ldb.maybeWrite(false)
	require.Nil(t, err)
	require.True(t, wrote)
	require.EqualValues(t, ldb.wb.Count(), 0)

	// Close db.
	closed := !ldb.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
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
	db, err := db.OpenLevelDB(ctx, id, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compactRouter := actor.NewRouter(t.Name())
	compactMB := actor.NewMailbox(actor.ID(id), 1)
	compactRouter.InsertMailbox4Test(compactMB.ID(), compactMB)
	compact := NewCompactScheduler(compactRouter, cfg)
	ldb, _, err := NewDBActor(id, db, cfg, compact, closedWg, "")
	require.Nil(t, err)

	// Lower compactThreshold to speed up tests.
	compact.compactThreshold = 2

	// Empty task must not trigger compact.
	task, snapCh := makeTask(make(map[message.Key][]byte), true)
	closed := !ldb.Poll(ctx, task)
	require.False(t, closed)
	<-snapCh
	_, ok := compactMB.Receive()
	require.False(t, ok)

	// Delete 3 keys must trigger compact.
	dels := map[message.Key][]byte{"a": {}, "b": {}, "c": {}}
	task, snapCh = makeTask(dels, true)
	closed = !ldb.Poll(ctx, task)
	require.False(t, closed)
	<-snapCh
	_, ok = compactMB.Receive()
	require.True(t, ok)

	// Delete 1 key must not trigger compact.
	dels = map[message.Key][]byte{"a": {}}
	task, snapCh = makeTask(dels, true)
	closed = !ldb.Poll(ctx, task)
	require.False(t, closed)
	<-snapCh
	_, ok = compactMB.Receive()
	require.False(t, ok)

	// Close db.
	closed = !ldb.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func makeTask(events map[message.Key][]byte, needSnap bool) ([]actormsg.Message, chan message.LimitedSnapshot) {
	snapCh := make(chan message.LimitedSnapshot, 1)
	return []actormsg.Message{actormsg.SorterMessage(message.Task{
		Events:   events,
		SnapCh:   snapCh,
		NeedSnap: needSnap,
	})}, snapCh
}

func TestPutReadDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter(t.Name()), cfg)
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg, "")
	require.Nil(t, err)

	// Put only.
	tasks, snapCh := makeTask(map[message.Key][]byte{"key": {}}, false)
	closed := !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	_, ok := <-snapCh
	require.False(t, ok)

	// Put and read.
	tasks, snapCh = makeTask(map[message.Key][]byte{"key": []byte("value")}, true)
	closed = !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	snap, ok := <-snapCh
	require.True(t, ok)
	iter := snap.Iterator([]byte(""), []byte{0xff})
	require.NotNil(t, iter)
	ok = iter.Seek([]byte(""))
	require.True(t, ok)
	require.EqualValues(t, iter.Key(), "key")
	ok = iter.Next()
	require.False(t, ok)
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Read only.
	tasks, snapCh = makeTask(make(map[message.Key][]byte), true)
	closed = !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	snap, ok = <-snapCh
	require.True(t, ok)
	iter = snap.Iterator([]byte(""), []byte{0xff})
	require.NotNil(t, iter)
	ok = iter.Seek([]byte(""))
	require.True(t, ok)
	require.EqualValues(t, iter.Key(), "key")
	ok = iter.Next()
	require.False(t, ok)
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Delete and read.
	tasks, snapCh = makeTask(map[message.Key][]byte{"key": {}}, true)
	closed = !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	snap, ok = <-snapCh
	require.True(t, ok)
	iter = snap.Iterator([]byte(""), []byte{0xff})
	require.NotNil(t, iter)
	ok = iter.Seek([]byte(""))
	require.False(t, ok, string(iter.Key()))
	require.Nil(t, iter.Release())
	require.Nil(t, snap.Release())

	// Close db.
	closed = !ldb.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
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

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter(t.Name()), cfg)
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg, "")
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
		tasks, _ := makeTask(map[message.Key][]byte{message.Key(key): value}, false)
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
				tasks, _ := makeTask(map[message.Key][]byte{message.Key(key): value}, false)
				closed := !ldb.Poll(ctx, tasks)
				require.False(t, closed)

			// 1 for delete.
			case 1:
				keys := model.iter(minKey, maxKey)
				delKey := keys[rd.Intn(len(keys))]
				model.delete(delKey)
				tasks, _ := makeTask(map[message.Key][]byte{delKey: {}}, false)
				closed := !ldb.Poll(ctx, tasks)
				require.False(t, closed)
			}
		}

		tasks, snapCh := makeTask(map[message.Key][]byte{}, true)
		closed := !ldb.Poll(ctx, tasks)
		require.False(t, closed)
		snap := <-snapCh
		iter := snap.Iterator([]byte(minKey), []byte(maxKey))
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
		require.Nil(t, snap.Release())
	}

	// Close db.
	closed := !ldb.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	require.Nil(t, db.Close())
}

func TestContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	compact := NewCompactScheduler(actor.NewRouter(t.Name()), cfg)
	ldb, _, err := NewDBActor(0, db, cfg, compact, closedWg, "")
	require.Nil(t, err)

	cancel()
	tasks, _ := makeTask(map[message.Key][]byte{"key": {}}, true)
	closed := !ldb.Poll(ctx, tasks)
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}
