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
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/leakutil"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"))
}

func TestMaybeWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.GetDefaultServerConfig().Clone().Sorter
	cfg.SortDir = t.TempDir()
	cfg.LevelDBCount = 1

	db, err := OpenDB(ctx, 1, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	ldb, _, err := NewLevelDBActor(ctx, 0, db, cfg, closedWg, "")
	require.Nil(t, err)
	stats := leveldb.DBStats{}
	err = ldb.db.Stats(&stats)
	require.Nil(t, err)
	writeBase := stats.IOWrite
	wb := &leveldb.Batch{}

	// Empty batch
	err = ldb.maybeWrite(wb, false)
	require.Nil(t, err)
	err = ldb.db.Stats(&stats)
	require.Nil(t, err)
	require.Equal(t, stats.IOWrite, writeBase)

	// Empty batch, force write
	err = ldb.maybeWrite(wb, true)
	require.Nil(t, err)
	err = ldb.db.Stats(&stats)
	require.Nil(t, err)
	require.Equal(t, stats.IOWrite, writeBase)

	// None empty batch
	wb.Put([]byte("abc"), []byte("abc"))
	err = ldb.maybeWrite(wb, false)
	require.Nil(t, err)
	require.Equal(t, wb.Len(), 1)

	// None empty batch
	err = ldb.maybeWrite(wb, true)
	require.Nil(t, err)
	require.Equal(t, wb.Len(), 0)

	wb.Put([]byte("abc"), []byte("abc"))
	ldb.wbSize = 1
	require.Greater(t, len(wb.Dump()), ldb.wbSize)
	err = ldb.maybeWrite(wb, false)
	require.Nil(t, err)
	require.Equal(t, wb.Len(), 0)

	// Close leveldb.
	closed := !ldb.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func makeTask(events map[message.Key][]byte, needIter bool) ([]actormsg.Message, chan message.LimitedIterator) {
	iterCh := make(chan message.LimitedIterator, 1)
	return []actormsg.Message{actormsg.SorterMessage(message.Task{
		Events:   events,
		IterCh:   iterCh,
		NeedIter: needIter,
	})}, iterCh
}

func TestPutReadDelete(t *testing.T) {
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Sorter
	cfg.SortDir = t.TempDir()
	cfg.LevelDBCount = 1

	db, err := OpenDB(ctx, 1, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	ldb, _, err := NewLevelDBActor(ctx, 0, db, cfg, closedWg, "")
	require.Nil(t, err)

	// Put only.
	tasks, iterCh := makeTask(map[message.Key][]byte{"key": {}}, false)
	closed := !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	_, ok := <-iterCh
	require.False(t, ok)

	// Put and read.
	tasks, iterCh = makeTask(map[message.Key][]byte{"key": []byte("value")}, true)
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
	iter.Release()
	iter.Sema.Release(1)

	// Read only.
	tasks, iterCh = makeTask(make(map[message.Key][]byte), true)
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
	iter.Release()
	iter.Sema.Release(1)

	// Delete and read.
	tasks, iterCh = makeTask(map[message.Key][]byte{"key": {}}, true)
	closed = !ldb.Poll(ctx, tasks)
	require.False(t, closed)
	iter, ok = <-iterCh
	require.True(t, ok)
	require.NotNil(t, iter)
	ok = iter.Seek([]byte(""))
	require.False(t, ok, string(iter.Key()))
	iter.Release()
	iter.Sema.Release(1)

	// Close leveldb.
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
	seed := time.Now().Unix()
	rd := rand.New(rand.NewSource(seed))
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Sorter
	cfg.SortDir = t.TempDir()
	cfg.LevelDBCount = 1

	db, err := OpenDB(ctx, 1, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	ldb, _, err := NewLevelDBActor(ctx, 0, db, cfg, closedWg, "")
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
		// Put to leveldb.
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

		tasks, iterCh := makeTask(map[message.Key][]byte{}, true)
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
	}

	// Close leveldb.
	closed := !ldb.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	require.Nil(t, db.Close())
}

func TestContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.GetDefaultServerConfig().Clone().Sorter
	cfg.SortDir = t.TempDir()
	cfg.LevelDBCount = 1

	db, err := OpenDB(ctx, 1, cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	ldb, _, err := NewLevelDBActor(ctx, 0, db, cfg, closedWg, "")
	require.Nil(t, err)

	cancel()
	tasks, _ := makeTask(map[message.Key][]byte{"key": {}}, true)
	closed := !ldb.Poll(ctx, tasks)
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}
