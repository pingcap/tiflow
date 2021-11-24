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
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestModelChecking(t *testing.T) {
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Sorter
	cfg.SortDir = t.TempDir()
	cfg.LevelDB.Count = 1

	ldb, err := leveldb.Open(storage.NewMemStorage(), &opt.Options{})
	require.Nil(t, err)
	db, err := OpenDB(ctx, 1, cfg)
	require.Nil(t, err)

	// Collect metrics
	db.CollectMetrics("", 0)

	// Batch
	lbatch := leveldb.MakeBatch(0)
	lbatch.Put([]byte("k1"), []byte("v1"))
	lbatch.Put([]byte("k2"), []byte("v2"))
	lbatch.Put([]byte("k3"), []byte("v3"))
	lbatch.Delete([]byte("k2"))
	batch := db.Batch(0)
	batch.Put([]byte("k1"), []byte("v1"))
	batch.Put([]byte("k2"), []byte("v2"))
	batch.Put([]byte("k3"), []byte("v3"))
	batch.Delete([]byte("k2"))
	// Count and Commit
	require.EqualValues(t, lbatch.Len(), batch.Count())
	require.Nil(t, ldb.Write(lbatch, nil))
	require.Nil(t, batch.Commit())
	// Reset
	lbatch.Reset()
	batch.Reset()
	require.EqualValues(t, lbatch.Len(), batch.Count())

	// Snapshot
	lsnap, err := ldb.GetSnapshot()
	require.Nil(t, err)
	snap, err := db.Snapshot()
	require.Nil(t, err)

	// Iterator
	liter := lsnap.NewIterator(&util.Range{
		Start: []byte(""),
		Limit: []byte("k4"),
	}, nil)
	iter := snap.Iterator([]byte(""), []byte("k4"))
	// First
	var ok bool
	ok = liter.First()
	require.True(t, ok)
	ok = iter.First()
	require.True(t, ok)
	// Key, Value
	var key, val []byte
	key = liter.Key()
	val = liter.Value()
	require.Equal(t, []byte("k1"), key)
	require.Equal(t, []byte("v1"), val)
	key = iter.Key()
	val = iter.Value()
	require.Equal(t, []byte("k1"), key)
	require.Equal(t, []byte("v1"), val)
	// Next
	ok = liter.Next()
	require.True(t, ok)
	ok = iter.Next()
	require.True(t, ok)
	key = liter.Key()
	require.Equal(t, []byte("k3"), key)
	key = iter.Key()
	require.Equal(t, []byte("k3"), key)
	// Error
	require.Nil(t, liter.Error())
	require.Nil(t, iter.Error())

	// Release
	liter.Release()
	require.Nil(t, iter.Release())
	lsnap.Release()
	require.Nil(t, snap.Release())

	// Close
	require.Nil(t, db.Close())
	require.Nil(t, ldb.Close())
}
