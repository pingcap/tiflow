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
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func TestDB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := OpenLevelDB(ctx, 1, filepath.Join(t.TempDir(), "1"), cfg)
	require.Nil(t, err)
	testDB(t, db)

	db, err = OpenPebble(ctx, 1, filepath.Join(t.TempDir(), "2"), cfg)
	require.Nil(t, err)
	testDB(t, db)
}

func testDB(t *testing.T, db DB) {
	ldb, err := leveldb.Open(storage.NewMemStorage(), &opt.Options{})
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
	require.True(t, liter.First())
	require.True(t, iter.First())
	// Valid
	require.True(t, liter.Valid())
	require.True(t, iter.Valid())
	// Key, Value
	require.Equal(t, []byte("k1"), liter.Key())
	require.Equal(t, []byte("v1"), liter.Value())
	require.Equal(t, []byte("k1"), iter.Key())
	require.Equal(t, []byte("v1"), iter.Value())
	// Next
	require.True(t, liter.Next())
	require.True(t, iter.Next())
	require.Equal(t, []byte("k3"), liter.Key())
	require.Equal(t, []byte("k3"), iter.Key())
	// Error
	require.Nil(t, liter.Error())
	require.Nil(t, iter.Error())
	// Invalid
	require.False(t, liter.Next())
	require.False(t, iter.Next())
	require.False(t, liter.Valid())
	require.False(t, iter.Valid())

	// Release
	liter.Release()
	require.Nil(t, iter.Release())
	lsnap.Release()
	require.Nil(t, snap.Release())

	// Compact
	db.Compact([]byte{0x00}, []byte{0xff})

	// Close
	require.Nil(t, db.Close())
	require.Nil(t, ldb.Close())
}

func TestPebbleMetrics(t *testing.T) {
	t.Parallel()

	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	id := 1
	option, ws := buildPebbleOption(id, cfg)
	db, err := pebble.Open(t.TempDir(), &option)
	require.Nil(t, err)
	pdb := &pebbleDB{
		db:               db,
		metricWriteStall: ws,
	}

	// Collect empty metrics.
	pdb.CollectMetrics("", id)

	// Write stall.
	option.EventListener.WriteStallBegin(pebble.WriteStallBeginInfo{})
	time.Sleep(100 * time.Millisecond)
	option.EventListener.WriteStallEnd()
	require.EqualValues(t, 1, ws.counter)
	require.Less(t, time.Duration(0), ws.duration.Load().(time.Duration))

	// Collect write stall metrics.
	pdb.CollectMetrics("", id)
	require.EqualValues(t, 1, ws.counter)
	require.Equal(t, time.Duration(0), ws.duration.Load().(time.Duration))

	// Filter out of order write stall end.
	option.EventListener.WriteStallEnd()
	require.Equal(t, time.Duration(0), ws.duration.Load().(time.Duration))

	// Write stall again.
	option.EventListener.WriteStallBegin(pebble.WriteStallBeginInfo{})
	time.Sleep(10 * time.Millisecond)
	option.EventListener.WriteStallEnd()
	require.EqualValues(t, 2, ws.counter)
	require.Less(t, time.Duration(0), ws.duration.Load().(time.Duration))

	require.Nil(t, pdb.Close())
}
