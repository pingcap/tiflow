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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/pkg/config"
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

	db, err = OpenPebble(ctx, 1, filepath.Join(t.TempDir(), "2"), 0, cfg)
	require.Nil(t, err)
	testDB(t, db)
}

func testDB(t *testing.T, db DB) {
	ldb, err := leveldb.Open(storage.NewMemStorage(), &opt.Options{})
	require.Nil(t, err)

	// Collect metrics
	db.CollectMetrics(0)

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

	// Iterator
	liter := ldb.NewIterator(&util.Range{
		Start: []byte(""),
		Limit: []byte("k4"),
	}, nil)
	iter := db.Iterator([]byte(""), []byte("k4"))
	// Seek
	require.True(t, liter.Seek([]byte{}))
	require.True(t, iter.Seek([]byte{}))
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

	// Compact
	require.Nil(t, db.Compact([]byte{0x00}, []byte{0xff}))

	// Close
	require.Nil(t, db.Close())
	require.Nil(t, ldb.Close())
}

func TestPebbleMetrics(t *testing.T) {
	t.Parallel()

	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	id := 1
	option, ws := buildPebbleOption(id, 0, cfg)
	db, err := pebble.Open(t.TempDir(), &option)
	require.Nil(t, err)
	pdb := &pebbleDB{
		db:               db,
		metricWriteStall: ws,
	}

	// Collect empty metrics.
	pdb.CollectMetrics(id)

	// Write stall.
	option.EventListener.WriteStallBegin(pebble.WriteStallBeginInfo{})
	time.Sleep(100 * time.Millisecond)
	option.EventListener.WriteStallEnd()
	require.EqualValues(t, 1, ws.counter)
	require.Less(t, time.Duration(0), ws.duration.Load().(time.Duration))

	// Collect write stall metrics.
	pdb.CollectMetrics(id)
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

// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/pkg/db
// cpu: Intel(R) Xeon(R) CPU E5-2630 v4 @ 2.20GHz
// BenchmarkNext/leveldb/next_1_event(s)-40                 4492518               272.8 ns/op             0 B/op          0 allocs/op
// BenchmarkNext/leveldb/next_4_event(s)-40                 1218038              1023 ns/op               0 B/op           0 allocs/op
// BenchmarkNext/leveldb/next_16_event(s)-40                 269282              4062 ns/op               0 B/op           0 allocs/op
// BenchmarkNext/leveldb/next_64_event(s)-40                  72012             16933 ns/op               0 B/op          0 allocs/op
// BenchmarkNext/leveldb/next_256_event(s)-40                 19056             65554 ns/op               0 B/op          0 allocs/op
// BenchmarkNext/leveldb/next_1024_event(s)-40                 4311            303426 ns/op               0 B/op          0 allocs/op
// BenchmarkNext/leveldb/next_4096_event(s)-40                  853           1248045 ns/op               1 B/op          0 allocs/op
// BenchmarkNext/leveldb/next_16384_event(s)-40                 230           4902989 ns/op           43826 B/op        389 allocs/op
// BenchmarkNext/leveldb/next_65536_event(s)-40                  25         240067525 ns/op         1687187 B/op       1708 allocs/op
// BenchmarkNext/leveldb/next_262144_event(s)-40                  4         285807572 ns/op          840336 B/op       6532 allocs/op
// BenchmarkNext/pebble/next_1_event(s)-40                  4241365               284.0 ns/op             0 B/op          0 allocs/op
// BenchmarkNext/pebble/next_4_event(s)-40                  1844215               683.1 ns/op             0 B/op          0 allocs/op
// BenchmarkNext/pebble/next_16_event(s)-40                  533388              2438 ns/op               0 B/op          0 allocs/op
// BenchmarkNext/pebble/next_64_event(s)-40                  118070              8653 ns/op               0 B/op          0 allocs/op
// BenchmarkNext/pebble/next_256_event(s)-40                  34298             37768 ns/op               0 B/op          0 allocs/op
// BenchmarkNext/pebble/next_1024_event(s)-40                  3860            259939 ns/op               5 B/op          0 allocs/op
// BenchmarkNext/pebble/next_4096_event(s)-40                   946           1194918 ns/op              20 B/op          0 allocs/op
// BenchmarkNext/pebble/next_16384_event(s)-40                  331           3577048 ns/op              77 B/op          0 allocs/op
// BenchmarkNext/pebble/next_65536_event(s)-40                   40          27640122 ns/op             651 B/op          0 allocs/op
// BenchmarkNext/pebble/next_262144_event(s)-40                   7         149654135 ns/op            5512 B/op          3 allocs/op
func BenchmarkNext(b *testing.B) {
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	cases := []struct {
		name string
		dbfn func(name string) DB
	}{{
		name: "leveldb",
		dbfn: func(name string) DB {
			db, err := OpenLevelDB(ctx, 1, filepath.Join(b.TempDir(), name), cfg)
			require.Nil(b, err)
			return db
		},
	}, {
		name: "pebble",
		dbfn: func(name string) DB {
			gb := 1024 * 1024 * 1024
			db, err := OpenPebble(ctx, 1, filepath.Join(b.TempDir(), name), gb, cfg)
			require.Nil(b, err)
			return db
		},
	}}

	rd := rand.New(rand.NewSource(0))
	for _, cs := range cases {
		b.Run(cs.name, func(b *testing.B) {
			for exp := 0; exp < 10; exp++ {
				count := int(math.Pow(4, float64(exp)))
				db := cs.dbfn(fmt.Sprintf("%s-%d", cs.name, count))
				batch := db.Batch(256)
				// Key length for typical workload, see sorter/encoding/key.go
				// 4 + 8 + 8 + 8 + 2 + 44 (key length, obtain by sst_dump a tikv sst file)
				key := [74]byte{}
				// Value length for typical workload, see sorter/encoding/value.go
				// 128 + 314 (key length, obtain by sst_dump a tikv sst file)
				value := [442]byte{}
				for i := 0; i < count; i++ {
					n, err := rd.Read(key[:])
					require.EqualValues(b, len(key), n)
					require.Nil(b, err)
					n, err = rd.Read(value[:])
					require.EqualValues(b, len(value), n)
					require.Nil(b, err)
					batch.Put(key[:], value[:])
					if batch.Count() == 256 {
						require.Nil(b, batch.Commit())
						batch.Reset()
					}
				}
				require.Nil(b, batch.Commit())
				b.ResetTimer()

				b.Run(fmt.Sprintf("next %d event(s)", count), func(b *testing.B) {
					iter := db.Iterator([]byte{}, bytes.Repeat([]byte{0xff}, len(key)))
					require.Nil(b, iter.Error())
					for i := 0; i < b.N; i++ {
						for ok := iter.Seek([]byte{}); ok; ok = iter.Next() {
						}
					}
				})
			}
		})
	}
}
