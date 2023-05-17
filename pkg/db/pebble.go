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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/zap"
)

type pebbleLogger struct{ id int }

var _ pebble.Logger = (*pebbleLogger)(nil)

func (logger *pebbleLogger) Infof(format string, args ...interface{}) {
	// Do not output low-level pebble log to TiCDC log.
	log.Debug(fmt.Sprintf(format, args...), zap.Int("db", logger.id))
}

func (logger *pebbleLogger) Fatalf(format string, args ...interface{}) {
	log.Panic(fmt.Sprintf(format, args...), zap.Int("db", logger.id))
}

// TODO: Update DB config once we switch to pebble,
//
//	as some configs are not applicable to pebble.
func buildPebbleOption(id int, memInByte int, cfg *config.DBConfig) (pebble.Options, *writeStall) {
	var option pebble.Options
	option.ErrorIfExists = true
	option.DisableWAL = false // Delete range requires WAL.
	option.MaxOpenFiles = cfg.MaxOpenFiles / cfg.Count
	option.Cache = pebble.NewCache(int64(memInByte))
	option.MaxConcurrentCompactions = 6
	option.L0CompactionThreshold = cfg.CompactionL0Trigger
	option.L0StopWritesThreshold = cfg.WriteL0PauseTrigger
	option.LBaseMaxBytes = 64 << 20 // 64 MB
	option.MemTableSize = cfg.WriterBufferSize
	option.MemTableStopWritesThreshold = 4
	option.Levels = make([]pebble.LevelOptions, 7)
	for i := 0; i < len(option.Levels); i++ {
		l := &option.Levels[i]
		l.BlockSize = cfg.BlockSize
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i == 0 {
			l.TargetFileSize = 8 << 20 // 8 MB
		} else if i < 4 {
			l.TargetFileSize = option.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	option.Levels[6].FilterPolicy = nil
	option.FlushSplitBytes = option.Levels[0].TargetFileSize
	option.EnsureDefaults()

	// Event listener for logging and metrics.
	option.EventListener = pebble.MakeLoggingEventListener(&pebbleLogger{id: id})
	ws := &writeStall{}
	var stallTimeStamp int64
	option.EventListener.WriteStallBegin = func(_ pebble.WriteStallBeginInfo) {
		atomic.AddInt64(&ws.counter, 1)
		// If it's already stall, discard this time.
		atomic.CompareAndSwapInt64(&stallTimeStamp, 0, time.Now().UnixNano())
	}
	option.EventListener.WriteStallEnd = func() {
		start := atomic.LoadInt64(&stallTimeStamp)
		if start == 0 {
			return
		}
		atomic.StoreInt64(&stallTimeStamp, 0)
		ws.duration.Store(time.Since(time.Unix(0, start)))
	}
	return option, ws
}

// OpenPebble opens a pebble.
func OpenPebble(
	ctx context.Context, id int, path string, memInByte int, cfg *config.DBConfig,
) (DB, error) {
	option, ws := buildPebbleOption(id, memInByte, cfg)
	dbDir := filepath.Join(path, fmt.Sprintf("%04d", id))
	err := retry.Do(ctx, func() error {
		err1 := os.RemoveAll(dbDir)
		if err1 != nil {
			log.Warn("clean data dir fails",
				zap.String("dir", dbDir), zap.Error(err1))
		}
		return err1
	},
		retry.WithBackoffBaseDelay(500), // 0.5s
		retry.WithMaxTries(4))           // 2s in total.
	if err != nil {
		return nil, errors.Trace(err)
	}

	db, err := pebble.Open(dbDir, &option)
	if err != nil {
		return nil, err
	}
	return &pebbleDB{
		db:               db,
		metricWriteStall: ws,
	}, nil
}

// Metrics of pebble write stall.
type writeStall struct {
	counter  int64
	duration atomic.Value
}

type pebbleDB struct {
	db               *pebble.DB
	metricWriteStall *writeStall
}

var _ DB = (*pebbleDB)(nil)

func (p *pebbleDB) Iterator(lowerBound, upperBound []byte) Iterator {
	return pebbleIter{Iterator: p.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})}
}

func (p *pebbleDB) Batch(cap int) Batch {
	return pebbleBatch{
		Batch: p.db.NewBatch(),
	}
}

func (p *pebbleDB) DeleteRange(start, end []byte) error {
	return p.db.DeleteRange(start, end, nil)
}

func (p *pebbleDB) Compact(start, end []byte) error {
	return p.db.Compact(start, end)
}

func (p *pebbleDB) Close() error {
	return p.db.Close()
}

// TODO: Update metrics once we switch to pebble,
//
//	as some metrics are not applicable to pebble.
func (p *pebbleDB) CollectMetrics(i int) {
	db := p.db
	stats := db.Metrics()
	id := strconv.Itoa(i)
	sum := 0
	for i := range stats.Levels {
		sum += int(stats.Levels[i].Size)
	}
	sorter.OnDiskDataSizeGauge.
		WithLabelValues(id).Set(float64(stats.DiskSpaceUsage()))
	sorter.InMemoryDataSizeGauge.
		WithLabelValues(id).Set(float64(stats.BlockCache.Size))
	dbIteratorGauge.
		WithLabelValues(id).Set(float64(stats.TableIters))
	dbWriteDelayCount.
		WithLabelValues(id).
		Set(float64(atomic.LoadInt64(&p.metricWriteStall.counter)))
	stallDuration := p.metricWriteStall.duration.Load()
	if stallDuration != nil && stallDuration.(time.Duration) != time.Duration(0) {
		p.metricWriteStall.duration.Store(time.Duration(0))
		dbWriteDelayDuration.
			WithLabelValues(id).
			Set(stallDuration.(time.Duration).Seconds())
	}
	metricLevelCount := dbLevelCount.
		MustCurryWith(map[string]string{"id": id})
	for level, metric := range stats.Levels {
		metricLevelCount.WithLabelValues(fmt.Sprint(level)).Set(float64(metric.NumFiles))
	}
	dbBlockCacheAccess.
		WithLabelValues(id, "hit").Set(float64(stats.BlockCache.Hits))
	dbBlockCacheAccess.
		WithLabelValues(id, "miss").Set(float64(stats.BlockCache.Misses))
}

type pebbleBatch struct {
	*pebble.Batch
}

var _ Batch = (*pebbleBatch)(nil)

func (b pebbleBatch) Put(key, value []byte) {
	_ = b.Batch.Set(key, value, pebble.NoSync)
}

func (b pebbleBatch) Delete(key []byte) {
	_ = b.Batch.Delete(key, pebble.NoSync)
}

func (b pebbleBatch) Commit() error {
	return b.Batch.Commit(pebble.NoSync)
}

func (b pebbleBatch) Count() uint32 {
	return b.Batch.Count()
}

func (b pebbleBatch) Repr() []byte {
	return b.Batch.Repr()
}

func (b pebbleBatch) Reset() {
	b.Batch.Reset()
}

type pebbleIter struct {
	*pebble.Iterator
}

var _ Iterator = (*pebbleIter)(nil)

func (i pebbleIter) Valid() bool {
	return i.Iterator.Valid()
}

func (i pebbleIter) Seek(key []byte) bool {
	return i.Iterator.SeekGE(key)
}

func (i pebbleIter) Next() bool {
	return i.Iterator.Next()
}

func (i pebbleIter) Key() []byte {
	return i.Iterator.Key()
}

func (i pebbleIter) Value() []byte {
	return i.Iterator.Value()
}

func (i pebbleIter) Error() error {
	return i.Iterator.Error()
}

func (i pebbleIter) Release() error {
	return i.Iterator.Close()
}
