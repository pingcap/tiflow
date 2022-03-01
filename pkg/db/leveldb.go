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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
)

func buildOption(cfg *config.DBConfig) opt.Options {
	var option opt.Options
	// We may have multiple DB instance, so we divide resources.
	option.OpenFilesCacheCapacity = cfg.MaxOpenFiles / cfg.Count
	option.BlockCacheCapacity = cfg.BlockCacheSize / cfg.Count
	option.BlockSize = cfg.BlockSize
	option.WriteBuffer = cfg.WriterBufferSize
	option.Compression = opt.NoCompression
	if cfg.Compression == "snappy" {
		option.Compression = opt.SnappyCompression
	}
	option.CompactionTableSize = cfg.TargetFileSizeBase
	option.CompactionL0Trigger = cfg.CompactionL0Trigger
	option.WriteL0SlowdownTrigger = cfg.WriteL0SlowdownTrigger
	option.WriteL0PauseTrigger = cfg.WriteL0PauseTrigger
	option.ErrorIfExist = true
	option.NoSync = true

	return option
}

// OpenLevelDB opens a leveldb.
func OpenLevelDB(ctx context.Context, id int, path string, cfg *config.DBConfig) (DB, error) {
	// TODO make sure path is under data dir.
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
		return nil, cerrors.ErrLevelDBSorterError.GenWithStackByArgs(err)
	}
	option := buildOption(cfg)
	db, err := leveldb.OpenFile(dbDir, &option)
	if err != nil {
		return nil, cerrors.ErrLevelDBSorterError.GenWithStackByArgs(err)
	}
	return &levelDB{
		db: db,
	}, nil
}

type levelDB struct {
	db *leveldb.DB
}

var _ DB = (*levelDB)(nil)

func (p *levelDB) Iterator(lowerBound, upperBound []byte) Iterator {
	return leveldbIter{Iterator: p.db.NewIterator(&util.Range{
		Start: lowerBound,
		Limit: upperBound,
	}, nil)}
}

func (p *levelDB) Batch(cap int) Batch {
	return leveldbBatch{
		db:    p.db,
		Batch: leveldb.MakeBatch(cap),
	}
}

func (p *levelDB) DeleteRange(start, end []byte) error {
	return errors.ErrUnimplemented.FastGenByArgs("leveldb.DeleteRange")
}

func (p *levelDB) Compact(start, end []byte) error {
	return p.db.CompactRange(util.Range{Start: start, Limit: end})
}

func (p *levelDB) Close() error {
	return p.db.Close()
}

func (p *levelDB) CollectMetrics(i int) {
	db := p.db
	stats := leveldb.DBStats{}
	err := db.Stats(&stats)
	if err != nil {
		log.Panic("leveldb error", zap.Error(err), zap.Int("db", i))
	}
	id := strconv.Itoa(i)
	sorter.OnDiskDataSizeGauge.
		WithLabelValues(id).Set(float64(stats.LevelSizes.Sum()))
	sorter.InMemoryDataSizeGauge.
		WithLabelValues(id).Set(float64(stats.BlockCacheSize))
	sorter.OpenFileCountGauge.
		WithLabelValues(id).Set(float64(stats.OpenedTablesCount))
	dbSnapshotGauge.
		WithLabelValues(id).Set(float64(stats.AliveSnapshots))
	dbIteratorGauge.
		WithLabelValues(id).Set(float64(stats.AliveIterators))
	dbReadBytes.
		WithLabelValues(id).Set(float64(stats.IORead))
	dbWriteBytes.
		WithLabelValues(id).Set(float64(stats.IOWrite))
	dbWriteDelayCount.
		WithLabelValues(id).Set(float64(stats.WriteDelayCount))
	dbWriteDelayDuration.
		WithLabelValues(id).Set(stats.WriteDelayDuration.Seconds())
	metricLevelCount := dbLevelCount.
		MustCurryWith(map[string]string{"id": id})
	for level, count := range stats.LevelTablesCounts {
		metricLevelCount.WithLabelValues(strconv.Itoa(level)).Set(float64(count))
	}
}

type leveldbBatch struct {
	db *leveldb.DB
	*leveldb.Batch
}

var _ Batch = (*leveldbBatch)(nil)

func (b leveldbBatch) Put(key, value []byte) {
	b.Batch.Put(key, value)
}

func (b leveldbBatch) Delete(key []byte) {
	b.Batch.Delete(key)
}

func (b leveldbBatch) Commit() error {
	return b.db.Write(b.Batch, nil)
}

func (b leveldbBatch) Count() uint32 {
	return uint32(b.Batch.Len())
}

func (b leveldbBatch) Repr() []byte {
	return b.Batch.Dump()
}

func (b leveldbBatch) Reset() {
	b.Batch.Reset()
}

type leveldbIter struct {
	iterator.Iterator
}

var _ Iterator = (*leveldbIter)(nil)

func (i leveldbIter) Valid() bool {
	return i.Iterator.Valid()
}

func (i leveldbIter) Next() bool {
	return i.Iterator.Next()
}

func (i leveldbIter) Key() []byte {
	return i.Iterator.Key()
}

func (i leveldbIter) Value() []byte {
	return i.Iterator.Value()
}

func (i leveldbIter) Error() error {
	return i.Iterator.Error()
}

func (i leveldbIter) Release() error {
	i.Iterator.Release()
	return nil
}
