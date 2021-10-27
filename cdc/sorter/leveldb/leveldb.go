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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/sorter/encoding"
	"github.com/pingcap/ticdc/cdc/sorter/leveldb/message"
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// OpenDB opens a leveldb.
func OpenDB(ctx context.Context, id int, cfg *config.SorterConfig) (*leveldb.DB, error) {
	var option opt.Options
	option.OpenFilesCacheCapacity = cfg.MaxOpenFiles / cfg.LevelDBCount
	option.BlockCacheCapacity = cfg.BlockCacheSize / cfg.LevelDBCount
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

	dbDir := filepath.Join(cfg.SortDir, fmt.Sprintf("%04d", id))
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
	db, err := leveldb.OpenFile(dbDir, &option)
	return db, errors.Trace(err)
}

// LevelActor is a leveldb actor, it reads, writes and deletes key value pair
// in its leveldb.
type LevelActor struct {
	id       actor.ID
	db       *leveldb.DB
	iterSema *semaphore.Weighted
	wbSize   int
	closedWg *sync.WaitGroup

	serde *encoding.MsgPackGenSerde

	metricWriteDuration prometheus.Observer
	metricWriteBytes    prometheus.Counter
}

var _ actor.Actor = (*LevelActor)(nil)

// NewLevelDBActor returns a leveldb actor.
func NewLevelDBActor(
	ctx context.Context, id int, db *leveldb.DB, cfg *config.SorterConfig,
	wg *sync.WaitGroup, captureAddr string,
) (*LevelActor, actor.Mailbox, error) {
	idTag := fmt.Sprint(id)
	// Write batch size should be larger than block size to save CPU.
	wbSize := cfg.BlockSize * 16
	// IterCount limits the total number of opened iterators to release leveldb
	// resources (memtables and SST files) in time.
	iterSema := semaphore.NewWeighted(int64(cfg.LevelDBConcurrency))
	mb := actor.NewMailbox(actor.ID(id), cfg.LevelDBConcurrency)
	wg.Add(1)
	return &LevelActor{
		id:       actor.ID(id),
		db:       db,
		iterSema: iterSema,
		wbSize:   wbSize,
		closedWg: wg,

		serde: &encoding.MsgPackGenSerde{},

		metricWriteDuration: sorterWriteHistogram.WithLabelValues(captureAddr, idTag),
		metricWriteBytes:    sorterWriteBytes.WithLabelValues(captureAddr, idTag),
	}, mb, nil
}

func (ldb *LevelActor) close(err error) {
	log.Info("leveldb actor quit", zap.Uint64("ID", uint64(ldb.id)), zap.Error(err))
	ldb.closedWg.Done()
}

func (ldb *LevelActor) maybeWrite(batch *leveldb.Batch, force bool) error {
	bytes := len(batch.Dump())
	if bytes >= ldb.wbSize || (force && bytes != 0) {
		startTime := time.Now()
		err := ldb.db.Write(batch, nil)
		if err != nil {
			return errors.Trace(err)
		}
		ldb.metricWriteDuration.Observe(time.Since(startTime).Seconds())
		ldb.metricWriteBytes.Add(float64(bytes))
		batch.Reset()
	}
	return nil
}

// Poll implements actor.Actor.
// It handles tasks by writing kv, deleting kv and taking iterators.
func (ldb *LevelActor) Poll(ctx context.Context, tasks []actormsg.Message) bool {
	select {
	case <-ctx.Done():
		ldb.close(ctx.Err())
		return false
	default:
	}
	var batch leveldb.Batch
	for i := range tasks {
		var task message.Task
		msg := tasks[i]
		switch msg.Tp {
		case actormsg.TypeSorter:
			task = msg.SorterTask
		case actormsg.TypeStop:
			ldb.close(nil)
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msg))
		}
		_, events, needIter, iterCh := task.TableID, task.Events, task.NeedIter, task.IterCh

		for k, v := range events {
			if len(v) != 0 {
				batch.Put([]byte(k), v)
			} else {
				// Delete the key if value is empty
				batch.Delete([]byte(k))
			}

			forceWrite := false
			if err := ldb.maybeWrite(&batch, forceWrite); err != nil {
				log.Panic("leveldb error", zap.Error(err))
			}
		}

		forceWrite := true
		if err := ldb.maybeWrite(&batch, forceWrite); err != nil {
			log.Panic("leveldb error", zap.Error(err))
		}

		if needIter {
			err := ldb.iterSema.Acquire(ctx, 1)
			if err != nil {
				if errors.Cause(err) == context.Canceled {
					ldb.close(err)
					return false
				}
				log.Panic("leveldb error, acquire iter", zap.Error(err))
			}
			iter := ldb.db.NewIterator(task.Irange, nil)
			iterCh <- message.LimitedIterator{
				Iterator: iter,
				Sema:     ldb.iterSema,
			}
		}
		close(iterCh)
	}

	return true
}
