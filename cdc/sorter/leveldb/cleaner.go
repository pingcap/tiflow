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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/sorter/encoding"
	"github.com/pingcap/ticdc/cdc/sorter/leveldb/message"
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/syndtr/goleveldb/leveldb"
	lutil "github.com/syndtr/goleveldb/leveldb/util"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// CleanerActor is an actor that can clean up table data asynchronously.
type CleanerActor struct {
	id       actor.ID
	db       *leveldb.DB
	wbSize   int
	closedWg *sync.WaitGroup

	limiter *rate.Limiter
	router  *actor.Router
}

var _ actor.Actor = (*CleanerActor)(nil)

// NewCleanerActor returns a cleaner actor.
func NewCleanerActor(
	id int, db *leveldb.DB, router *actor.Router,
	cfg *config.SorterConfig, wg *sync.WaitGroup,
) (*CleanerActor, actor.Mailbox, error) {
	wg.Add(1)
	wbSize := 500 // default write batch size.
	if (cfg.LevelDB.CleanupSpeedLimit / 2) < wbSize {
		// wb size must be less than speed limit, otherwise it is easily
		// rate-limited.
		wbSize = cfg.LevelDB.CleanupSpeedLimit / 2
	}
	limiter := rate.NewLimiter(rate.Limit(cfg.LevelDB.CleanupSpeedLimit), wbSize*2)
	mb := actor.NewMailbox(actor.ID(id), cfg.LevelDB.Concurrency)
	return &CleanerActor{
		id:       actor.ID(id),
		db:       db,
		wbSize:   wbSize,
		closedWg: wg,
		limiter:  limiter,
		router:   router,
	}, mb, nil
}

// Poll implements actor.Actor.
func (clean *CleanerActor) Poll(ctx context.Context, tasks []actormsg.Message) bool {
	select {
	case <-ctx.Done():
		clean.close(ctx.Err())
		return false
	default:
	}

	reschedulePos := -1
	rescheduleDelay := time.Duration(0)
	var batch leveldb.Batch
TASKS:
	for pos := range tasks {
		var task message.Task
		msg := tasks[pos]
		switch msg.Tp {
		case actormsg.TypeSorterTask:
			task = msg.SorterTask
		case actormsg.TypeStop:
			clean.close(nil)
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msg))
		}
		if !task.Cleanup {
			log.Panic("unexpected message", zap.Any("message", msg))
		}

		start := encoding.EncodeTsKey(task.UID, task.TableID, 0)
		limit := encoding.EncodeTsKey(task.UID, task.TableID+1, 0)
		iterRange := &lutil.Range{
			Start: start,
			Limit: limit,
		}
		iter := clean.db.NewIterator(iterRange, nil)

		// Force writes the first batch if the task is rescheduled (rate limited).
		force := task.CleanupRatelimited

		for hasNext := iter.Seek(start); hasNext; hasNext = iter.Next() {
			batch.Delete(iter.Key())

			// TODO it's similar to LevelActor.maybeWrite,
			//      they should be unified.
			if batch.Len() >= clean.wbSize {
				delay, err := clean.writeRateLimited(&batch, force)
				if err != nil {
					log.Panic("leveldb error", zap.Error(err))
				}
				if delay != 0 {
					// Rate limited, break and reschedule tasks.
					// After the delay, this batch can be write forcibly.
					reschedulePos = pos
					rescheduleDelay = delay
					break TASKS
				}
				batch.Reset()
				force = false
			}
		}
		// Release iterator in time.
		iter.Release()
		// Ignore rate limit and force write remaining kv.
		_, err := clean.writeRateLimited(&batch, true)
		if err != nil {
			log.Panic("leveldb error", zap.Error(err))
		}
	}

	// Reschedule rate limited tasks.
	if reschedulePos >= 0 {
		clean.reschedule(ctx, tasks[reschedulePos:], rescheduleDelay)
	}

	return true
}

func (clean *CleanerActor) close(err error) {
	log.Info("cleaner actor quit",
		zap.Uint64("ID", uint64(clean.id)), zap.Error(err))
	clean.closedWg.Done()
}

func (clean *CleanerActor) writeRateLimited(
	batch *leveldb.Batch, force bool,
) (time.Duration, error) {
	count := batch.Len()
	// Skip rate limiter, if force write.
	if !force {
		reservation := clean.limiter.ReserveN(time.Now(), count)
		if reservation != nil {
			if !reservation.OK() {
				log.Panic("write batch too large",
					zap.Int("wbSize", count),
					zap.Int("limit", clean.limiter.Burst()))
			}
			delay := reservation.Delay()
			if delay != 0 {
				// Rate limited, wait.
				return delay, nil
			}
		}
	}
	err := clean.db.Write(batch, nil)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return 0, nil
}

func (clean *CleanerActor) reschedule(
	ctx context.Context, tasks []actormsg.Message, delay time.Duration,
) {
	id := clean.id
	msgs := append([]actormsg.Message{}, tasks...)
	// Reschedule tasks respect after delay.
	time.AfterFunc(delay, func() {
		for i := range msgs {
			// Mark the first task is rescheduled due to rate limit.
			if i == 0 {
				msgs[i].SorterTask.CleanupRatelimited = true
			}
			// Blocking send to ensure that no tasks are lost.
			err := clean.router.SendB(ctx, id, msgs[i])
			if err != nil {
				log.Warn("drop table clean-up task",
					zap.Uint64("tableID", msgs[i].SorterTask.TableID))
			}
		}
	})
}
