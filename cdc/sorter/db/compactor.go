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
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sorter/db/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type deleteThrottle struct {
	count    int
	nextTime time.Time
	rnd      *rand.Rand

	// The number of delete keys that triggers delete.
	countThreshold int
	period         time.Duration
}

func (d *deleteThrottle) reset(now time.Time) {
	// Randomize next time to avoid thundering herd problem.
	randFactor := d.rnd.Int63n(int64(d.period))
	period := d.period + time.Duration(randFactor)
	d.nextTime = now.Add(period)
	d.count = 0
}

func (d *deleteThrottle) trigger(count int, now time.Time) bool {
	if d.rnd == nil {
		// Init rnd.
		d.rnd = rand.New(rand.NewSource(rand.Int63()))
		d.reset(now)
	}
	d.count += count
	if d.count >= d.countThreshold || now.After(d.nextTime) {
		// Throttle is triggered, reset before returning true.
		d.reset(now)
		return true
	}
	return false
}

// CompactActor is an actor that compacts db.
// It GCs delete kv entries and reclaim disk space.
type CompactActor struct {
	id       actor.ID
	db       db.DB
	delete   deleteThrottle
	closedWg *sync.WaitGroup

	metricCompactDuration prometheus.Observer
}

var _ actor.Actor[message.Task] = (*CompactActor)(nil)

// NewCompactActor returns a compactor actor.
func NewCompactActor(
	id int, db db.DB, wg *sync.WaitGroup, cfg *config.DBConfig,
) (*CompactActor, actor.Mailbox[message.Task], error) {
	wg.Add(1)
	idTag := strconv.Itoa(id)
	// Compact is CPU intensive, set capacity to 1 to reduce unnecessary tasks.
	mb := actor.NewMailbox[message.Task](actor.ID(id), 1)
	return &CompactActor{
		id:       actor.ID(id),
		db:       db,
		closedWg: wg,
		delete: deleteThrottle{
			countThreshold: cfg.CompactionDeletionThreshold,
			period:         time.Duration(cfg.CompactionPeriod * int(time.Second)),
		},

		metricCompactDuration: sorterCompactDurationHistogram.WithLabelValues(idTag),
	}, mb, nil
}

// Poll implements actor.Actor.
func (c *CompactActor) Poll(ctx context.Context, tasks []actormsg.Message[message.Task]) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}

	// Only compact once for every batch.
	count := 0
	for pos := range tasks {
		msg := tasks[pos]
		switch msg.Tp {
		case actormsg.TypeValue:
			count += msg.Value.DeleteReq.Count
		case actormsg.TypeStop:
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msg))
		}
	}

	now := time.Now()
	if !c.delete.trigger(count, now) {
		return true
	}

	// A range that is large enough to cover entire db effectively.
	// See sorter/encoding/key.go.
	start, end := []byte{0x0}, bytes.Repeat([]byte{0xff}, 128)
	if err := c.db.Compact(start, end); err != nil {
		log.Error("db compact error", zap.Error(err))
	}
	c.metricCompactDuration.Observe(time.Since(now).Seconds())

	return true
}

// OnClose releases CompactActor resource.
func (c *CompactActor) OnClose() {
	log.Info("compactor actor quit", zap.Uint64("ID", uint64(c.id)))
	c.closedWg.Done()
}

// NewCompactScheduler returns a new compact scheduler.
func NewCompactScheduler(router *actor.Router[message.Task]) *CompactScheduler {
	return &CompactScheduler{router: router}
}

// CompactScheduler schedules compact tasks to compactors.
type CompactScheduler struct {
	// A router to compactors.
	router *actor.Router[message.Task]
}

// tryScheduleCompact try to schedule a compact task.
// Returns true if it schedules compact task successfully.
func (s *CompactScheduler) tryScheduleCompact(id actor.ID, deleteCount int) bool {
	task := message.Task{
		DeleteReq: &message.DeleteRequest{
			// Compactor only needs count. DeleteRange is wrote by db actor.
			Count: deleteCount,
		},
	}
	err := s.router.Send(id, actormsg.ValueMessage(task))
	// An ongoing compaction may block compactor and cause channel full,
	// skip send the task as there is a pending task.
	if err != nil && cerrors.ErrMailboxFull.NotEqual(err) {
		log.Warn("schedule compact failed", zap.Error(err))
		return false
	}
	return true
}
