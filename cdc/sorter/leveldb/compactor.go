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
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// CompactActor is an actor that compacts db.
// It GCs delete kv entries and reclaim disk space.
type CompactActor struct {
	id       actor.ID
	db       db.DB
	closedWg *sync.WaitGroup

	metricCompactDuration prometheus.Observer
}

var _ actor.Actor = (*CompactActor)(nil)

// NewCompactActor returns a compactor actor.
func NewCompactActor(
	id int, db db.DB, wg *sync.WaitGroup, captureAddr string,
) (*CompactActor, actor.Mailbox, error) {
	wg.Add(1)
	idTag := strconv.Itoa(id)
	// Compact is CPU intensive, set capacity to 1 to reduce unnecessary tasks.
	mb := actor.NewMailbox(actor.ID(id), 1)
	return &CompactActor{
		id:       actor.ID(id),
		db:       db,
		closedWg: wg,

		metricCompactDuration: sorterCompactDurationHistogram.WithLabelValues(captureAddr, idTag),
	}, mb, nil
}

// Poll implements actor.Actor.
func (c *CompactActor) Poll(ctx context.Context, tasks []actormsg.Message) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}

	// Only compact once for every batch.
	for pos := range tasks {
		msg := tasks[pos]
		switch msg.Tp {
		case actormsg.TypeTick:
		case actormsg.TypeStop:
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msg))
		}
	}

	// A range that is large enough to cover entire db effectively.
	// See see sorter/encoding/key.go.
	start, end := []byte{0x0}, bytes.Repeat([]byte{0xff}, 128)
	now := time.Now()
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
func NewCompactScheduler(
	router *actor.Router, cfg *config.DBConfig,
) *CompactScheduler {
	return &CompactScheduler{
		router:           router,
		compactThreshold: cfg.CompactionDeletionThreshold,
	}
}

// CompactScheduler schedules compact tasks to compactors.
type CompactScheduler struct {
	// A router to compactors.
	router *actor.Router
	// The number of delete keys that triggers compact.
	compactThreshold int
}

func (s *CompactScheduler) maybeCompact(id actor.ID, deleteCount int) bool {
	if deleteCount < s.compactThreshold {
		return false
	}
	err := s.router.Send(id, actormsg.TickMessage())
	// An ongoing compaction may block compactor and cause channel full,
	// skip send the task as there is a pending task.
	if err != nil && cerrors.ErrMailboxFull.NotEqual(err) {
		log.Warn("schedule compact failed", zap.Error(err))
		return false
	}
	return true
}
