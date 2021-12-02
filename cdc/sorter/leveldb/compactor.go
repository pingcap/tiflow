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
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/db"
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
	id int, db db.DB, cfg *config.DBConfig,
	wg *sync.WaitGroup, captureAddr string,
) (*CompactActor, actor.Mailbox, error) {
	wg.Add(1)
	idTag := strconv.Itoa(id)
	mb := actor.NewMailbox(actor.ID(id), cfg.Concurrency)
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
		c.close(ctx.Err())
		return false
	default:
	}

	// Only compact once for every batch.
	for pos := range tasks {
		msg := tasks[pos]
		switch msg.Tp {
		case actormsg.TypeTick:
		case actormsg.TypeStop:
			c.close(nil)
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msg))
		}
	}

	start, end := []byte{0x0}, bytes.Repeat([]byte{0xff}, 16)
	now := time.Now()
	if err := c.db.Compact(start, end); err != nil {
		log.Error("db compact error", zap.Error(err))
	}
	c.metricCompactDuration.Observe(time.Since(now).Seconds())

	return true
}

func (c *CompactActor) close(err error) {
	log.Info("compactor actor quit",
		zap.Uint64("ID", uint64(c.id)), zap.Error(err))
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
	if err != nil {
		// An ongoing compaction may block compactor and cause channel full.
		log.Warn("schedule compact failed", zap.Error(err))
		return false
	}
	return true
}
