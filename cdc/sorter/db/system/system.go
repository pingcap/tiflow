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

package system

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/memory"
	dbsorter "github.com/pingcap/tiflow/cdc/sorter/db"
	"github.com/pingcap/tiflow/cdc/sorter/db/message"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// The interval of collecting db metrics.
const defaultMetricInterval = 15 * time.Second

// State of a system.
type sysState int

const (
	sysStateInit sysState = iota
	sysStateStarted
	sysStateStopped
)

// System manages db sorter resource.
type System struct {
	dbs           []db.DB
	dbSystem      *actor.System[message.Task]
	DBRouter      *actor.Router[message.Task]
	WriterSystem  *actor.System[message.Task]
	WriterRouter  *actor.Router[message.Task]
	ReaderSystem  *actor.System[message.Task]
	ReaderRouter  *actor.Router[message.Task]
	compactSystem *actor.System[message.Task]
	compactRouter *actor.Router[message.Task]
	compactSched  *dbsorter.CompactScheduler
	dir           string
	memPercentage float64
	cfg           *config.DBConfig
	closedCh      chan struct{}
	closedWg      *sync.WaitGroup

	state   sysState
	stateMu *sync.Mutex
}

// NewSystem returns a system.
func NewSystem(dir string, memPercentage float64, cfg *config.DBConfig) *System {
	// A system polles actors that read and write db.
	dbSystem, dbRouter := actor.NewSystemBuilder[message.Task]("sorter-db").
		WorkerNumber(cfg.Count).Build()
	// A system polles actors that compact db, garbage collection.
	compactSystem, compactRouter := actor.NewSystemBuilder[message.Task]("sorter-compactor").
		WorkerNumber(cfg.Count).Build()
	// A system polles actors that receive events from Puller and batch send
	// writes to db.
	writerSystem, writerRouter := actor.NewSystemBuilder[message.Task]("sorter-writer").
		WorkerNumber(cfg.Count).Throughput(4, 64).Build()
	readerSystem, readerRouter := actor.NewSystemBuilder[message.Task]("sorter-reader").
		WorkerNumber(cfg.Count).Throughput(4, 64).Build()
	compactSched := dbsorter.NewCompactScheduler(compactRouter)
	return &System{
		dbSystem:      dbSystem,
		DBRouter:      dbRouter,
		WriterSystem:  writerSystem,
		WriterRouter:  writerRouter,
		ReaderSystem:  readerSystem,
		ReaderRouter:  readerRouter,
		compactSystem: compactSystem,
		compactRouter: compactRouter,
		compactSched:  compactSched,
		dir:           dir,
		memPercentage: memPercentage,
		cfg:           cfg,
		closedCh:      make(chan struct{}),
		closedWg:      new(sync.WaitGroup),
		state:         sysStateInit,
		stateMu:       new(sync.Mutex),
	}
}

// DBActorID returns an DBActorID correspond with tableID.
func (s *System) DBActorID(tableID uint64) actor.ID {
	h := fnv.New64()
	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], tableID)
	h.Write(b[:])
	return actor.ID(h.Sum64() % uint64(s.cfg.Count))
}

// CompactScheduler returns compaction scheduler.
func (s *System) CompactScheduler() *dbsorter.CompactScheduler {
	return s.compactSched
}

// Start starts a system.
func (s *System) Start(ctx context.Context) error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.state == sysStateStarted {
		// Already started.
		return nil
	} else if s.state == sysStateStopped {
		return cerrors.ErrStartAStoppedDBSystem.GenWithStackByArgs()
	}
	s.state = sysStateStarted

	s.compactSystem.Start(ctx)
	s.dbSystem.Start(ctx)
	s.WriterSystem.Start(ctx)
	s.ReaderSystem.Start(ctx)
	totalMemory, err := memory.MemTotal()
	if err != nil {
		return errors.Trace(err)
	}
	memInBytePerDB := float64(totalMemory) * s.memPercentage / float64(s.cfg.Count)
	for id := 0; id < s.cfg.Count; id++ {
		// Open db.
		db, err := db.OpenPebble(
			ctx, id, s.dir, s.cfg, db.WithCache(int(memInBytePerDB)), db.WithTableCRTsCollectors())
		if err != nil {
			return errors.Trace(err)
		}
		s.dbs = append(s.dbs, db)
		// Create and spawn compactor actor.
		compactor, cmb, err := dbsorter.NewCompactActor(id, db, s.closedWg, s.cfg)
		if err != nil {
			return errors.Trace(err)
		}
		err = s.compactSystem.Spawn(cmb, compactor)
		if err != nil {
			return errors.Trace(err)
		}
		// Create and spawn db actor.
		dbac, dbmb, err := dbsorter.NewDBActor(id, db, s.cfg, s.compactSched, s.closedWg)
		if err != nil {
			return errors.Trace(err)
		}
		err = s.dbSystem.Spawn(dbmb, dbac)
		if err != nil {
			return errors.Trace(err)
		}
	}
	s.closedWg.Add(1)
	go func() {
		defer s.closedWg.Done()
		metricsTimer := time.NewTimer(defaultMetricInterval)
		defer metricsTimer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.closedCh:
				return
			case <-metricsTimer.C:
				collectMetrics(s.dbs)
				metricsTimer.Reset(defaultMetricInterval)
			}
		}
	}()
	return nil
}

// Stop stops a system.
func (s *System) Stop() {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	switch s.state {
	case sysStateStopped:
		// Already stopped.
		return
	case sysStateInit:
		// Not started.
		return
	}
	s.state = sysStateStopped

	// Stop all actors and system to release resource.
	s.WriterSystem.Stop()
	s.ReaderSystem.Stop()
	// TODO: compact is not context-aware, it may block.
	s.compactSystem.Stop()
	s.dbSystem.Stop()
	// Close metrics goroutine.
	close(s.closedCh)

	// Close dbs.
	for i, db := range s.dbs {
		err := db.Close()
		if err != nil {
			log.Warn("db close error", zap.Int("ID", i), zap.Error(err))
		}
	}
	// Wait actors and metrics goroutine.
	s.closedWg.Wait()
}

func collectMetrics(dbs []db.DB) {
	for i := range dbs {
		db := dbs[i]
		db.CollectMetrics(i)
	}
}
