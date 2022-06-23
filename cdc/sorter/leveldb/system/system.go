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
	lsorter "github.com/pingcap/tiflow/cdc/sorter/leveldb"
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
	dbSystem      *actor.System
	dbRouter      *actor.Router
	cleanSystem   *actor.System
	cleanRouter   *actor.Router
	compactSystem *actor.System
	compactRouter *actor.Router
	compactSched  *lsorter.CompactScheduler
	dir           string
	cfg           *config.DBConfig
	closedCh      chan struct{}
	closedWg      *sync.WaitGroup

	state   sysState
	stateMu *sync.Mutex
}

// NewSystem returns a system.
func NewSystem(dir string, cfg *config.DBConfig) *System {
	dbSystem, dbRouter := actor.NewSystemBuilder("sorter").
		WorkerNumber(cfg.Count).Build()
	cleanSystem, cleanRouter := actor.NewSystemBuilder("cleaner").
		WorkerNumber(cfg.Count).Build()
	compactSystem, compactRouter := actor.NewSystemBuilder("compactor").
		WorkerNumber(cfg.Count).Build()
	compactSched := lsorter.NewCompactScheduler(compactRouter, cfg)
	return &System{
		dbSystem:      dbSystem,
		dbRouter:      dbRouter,
		cleanSystem:   cleanSystem,
		cleanRouter:   cleanRouter,
		compactSystem: compactSystem,
		compactRouter: compactRouter,
		compactSched:  compactSched,
		dir:           dir,
		cfg:           cfg,
		closedCh:      make(chan struct{}),
		closedWg:      new(sync.WaitGroup),
		state:         sysStateInit,
		stateMu:       new(sync.Mutex),
	}
}

// ActorID returns an ActorID correspond with tableID.
func (s *System) ActorID(tableID uint64) actor.ID {
	h := fnv.New64()
	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], tableID)
	h.Write(b[:])
	return actor.ID(h.Sum64() % uint64(s.cfg.Count))
}

// Router returns db actors router.
func (s *System) Router() *actor.Router {
	return s.dbRouter
}

// CleanerRouter returns cleaner actors router.
func (s *System) CleanerRouter() *actor.Router {
	return s.cleanRouter
}

// CompactScheduler returns compaction scheduler.
func (s *System) CompactScheduler() *lsorter.CompactScheduler {
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
		return cerrors.ErrStartAStoppedLevelDBSystem.GenWithStackByArgs()
	}
	s.state = sysStateStarted

	s.compactSystem.Start(ctx)
	s.dbSystem.Start(ctx)
	s.cleanSystem.Start(ctx)
	captureAddr := config.GetGlobalServerConfig().AdvertiseAddr
	dbCount := s.cfg.Count
	for id := 0; id < dbCount; id++ {
		// Open db.
		db, err := db.OpenPebble(ctx, id, s.dir, s.cfg)
		if err != nil {
			return errors.Trace(err)
		}
		s.dbs = append(s.dbs, db)
		// Create and spawn compactor actor.
		compactor, cmb, err :=
			lsorter.NewCompactActor(id, db, s.closedWg, captureAddr)
		if err != nil {
			return errors.Trace(err)
		}
		err = s.compactSystem.Spawn(cmb, compactor)
		if err != nil {
			return errors.Trace(err)
		}
		// Create and spawn db actor.
		dbac, dbmb, err :=
			lsorter.NewDBActor(id, db, s.cfg, s.compactSched, s.closedWg, captureAddr)
		if err != nil {
			return errors.Trace(err)
		}
		err = s.dbSystem.Spawn(dbmb, dbac)
		if err != nil {
			return errors.Trace(err)
		}
		// Create and spawn cleaner actor.
		clac, clmb, err := lsorter.NewCleanerActor(
			id, db, s.cleanRouter, s.compactSched, s.cfg, s.closedWg)
		if err != nil {
			return errors.Trace(err)
		}
		err = s.cleanSystem.Spawn(clmb, clac)
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
				collectMetrics(s.dbs, captureAddr)
				metricsTimer.Reset(defaultMetricInterval)
			}
		}
	}()
	return nil
}

// Stop stops a system.
func (s *System) Stop() error {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	switch s.state {
	case sysStateStopped:
		// Already stopped.
		return nil
	case sysStateInit:
		// Not started.
		return nil
	}
	s.state = sysStateStopped

	// Stop all actors and system to release resource.
	err := s.cleanSystem.Stop()
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: compact is not context-aware, it may block.
	err = s.compactSystem.Stop()
	if err != nil {
		return errors.Trace(err)
	}
	err = s.dbSystem.Stop()
	if err != nil {
		return errors.Trace(err)
	}
	// Close metrics goroutine.
	close(s.closedCh)

	// Close dbs.
	for _, db := range s.dbs {
		err = db.Close()
		if err != nil {
			log.Warn("db close error", zap.Error(err))
		}
	}
	// Wait actors and metrics goroutine.
	s.closedWg.Wait()
	return nil
}

func collectMetrics(dbs []db.DB, captureAddr string) {
	for i := range dbs {
		db := dbs[i]
		db.CollectMetrics(captureAddr, i)
	}
}
