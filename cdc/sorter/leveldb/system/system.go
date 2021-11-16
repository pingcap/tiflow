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
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/sorter"
	lsorter "github.com/pingcap/ticdc/cdc/sorter/leveldb"
	"github.com/pingcap/ticdc/pkg/actor"
	"github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"
)

// The interval of collecting leveldb metrics.
const defaultMetricInterval = 15 * time.Second

// State of a system.
type sysState int

const (
	sysStateInit sysState = iota
	sysStateStarted
	sysStateStopped
)

// System manages leveldb sorter resource.
type System struct {
	dbs         []*leveldb.DB
	dbSystem    *actor.System
	dbRouter    *actor.Router
	cleanSystem *actor.System
	cleanRouter *actor.Router
	cfg         *config.SorterConfig
	closedCh    chan struct{}
	closedWg    *sync.WaitGroup

	state   sysState
	stateMu *sync.Mutex
}

// NewSystem returns a system.
func NewSystem(cfg *config.SorterConfig) *System {
	dbSystem, dbRouter := actor.NewSystemBuilder("sorter").
		WorkerNumber(cfg.LevelDB.Count).Build()
	cleanSystem, cleanRouter := actor.NewSystemBuilder("cleaner").
		WorkerNumber(cfg.LevelDB.Count).Build()
	return &System{
		dbSystem:    dbSystem,
		dbRouter:    dbRouter,
		cleanSystem: cleanSystem,
		cleanRouter: cleanRouter,
		cfg:         cfg,
		closedCh:    make(chan struct{}),
		closedWg:    new(sync.WaitGroup),
		state:       sysStateInit,
		stateMu:     new(sync.Mutex),
	}
}

// ActorID returns an ActorID correspond with tableID.
func (s *System) ActorID(tableID uint64) actor.ID {
	h := fnv.New64()
	b := [8]byte{}
	binary.LittleEndian.PutUint64(b[:], tableID)
	h.Write(b[:])
	return actor.ID(h.Sum64() % uint64(s.cfg.LevelDB.Count))
}

// Router returns leveldb actors router.
func (s *System) Router() *actor.Router {
	return s.dbRouter
}

// CleanerRouter returns cleaner actors router.
func (s *System) CleanerRouter() *actor.Router {
	return s.cleanRouter
}

// broadcase messages to actors in the router.
// Caveats it may lose messages quietly.
func (s *System) broadcast(ctx context.Context, router *actor.Router, msg message.Message) {
	dbCount := s.cfg.LevelDB.Count
	for id := 0; id < dbCount; id++ {
		err := router.SendB(ctx, actor.ID(id), msg)
		if err != nil {
			log.Warn("broadcast message failed",
				zap.Int("ID", id), zap.Any("message", msg))
		}
	}
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

	s.dbSystem.Start(ctx)
	s.cleanSystem.Start(ctx)
	captureAddr := config.GetGlobalServerConfig().AdvertiseAddr
	dbCount := s.cfg.LevelDB.Count
	for id := 0; id < dbCount; id++ {
		// Open leveldb.
		db, err := lsorter.OpenDB(ctx, id, s.cfg)
		if err != nil {
			return errors.Trace(err)
		}
		s.dbs = append(s.dbs, db)
		// Create and spawn leveldb actor.
		dbac, dbmb, err := lsorter.NewLevelDBActor(
			ctx, id, db, s.cfg, s.closedWg, captureAddr)
		if err != nil {
			return errors.Trace(err)
		}
		err = s.dbSystem.Spawn(dbmb, dbac)
		if err != nil {
			return errors.Trace(err)
		}
		// Create and spawn cleaner actor.
		clac, clmb, err := lsorter.NewCleanerActor(
			id, db, s.cleanRouter, s.cfg, s.closedWg)
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

	// TODO caller should pass context.
	deadline := time.Now().Add(1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	// Close actors
	s.broadcast(ctx, s.dbRouter, message.StopMessage())
	s.broadcast(ctx, s.cleanRouter, message.StopMessage())
	// Close metrics goroutine.
	close(s.closedCh)
	// Wait actors and metrics goroutine.
	s.closedWg.Wait()

	// Stop systems.
	err := s.dbSystem.Stop()
	if err != nil {
		return errors.Trace(err)
	}
	err = s.cleanSystem.Stop()
	if err != nil {
		return errors.Trace(err)
	}

	// Close leveldbs.
	for _, db := range s.dbs {
		err = db.Close()
		if err != nil {
			log.Warn("leveldb close error", zap.Error(err))
		}
	}
	return nil
}

func collectMetrics(dbs []*leveldb.DB, captureAddr string) {
	for i := range dbs {
		db := dbs[i]
		stats := leveldb.DBStats{}
		err := db.Stats(&stats)
		if err != nil {
			log.Panic("leveldb error", zap.Error(err), zap.Int("db", i))
		}
		id := strconv.Itoa(i)
		sorter.OnDiskDataSizeGauge.
			WithLabelValues(captureAddr, id).Set(float64(stats.LevelSizes.Sum()))
		sorter.InMemoryDataSizeGauge.
			WithLabelValues(captureAddr, id).Set(float64(stats.BlockCacheSize))
		sorter.OpenFileCountGauge.
			WithLabelValues(captureAddr, id).Set(float64(stats.OpenedTablesCount))
		sorterDBSnapshotGauge.
			WithLabelValues(captureAddr, id).Set(float64(stats.AliveSnapshots))
		sorterDBIteratorGauge.
			WithLabelValues(captureAddr, id).Set(float64(stats.AliveIterators))
		sorterDBReadBytes.
			WithLabelValues(captureAddr, id).Set(float64(stats.IORead))
		sorterDBWriteBytes.
			WithLabelValues(captureAddr, id).Set(float64(stats.IOWrite))
		sorterDBWriteDelayCount.
			WithLabelValues(captureAddr, id).Set(float64(stats.WriteDelayCount))
		sorterDBWriteDelayDuration.
			WithLabelValues(captureAddr, id).Set(stats.WriteDelayDuration.Seconds())
		metricLevelCount := sorterDBLevelCount.
			MustCurryWith(map[string]string{"capture": captureAddr, "id": id})
		for level, count := range stats.LevelTablesCounts {
			metricLevelCount.WithLabelValues(strconv.Itoa(level)).Set(float64(count))
		}
	}
}
