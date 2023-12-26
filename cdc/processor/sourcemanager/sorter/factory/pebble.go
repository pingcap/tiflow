// Copyright 2022 PingCAP, Inc.
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

package factory

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	epebble "github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter/pebble"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

func createPebbleDBs(
	dir string, cfg *config.DBConfig,
	memQuotaInBytes uint64,
) ([]*pebble.DB, []writeStall, error) {
	dbs := make([]*pebble.DB, 0, cfg.Count)
	writeStalls := make([]writeStall, cfg.Count)

	cache := pebble.NewCache(int64(memQuotaInBytes))
	defer cache.Unref()
	for id := 0; id < cfg.Count; id++ {
		ws := writeStalls[id]
		adjust := func(opts *pebble.Options) {
			opts.EventListener = pebble.MakeLoggingEventListener(&pebbleLogger{id: id})

			opts.EventListener.WriteStallBegin = func(_ pebble.WriteStallBeginInfo) {
				atomic.AddUint64(&ws.counter, 1)
				atomic.CompareAndSwapInt64(&ws.startAt, 0, time.Now().UnixNano())
			}
			opts.EventListener.WriteStallEnd = func() {
				startAt := atomic.SwapInt64(&ws.startAt, 0)
				if startAt != 0 {
					elapsed := time.Since(time.Unix(0, startAt)).Milliseconds()
					atomic.AddInt64(&ws.durInMs, elapsed)
				}
			}
			opts.EventListener.CompactionEnd = func(job pebble.CompactionInfo) {
				idstr := strconv.Itoa(id + 1)
				x := sorter.CompactionDuration().WithLabelValues(idstr)
				x.Observe(job.TotalDuration.Seconds())
			}
		}

		db, err := epebble.OpenPebble(id, dir, cfg, cache, adjust)
		if err != nil {
			log.Error("create pebble fails", zap.String("dir", dir), zap.Int("id", id), zap.Error(err))
			for _, db := range dbs {
				db.Close()
			}
			return nil, nil, err
		}
		log.Info("create pebble instance success",
			zap.Int("id", id+1),
			zap.Uint64("sharedCacheSize", memQuotaInBytes))
		dbs = append(dbs, db)
	}
	return dbs, writeStalls, nil
}

type pebbleLogger struct{ id int }

var _ pebble.Logger = (*pebbleLogger)(nil)

func (logger *pebbleLogger) Infof(format string, args ...interface{}) {
	// Do not output low-level pebble log to TiCDC log.
	log.Debug(fmt.Sprintf(format, args...), zap.Int("db", logger.id))
}

func (logger *pebbleLogger) Fatalf(format string, args ...interface{}) {
	log.Panic(fmt.Sprintf(format, args...), zap.Int("db", logger.id))
}

type writeStall struct {
	counter uint64
	startAt int64
	durInMs int64
}
