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

package pebble

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/pebble/encoding"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

const (
	minTableCRTsLabel      string = "minCRTs"
	maxTableCRTsLabel      string = "maxCRTs"
	tableCRTsCollectorName string = "table-crts-collector"
)

type tableCRTsCollector struct {
	minTs uint64
	maxTs uint64
}

func (t *tableCRTsCollector) Add(key pebble.InternalKey, value []byte) error {
	crts := encoding.DecodeCRTs(key.UserKey)
	if crts > t.maxTs {
		t.maxTs = crts
	}
	if crts < t.minTs {
		t.minTs = crts
	}
	return nil
}

func (t *tableCRTsCollector) Finish(userProps map[string]string) error {
	userProps[minTableCRTsLabel] = fmt.Sprintf("%d", t.minTs)
	userProps[maxTableCRTsLabel] = fmt.Sprintf("%d", t.maxTs)
	return nil
}

func (t *tableCRTsCollector) Name() string {
	return tableCRTsCollectorName
}

// NOTE: both lowerBound and upperBound are included.
func iterTable(
	db *pebble.DB,
	uniqueID uint32, tableID model.TableID,
	lowerBound, upperBound engine.Position,
) *pebble.Iterator {
	// Pebble's iterator range is left-included but right-excluded.
	upperBoundNext := upperBound.Next()
	start := encoding.EncodeTsKey(uniqueID, uint64(tableID), lowerBound.CommitTs, lowerBound.StartTs)
	end := encoding.EncodeTsKey(uniqueID, uint64(tableID), upperBoundNext.CommitTs, upperBoundNext.StartTs)

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
		TableFilter: func(userProps map[string]string) bool {
			tableMinCRTs, _ := strconv.Atoi(userProps[minTableCRTsLabel])
			tableMaxCRTs, _ := strconv.Atoi(userProps[maxTableCRTsLabel])
			return uint64(tableMaxCRTs) >= lowerBound.CommitTs && uint64(tableMinCRTs) <= upperBound.CommitTs
		},
	})
	iter.First()
	return iter
}

// OpenPebble opens a pebble.
func OpenPebble(
	id int, path string, cfg *config.DBConfig,
	cacheSize uint64,
	adjusts ...func(*pebble.Options),
) (db *pebble.DB, err error) {
	dbDir := filepath.Join(path, fmt.Sprintf("%04d", id))
	if err = os.RemoveAll(dbDir); err != nil {
		log.Warn("clean data dir fails", zap.String("dir", dbDir), zap.Error(err))
		return
	}

	opts := buildPebbleOption(cfg)
	if cacheSize > 0 {
		opts.Cache = pebble.NewCache(int64(cacheSize))
		defer opts.Cache.Unref()
	}

	for _, adjust := range adjusts {
		adjust(opts)
	}

	db, err = pebble.Open(dbDir, opts)
	return
}

func buildPebbleOption(cfg *config.DBConfig) (opts *pebble.Options) {
	opts = new(pebble.Options)
	opts.ErrorIfExists = true
	opts.DisableWAL = false // Delete range requires WAL.
	opts.MaxOpenFiles = cfg.MaxOpenFiles / cfg.Count
	opts.MaxConcurrentCompactions = 6
	opts.L0CompactionThreshold = cfg.CompactionL0Trigger
	opts.L0StopWritesThreshold = cfg.WriteL0PauseTrigger
	opts.LBaseMaxBytes = 64 << 20 // 64 MB
	opts.MemTableSize = cfg.WriterBufferSize
	opts.MemTableStopWritesThreshold = 4
	opts.Levels = make([]pebble.LevelOptions, 7)
	opts.TablePropertyCollectors = append(opts.TablePropertyCollectors,
		func() pebble.TablePropertyCollector {
			return &tableCRTsCollector{minTs: math.MaxUint64, maxTs: 0}
		},
	)

	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = cfg.BlockSize
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i == 0 {
			l.TargetFileSize = 8 << 20 // 8 MB
		} else if i < 4 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	opts.Levels[6].FilterPolicy = nil
	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize
	opts.EnsureDefaults()
	return
}
