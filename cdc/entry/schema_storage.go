// Copyright 2020 PingCAP, Inc.
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

package entry

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timeta "github.com/pingcap/tidb/meta"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SchemaStorage stores the schema information with multi-version
type SchemaStorage interface {
	// GetSnapshot returns the snapshot which of ts is specified.
	// It may block caller when ts is larger than ResolvedTs.
	GetSnapshot(ctx context.Context, ts uint64) (*schema.Snapshot, error)
	// GetLastSnapshot returns the last snapshot
	GetLastSnapshot() *schema.Snapshot
	// HandleDDLJob creates a new snapshot in storage and handles the ddl job
	HandleDDLJob(job *timodel.Job) error
	// AdvanceResolvedTs advances the resolved ts
	AdvanceResolvedTs(ts uint64)
	// ResolvedTs returns the resolved ts of the schema storage
	ResolvedTs() uint64
	// DoGC removes snaps that are no longer needed at the specified TS.
	// It returns the TS from which the oldest maintained snapshot is valid.
	DoGC(ts uint64) (lastSchemaTs uint64)
}

type schemaStorageImpl struct {
	snaps      []*schema.Snapshot
	snapsMu    sync.RWMutex
	gcTs       uint64
	resolvedTs uint64

	filter         *filter.Filter
	forceReplicate bool

	id model.ChangeFeedID
}

// NewSchemaStorage creates a new schema storage
func NewSchemaStorage(
	meta *timeta.Meta, startTs uint64, filter *filter.Filter,
	forceReplicate bool, id model.ChangeFeedID,
) (SchemaStorage, error) {
	var snap *schema.Snapshot
	var err error
	if meta == nil {
		snap = schema.NewEmptySnapshot(forceReplicate)
	} else {
		snap, err = schema.NewSnapshotFromMeta(meta, startTs, forceReplicate)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	schema := &schemaStorageImpl{
		snaps:          []*schema.Snapshot{snap},
		resolvedTs:     startTs,
		filter:         filter,
		forceReplicate: forceReplicate,
		id:             id,
	}
	return schema, nil
}

func (s *schemaStorageImpl) getSnapshot(ts uint64) (*schema.Snapshot, error) {
	gcTs := atomic.LoadUint64(&s.gcTs)
	if ts < gcTs {
		// Unexpected error, caller should fail immediately.
		return nil, cerror.ErrSchemaStorageGCed.GenWithStackByArgs(ts, gcTs)
	}
	resolvedTs := atomic.LoadUint64(&s.resolvedTs)
	if ts > resolvedTs {
		// Caller should retry.
		return nil, cerror.ErrSchemaStorageUnresolved.GenWithStackByArgs(ts, resolvedTs)
	}
	s.snapsMu.RLock()
	defer s.snapsMu.RUnlock()
	i := sort.Search(len(s.snaps), func(i int) bool {
		return s.snaps[i].CurrentTs() > ts
	})
	if i <= 0 {
		// Unexpected error, caller should fail immediately.
		return nil, cerror.ErrSchemaSnapshotNotFound.GenWithStackByArgs(ts)
	}
	return s.snaps[i-1], nil
}

// GetSnapshot returns the snapshot which of ts is specified
func (s *schemaStorageImpl) GetSnapshot(ctx context.Context, ts uint64) (*schema.Snapshot, error) {
	var snap *schema.Snapshot

	// The infinite retry here is a temporary solution to the `ErrSchemaStorageUnresolved` caused by
	// DDL puller lagging too much.
	startTime := time.Now()
	logTime := startTime
	err := retry.Do(ctx, func() error {
		var err error
		snap, err = s.getSnapshot(ts)
		now := time.Now()
		if now.Sub(logTime) >= 30*time.Second && isRetryable(err) {
			log.Warn("GetSnapshot is taking too long, DDL puller stuck?",
				zap.Error(err),
				zap.Uint64("ts", ts),
				zap.Duration("duration", now.Sub(startTime)),
				zap.String("namespace", s.id.Namespace),
				zap.String("changefeed", s.id.ID))
			logTime = now
		}
		return err
	}, retry.WithBackoffBaseDelay(10), retry.WithIsRetryableErr(isRetryable))

	return snap, err
}

func isRetryable(err error) bool {
	return cerror.IsRetryableError(err) && cerror.ErrSchemaStorageUnresolved.Equal(err)
}

// GetLastSnapshot returns the last snapshot
func (s *schemaStorageImpl) GetLastSnapshot() *schema.Snapshot {
	s.snapsMu.RLock()
	defer s.snapsMu.RUnlock()
	return s.snaps[len(s.snaps)-1]
}

// HandleDDLJob creates a new snapshot in storage and handles the ddl job
func (s *schemaStorageImpl) HandleDDLJob(job *timodel.Job) error {
	if s.skipJob(job) {
		s.AdvanceResolvedTs(job.BinlogInfo.FinishedTS)
		return nil
	}
	s.snapsMu.Lock()
	defer s.snapsMu.Unlock()
	var snap *schema.Snapshot
	if len(s.snaps) > 0 {
		lastSnap := s.snaps[len(s.snaps)-1]
		if job.BinlogInfo.FinishedTS <= lastSnap.CurrentTs() {
			log.Info("ignore foregone DDL", zap.Int64("jobID", job.ID),
				zap.String("DDL", job.Query),
				zap.String("namespace", s.id.Namespace),
				zap.String("changefeed", s.id.ID),
				zap.Uint64("finishTs", job.BinlogInfo.FinishedTS))
			return nil
		}
		snap = lastSnap.Copy()
	} else {
		snap = schema.NewEmptySnapshot(s.forceReplicate)
	}
	if err := snap.HandleDDL(job); err != nil {
		log.Error("handle DDL failed", zap.String("DDL", job.Query),
			zap.Stringer("job", job), zap.Error(err),
			zap.String("namespace", s.id.Namespace),
			zap.String("changefeed", s.id.ID), zap.Uint64("finishTs", job.BinlogInfo.FinishedTS))
		return errors.Trace(err)
	}
	log.Info("handle DDL", zap.String("DDL", job.Query),
		zap.Stringer("job", job),
		zap.String("namespace", s.id.Namespace),
		zap.String("changefeed", s.id.ID),
		zap.Uint64("finishTs", job.BinlogInfo.FinishedTS))

	s.snaps = append(s.snaps, snap)
	s.AdvanceResolvedTs(job.BinlogInfo.FinishedTS)
	return nil
}

// AdvanceResolvedTs advances the resolved
func (s *schemaStorageImpl) AdvanceResolvedTs(ts uint64) {
	var swapped bool
	for !swapped {
		oldResolvedTs := atomic.LoadUint64(&s.resolvedTs)
		if ts < oldResolvedTs {
			return
		}
		swapped = atomic.CompareAndSwapUint64(&s.resolvedTs, oldResolvedTs, ts)
	}
}

// ResolvedTs returns the resolved ts of the schema storage
func (s *schemaStorageImpl) ResolvedTs() uint64 {
	return atomic.LoadUint64(&s.resolvedTs)
}

// DoGC removes snaps which of ts less than this specified ts
func (s *schemaStorageImpl) DoGC(ts uint64) (lastSchemaTs uint64) {
	s.snapsMu.Lock()
	defer s.snapsMu.Unlock()
	var startIdx int
	for i, snap := range s.snaps {
		if snap.CurrentTs() > ts {
			break
		}
		startIdx = i
	}
	if startIdx == 0 {
		return s.snaps[0].CurrentTs()
	}
	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("Do GC in schema storage")
		for i := 0; i < startIdx; i++ {
			s.snaps[i].PrintStatus(log.Debug)
		}
	}

	// NOTE: Drop must be called to remove stale versions.
	s.snaps[startIdx-1].Drop()

	// copy the part of the slice that is needed instead of re-slicing it
	// to maximize efficiency of Go runtime GC.
	newSnaps := make([]*schema.Snapshot, len(s.snaps)-startIdx)
	copy(newSnaps, s.snaps[startIdx:])
	s.snaps = newSnaps

	lastSchemaTs = s.snaps[0].CurrentTs()
	atomic.StoreUint64(&s.gcTs, lastSchemaTs)
	return
}

// SkipJob skip the job should not be executed
// TiDB write DDL Binlog for every DDL Job, we must ignore jobs that are cancelled or rollback
// For older version TiDB, it write DDL Binlog in the txn that the state of job is changed to *synced*
// Now, it write DDL Binlog in the txn that the state of job is changed to *done* (before change to *synced*)
// At state *done*, it will be always and only changed to *synced*.
func (s *schemaStorageImpl) skipJob(job *timodel.Job) bool {
	log.Debug("handle DDL new commit",
		zap.String("DDL", job.Query), zap.Stringer("job", job),
		zap.String("namespace", s.id.Namespace),
		zap.String("changefeed", s.id.ID))
	if s.filter != nil && s.filter.ShouldDiscardDDL(job.Type) {
		log.Info("discard DDL",
			zap.Int64("jobID", job.ID), zap.String("DDL", job.Query),
			zap.String("namespace", s.id.Namespace),
			zap.String("changefeed", s.id.ID))
		return true
	}
	return !job.IsSynced() && !job.IsDone()
}
