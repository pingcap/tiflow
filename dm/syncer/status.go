// Copyright 2019 PingCAP, Inc.
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

package syncer

import (
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/syncer/metrics"
)

// Status implements Unit.Status.
func (s *Syncer) Status(sourceStatus *binlog.SourceStatus) interface{} {
	syncerLocation := s.checkpoint.FlushedGlobalPoint()
	st := &pb.SyncStatus{
		TotalEvents:         s.count.Load(),
		TotalTps:            s.totalTps.Load(),
		RecentTps:           s.tps.Load(),
		SyncerBinlog:        syncerLocation.Position.String(),
		SecondsBehindMaster: s.secondsBehindMaster.Load(),
	}

	if syncerLocation.GetGTID() != nil {
		st.SyncerBinlogGtid = syncerLocation.GetGTID().String()
	}

	if sourceStatus != nil {
		st.MasterBinlog = sourceStatus.Location.Position.String()
		st.MasterBinlogGtid = sourceStatus.Location.GTIDSetStr()

		if s.cfg.EnableGTID {
			// rely on sorted GTID set when String()
			st.Synced = st.MasterBinlogGtid == st.SyncerBinlogGtid
		} else {
			syncRealPos, err := binlog.RealMySQLPos(syncerLocation.Position)
			if err != nil {
				s.tctx.L().Error("fail to parse real mysql position",
					zap.Any("position", syncerLocation.Position),
					log.ShortError(err))
			}
			st.Synced = syncRealPos.Compare(sourceStatus.Location.Position) == 0
		}
	}

	st.BinlogType = "unknown"
	if s.streamerController != nil {
		st.BinlogType = binlogTypeToString(s.streamerController.GetBinlogType())
	}

	// only support to show `UnresolvedGroups` in pessimistic mode now.
	if s.cfg.ShardMode == config.ShardPessimistic {
		st.UnresolvedGroups = s.sgk.UnresolvedGroups()
	}

	pendingShardInfo := s.pessimist.PendingInfo()
	if pendingShardInfo != nil {
		st.BlockingDDLs = pendingShardInfo.DDLs
	}

	failpoint.Inject("BlockSyncStatus", func(val failpoint.Value) {
		interval, err := time.ParseDuration(val.(string))
		if err != nil {
			s.tctx.L().Warn("inject failpoint BlockSyncStatus failed", zap.Reflect("value", val), zap.Error(err))
		} else {
			s.tctx.L().Info("set BlockSyncStatus", zap.String("failpoint", "BlockSyncStatus"), zap.Duration("value", interval))
			time.Sleep(interval)
		}
	})
	go s.printStatus(sourceStatus)
	return st
}

func (s *Syncer) printStatus(sourceStatus *binlog.SourceStatus) {
	if sourceStatus == nil {
		// often happened when source status is not interested, such as in an unit test
		return
	}
	now := time.Now()
	seconds := now.Unix() - s.lastTime.Load().Unix()
	totalSeconds := now.Unix() - s.start.Load().Unix()
	last := s.lastCount.Load()
	total := s.count.Load()

	totalBinlogSize := s.binlogSizeCount.Load()
	lastBinlogSize := s.lastBinlogSizeCount.Load()

	tps, totalTps := int64(0), int64(0)
	if seconds > 0 && totalSeconds > 0 {
		tps = (total - last) / seconds
		totalTps = total / totalSeconds

		s.currentLocationMu.RLock()
		currentLocation := s.currentLocationMu.currentLocation
		s.currentLocationMu.RUnlock()

		remainingSize := sourceStatus.Binlogs.After(currentLocation.Position)
		bytesPerSec := (totalBinlogSize - lastBinlogSize) / seconds
		if bytesPerSec > 0 {
			remainingSeconds := remainingSize / bytesPerSec
			s.tctx.L().Info("binlog replication progress",
				zap.Int64("total binlog size", totalBinlogSize),
				zap.Int64("last binlog size", lastBinlogSize),
				zap.Int64("cost time", seconds),
				zap.Int64("bytes/Second", bytesPerSec),
				zap.Int64("unsynced binlog size", remainingSize),
				zap.Int64("estimate time to catch up", remainingSeconds))
			metrics.RemainingTimeGauge.WithLabelValues(s.cfg.Name, s.cfg.SourceID, s.cfg.WorkerName).Set(float64(remainingSeconds))
		}
	}

	latestMasterPos := sourceStatus.Location.Position
	latestMasterGTIDSet := sourceStatus.Location.GetGTID()
	metrics.BinlogPosGauge.WithLabelValues("master", s.cfg.Name, s.cfg.SourceID).Set(float64(latestMasterPos.Pos))
	index, err := binlog.GetFilenameIndex(latestMasterPos.Name)
	if err != nil {
		s.tctx.L().Error("fail to parse binlog file", log.ShortError(err))
	} else {
		metrics.BinlogFileGauge.WithLabelValues("master", s.cfg.Name, s.cfg.SourceID).Set(float64(index))
	}

	s.tctx.L().Info("binlog replication status",
		zap.Int64("total_events", total),
		zap.Int64("total_tps", totalTps),
		zap.Int64("tps", tps),
		zap.Stringer("master_position", latestMasterPos),
		log.WrapStringerField("master_gtid", latestMasterGTIDSet),
		zap.Stringer("checkpoint", s.checkpoint))

	s.lastCount.Store(total)
	s.lastBinlogSizeCount.Store(totalBinlogSize)
	s.lastTime.Store(time.Now())
	s.totalTps.Store(totalTps)
	s.tps.Store(tps)
}
