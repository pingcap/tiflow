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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// Status implements Unit.Status.
func (s *Syncer) Status(sourceStatus *binlog.SourceStatus) interface{} {
	syncerLocation := s.checkpoint.FlushedGlobalPoint()
	st := &pb.SyncStatus{
		TotalEvents:         s.count.Load(),
		TotalTps:            s.totalRps.Load(),
		RecentTps:           s.rps.Load(),
		TotalRows:           s.count.Load(),
		TotalRps:            s.totalRps.Load(),
		RecentRps:           s.rps.Load(),
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
		st.BinlogType = s.streamerController.GetBinlogType().String()
	}

	// only support to show `UnresolvedGroups` in pessimistic mode now.
	if s.cfg.ShardMode == config.ShardPessimistic {
		st.UnresolvedGroups = s.sgk.UnresolvedGroups()
	}

	pendingShardInfo := s.pessimist.PendingInfo()
	if pendingShardInfo != nil {
		st.BlockingDDLs = pendingShardInfo.DDLs
	} else {
		pendingOptShardInfo := s.optimist.PendingInfo()
		pendingOptShardOperation := s.optimist.PendingOperation()
		if pendingOptShardOperation != nil && pendingOptShardOperation.ConflictStage == optimism.ConflictDetected {
			st.BlockDDLOwner = utils.GenDDLLockID(pendingOptShardInfo.Source, pendingOptShardInfo.UpSchema, pendingOptShardInfo.UpTable)
			st.ConflictMsg = pendingOptShardOperation.ConflictMsg
		}
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

	rps, totalRps := int64(0), int64(0)
	if seconds > 0 && totalSeconds > 0 {
		// todo: use speed recorder count rps
		rps = (total - last) / seconds
		totalRps = total / totalSeconds

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
			s.metricsProxies.Metrics.RemainingTimeGauge.Set(float64(remainingSeconds))
		}
	}

	latestMasterPos := sourceStatus.Location.Position
	latestMasterGTIDSet := sourceStatus.Location.GetGTID()
	s.metricsProxies.Metrics.BinlogMasterPosGauge.Set(float64(latestMasterPos.Pos))
	index, err := utils.GetFilenameIndex(latestMasterPos.Name)
	if err != nil {
		s.tctx.L().Error("fail to parse binlog file", log.ShortError(err))
	} else {
		s.metricsProxies.Metrics.BinlogMasterFileGauge.Set(float64(index))
	}

	s.tctx.L().Info("binlog replication status",
		zap.Int64("total_rows", total),
		zap.Int64("total_rps", totalRps),
		zap.Int64("rps", rps),
		zap.Stringer("master_position", latestMasterPos),
		log.WrapStringerField("master_gtid", latestMasterGTIDSet),
		zap.Stringer("checkpoint", s.checkpoint))

	s.lastCount.Store(total)
	s.lastBinlogSizeCount.Store(totalBinlogSize)
	s.lastTime.Store(time.Now())
	s.totalRps.Store(totalRps)
	s.rps.Store(rps)
}
