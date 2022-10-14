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
	"context"
	"path/filepath"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/binlogstream"
)

func (s *Syncer) setInitActiveRelayLog(ctx context.Context) error {
	if s.binlogType != binlogstream.LocalBinlog {
		return nil
	}

	var (
		pos          mysql.Position
		activeSubDir string
		extractPos   bool
		err          error
	)

	indexPath := filepath.Join(s.cfg.RelayDir, utils.UUIDIndexFilename)
	subDirs, err := utils.ParseUUIDIndex(indexPath)
	if err != nil {
		return terror.Annotatef(err, "UUID index file path %s", indexPath)
	}
	if len(subDirs) == 0 {
		return terror.ErrRelayNoValidRelaySubDir.Generate(s.cfg.RelayDir)
	}

	checkLocation := s.checkpoint.GlobalPoint()
	switch {
	case binlog.ComparePosition(checkLocation.Position, binlog.MinPosition) > 0:
		// continue from previous checkpoint
		pos = checkLocation.Position
		extractPos = true
	case s.cfg.Mode == config.ModeIncrement:
		// fresh start for task-mode increment
		pos = mysql.Position{
			Name: s.cfg.Meta.BinLogName,
			Pos:  s.cfg.Meta.BinLogPos,
		}
	default:
		// start from dumper or loader, get current pos from master
		pos, _, err = s.fromDB.GetMasterStatus(s.tctx.WithContext(ctx), s.cfg.Flavor)
		if err != nil {
			return terror.Annotatef(err, "get master status")
		}
	}

	if extractPos {
		activeSubDir, _, pos, err = binlog.ExtractPos(pos, subDirs)
		if err != nil {
			return err
		}
	} else {
		var uuid string
		latestSubDir := subDirs[len(subDirs)-1]
		uuid, err = s.fromDB.GetServerUUID(ctx, s.cfg.Flavor)
		if err != nil {
			return terror.WithScope(terror.Annotatef(err, "get server UUID"), terror.ScopeUpstream)
		}
		// latest should be the current
		if !strings.HasPrefix(latestSubDir, uuid) {
			return terror.ErrSyncerUnitUUIDNotLatest.Generate(uuid, subDirs)
		}
		activeSubDir = latestSubDir
	}

	if len(pos.Name) == 0 {
		s.tctx.Logger.Warn("empty position, may because only specify GTID and hasn't saved according binlog position")
		return nil
	}
	err = s.readerHub.UpdateActiveRelayLog(s.cfg.Name, activeSubDir, pos.Name)
	s.recordedActiveRelayLog = true
	s.tctx.L().Info("current earliest active relay log", log.WrapStringerField("active relay log", s.readerHub.EarliestActiveRelayLog()))
	return err
}

func (s *Syncer) updateActiveRelayLog(pos mysql.Position) error {
	if s.binlogType != binlogstream.LocalBinlog {
		return nil
	}

	if len(pos.Name) == 0 {
		s.tctx.Logger.Warn("empty position, may because only specify GTID and hasn't saved according binlog position")
		return nil
	}

	indexPath := filepath.Join(s.cfg.RelayDir, utils.UUIDIndexFilename)
	uuids, err := utils.ParseUUIDIndex(indexPath)
	if err != nil {
		return terror.Annotatef(err, "UUID index file path %s", indexPath)
	}
	if len(uuids) == 0 {
		return terror.ErrRelayNoValidRelaySubDir.Generate(s.cfg.RelayDir)
	}

	activeSubDir, _, pos, err := binlog.ExtractPos(pos, uuids)
	if err != nil {
		return err
	}

	err = s.readerHub.UpdateActiveRelayLog(s.cfg.Name, activeSubDir, pos.Name)
	s.tctx.L().Info("current earliest active relay log", log.WrapStringerField("active relay log", s.readerHub.EarliestActiveRelayLog()))
	return err
}

func (s *Syncer) removeActiveRelayLog() {
	if s.binlogType != binlogstream.LocalBinlog {
		return
	}

	s.readerHub.RemoveActiveRelayLog(s.cfg.Name)
	s.tctx.L().Info("current earliest active relay log", log.WrapStringerField("active relay log", s.readerHub.EarliestActiveRelayLog()))
}
