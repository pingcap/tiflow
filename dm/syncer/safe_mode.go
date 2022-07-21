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
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/dumpling/export"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

func (s *Syncer) enableSafeModeByTaskCliArgs(tctx *tcontext.Context) {
	//nolint:errcheck
	s.safeMode.Add(tctx, 1)
	s.tctx.L().Info("enable safe-mode because of task cli args")
}

func (s *Syncer) enableSafeModeInitializationPhase(tctx *tcontext.Context) {
	var err error
	defer func() {
		if err != nil {
			// send error to the fatal chan to interrupt the process
			s.runFatalChan <- unit.NewProcessError(err)
		}
	}()

	s.safeMode.Reset(tctx) // in initialization phase, reset first

	// cliArgs has higher priority than config
	if s.cliArgs != nil && s.cliArgs.SafeModeDuration != "" {
		s.enableSafeModeByTaskCliArgs(tctx)
		return
	}

	if s.cfg.SafeMode {
		//nolint:errcheck
		s.safeMode.Add(tctx, 1) // add 1 but has no corresponding -1, so keeps enabled
		s.tctx.L().Info("enable safe-mode by config")
	}

	var duration time.Duration

	exportCfg := export.DefaultConfig()
	logger := tctx.L()

	err = dumpling.ParseExtraArgs(&logger, exportCfg, strings.Fields(s.cfg.MydumperConfig.ExtraArgs))
	if err != nil {
		return
	}
	fresh, err := s.IsFreshTask(tctx.Ctx)
	if err != nil {
		return
	}
	var initPhaseSeconds string
	// SafeModeDuration doesn't work when task start first with consistency is none
	if !(exportCfg.Consistency == export.ConsistencyTypeNone && fresh) {
		initPhaseSeconds = s.cfg.SafeModeDuration
	}

	failpoint.Inject("SafeModeInitPhaseSeconds", func(val failpoint.Value) {
		initPhaseSeconds = val.(string)
		s.tctx.L().Info("set initPhaseSeconds", zap.String("failpoint", "SafeModeInitPhaseSeconds"), zap.String("value", initPhaseSeconds))
	})
	failpoint.Inject("SafeModeDurationSet", func(val failpoint.Value) {
		initPhaseSeconds = val.(string)
		s.tctx.L().Info("set initPhaseSeconds", zap.String("failpoint", "SafeModeDurationSet"), zap.String("value", initPhaseSeconds))
	})
	if initPhaseSeconds == "" {
		duration = time.Second * time.Duration(2*s.cfg.CheckpointFlushInterval)
	} else {
		duration, err = time.ParseDuration(initPhaseSeconds)
		if err != nil {
			s.tctx.L().Error("enable safe-mode failed due to duration parse failed", zap.String("duration", initPhaseSeconds))
			return
		}
	}
	exitPoint := s.checkpoint.SafeModeExitPoint()
	beginLocation := s.checkpoint.GlobalPoint()
	failpoint.Inject("SafeModeDurationSetBeginLoc", func() {
		beginLocation = binlog.MustZeroLocation(mysql.MySQLFlavor)
	})
	if exitPoint != nil {
		s.tctx.L().Info("compare exitPoint and beginLocation", zap.Stringer("exitPoint", exitPoint), zap.Stringer("beginLocation", beginLocation))
		if binlog.CompareLocation(*exitPoint, beginLocation, s.cfg.EnableGTID) == 0 {
			s.tctx.L().Info("exitPoint equal to beginLocation, so disable the safe mode")
			s.checkpoint.SaveSafeModeExitPoint(nil)
			return
		}

		//nolint:errcheck
		s.safeMode.Add(tctx, 1) // enable and will revert after pass SafeModeExitLoc
		s.tctx.L().Info("enable safe-mode for safe mode exit point, will exit at", zap.Stringer("location", *exitPoint))
	} else {
		s.tctx.L().Info("enable safe-mode because of task initialization", zap.Duration("duration", duration))

		if int64(duration) > 0 {
			//nolint:errcheck
			s.safeMode.Add(tctx, 1) // enable and will revert after 2 * CheckpointFlushInterval
			go func() {
				defer func() {
					err2 := s.safeMode.Add(tctx, -1)
					if err2 != nil {
						s.runFatalChan <- unit.NewProcessError(err2)
					}
					if !s.safeMode.Enable() {
						s.tctx.L().Info("disable safe-mode after task initialization finished")
					}
				}()

				select {
				case <-tctx.Context().Done():
				case <-time.After(duration):
				}
			}()
		}
	}

	if int64(duration) == 0 {
		failpoint.Inject("SafeModeInitPhaseSeconds", func(val failpoint.Value) {
			seconds := val.(string)
			if seconds == "0s" {
				failpoint.Return()
			}
		})
		if !fresh {
			err = terror.ErrSyncerReprocessWithSafeModeFail.Generate()
		}
	}
}
