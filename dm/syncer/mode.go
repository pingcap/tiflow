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
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/unit"
	tcontext "github.com/pingcap/dm/pkg/context"
)

func (s *Syncer) enableSafeModeInitializationPhase(tctx *tcontext.Context) {
	s.safeMode.Reset(tctx) // in initialization phase, reset first

	if s.cfg.SafeMode {
		//nolint:errcheck
		s.safeMode.Add(tctx, 1) // add 1 but has no corresponding -1, so keeps enabled
		s.tctx.L().Info("enable safe-mode by config")
	}
	if s.checkpoint.SafeModeExitPoint() != nil {
		//nolint:errcheck
		s.safeMode.Add(tctx, 1) // enable and will revert after pass SafeModeExitLoc
		s.tctx.L().Info("enable safe-mode for safe mode exit point, will exit at", zap.Stringer("location", *s.checkpoint.SafeModeExitPoint()))
	} else {
		//nolint:errcheck
		s.safeMode.Add(tctx, 1) // enable and will revert after 2 * CheckpointFlushInterval
		go func() {
			defer func() {
				err := s.safeMode.Add(tctx, -1)
				if err != nil {
					// send error to the fatal chan to interrupt the process
					s.runFatalChan <- unit.NewProcessError(err)
				}
				if !s.safeMode.Enable() {
					s.tctx.L().Info("disable safe-mode after task initialization finished")
				}
			}()

			initPhaseSeconds := s.cfg.CheckpointFlushInterval * 2

			failpoint.Inject("SafeModeInitPhaseSeconds", func(val failpoint.Value) {
				seconds, _ := val.(int)
				initPhaseSeconds = seconds
				s.tctx.L().Info("set initPhaseSeconds", zap.String("failpoint", "SafeModeInitPhaseSeconds"), zap.Int("value", seconds))
			})
			s.tctx.L().Info("enable safe-mode because of task initialization", zap.Int("duration in seconds", initPhaseSeconds))
			select {
			case <-tctx.Context().Done():
			case <-time.After(time.Duration(initPhaseSeconds) * time.Second):
			}
		}()
	}
}
