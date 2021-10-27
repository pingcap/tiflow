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
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

// TestInjector is used to support inject test cases into syncer.
// In some cases, we use failpoint to control the test flow,
// but we may need to control the flow based on some previous status,
// so we add this TestInjector to record these status.
// NOTE: if HTTP for failpoint works well, then we may remove this.
type TestInjector struct {
	flushCheckpointStage int
}

var testInjector = TestInjector{}

// handleFlushCheckpointStage handles failpoint of `FlushCheckpointStage`.
func handleFlushCheckpointStage(expectStage, maxStage int, stageStr string) error {
	if testInjector.flushCheckpointStage != expectStage {
		return nil
	}

	log.L().Info("set FlushCheckpointStage", zap.String("failpoint", "FlushCheckpointStage"), zap.Int("stage", testInjector.flushCheckpointStage))
	if testInjector.flushCheckpointStage == maxStage {
		testInjector.flushCheckpointStage = -1 // disable for following stages.
	} else {
		testInjector.flushCheckpointStage++
	}
	return terror.ErrSyncerFailpoint.Generatef("failpoint error for FlushCheckpointStage %s", stageStr)
}
