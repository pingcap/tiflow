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

package purger

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
)

// timeArgs represents args needed by timeStrategy.
type timeArgs struct {
	relayBaseDir   string
	safeTime       time.Time // if file's modified time is older than this, then it can be purged
	uuids          []string
	activeRelayLog *streamer.RelayLogInfo // earliest active relay log info
}

func (ta *timeArgs) SetActiveRelayLog(active *streamer.RelayLogInfo) {
	ta.activeRelayLog = active
}

func (ta *timeArgs) String() string {
	return fmt.Sprintf("(RelayBaseDir: %s, SafeTime: %s, UUIDs: %s, ActiveRelayLog: %s)",
		ta.relayBaseDir, ta.safeTime, strings.Join(ta.uuids, ";"), ta.activeRelayLog)
}

// timeStrategy represents a relay purge strategy by time
// similar to `PURGE BINARY LOGS BEFORE` in MySQL.
type timeStrategy struct {
	purging atomic.Bool

	logger log.Logger
}

func newTimeStrategy() PurgeStrategy {
	return &timeStrategy{
		logger: log.With(zap.String("component", "relay purger"), zap.String("strategy", "time")),
	}
}

func (s *timeStrategy) Check(args interface{}) (bool, error) {
	// for time strategy, we always try to do the purging
	return true, nil
}

func (s *timeStrategy) Stop() {
}

func (s *timeStrategy) Do(args interface{}) error {
	if !s.purging.CAS(false, true) {
		return terror.ErrRelayThisStrategyIsPurging.Generate()
	}
	defer s.purging.Store(false)

	ta, ok := args.(*timeArgs)
	if !ok {
		return terror.ErrRelayPurgeArgsNotValid.Generate(args, args)
	}

	return purgeRelayFilesBeforeFileAndTime(s.logger, ta.relayBaseDir, ta.uuids, ta.activeRelayLog, ta.safeTime)
}

func (s *timeStrategy) Purging() bool {
	return s.purging.Load()
}

func (s *timeStrategy) Type() strategyType {
	return strategyTime
}
