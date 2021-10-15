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

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/terror"
)

// inactiveArgs represents args needed by inactiveStrategy.
type inactiveArgs struct {
	relayBaseDir   string
	uuids          []string
	activeRelayLog *streamer.RelayLogInfo // earliest active relay log info
}

func (ia *inactiveArgs) SetActiveRelayLog(active *streamer.RelayLogInfo) {
	ia.activeRelayLog = active
}

func (ia *inactiveArgs) String() string {
	return fmt.Sprintf("(RelayBaseDir: %s, UUIDs: %s, ActiveRelayLog: %s)",
		ia.relayBaseDir, strings.Join(ia.uuids, ";"), ia.activeRelayLog)
}

// inactiveStrategy represents a relay purge strategy which purge all inactive relay log files
// definition of inactive relay log files:
//   * not writing by relay unit
//   * not reading by sync unit and will not be read by any running tasks
//     TODO zxc: judge tasks are running dumper / loader
type inactiveStrategy struct {
	purging atomic.Bool

	logger log.Logger
}

func newInactiveStrategy() PurgeStrategy {
	return &inactiveStrategy{
		logger: log.With(zap.String("component", "relay purger"), zap.String("strategy", "inactive binlog file")),
	}
}

func (s *inactiveStrategy) Check(args interface{}) (bool, error) {
	// do not support purge in the background
	return false, nil
}

func (s *inactiveStrategy) Do(args interface{}) error {
	if !s.purging.CAS(false, true) {
		return terror.ErrRelayThisStrategyIsPurging.Generate()
	}
	defer s.purging.Store(false)

	ia, ok := args.(*inactiveArgs)
	if !ok {
		return terror.ErrRelayPurgeArgsNotValid.Generate(args, args)
	}

	return purgeRelayFilesBeforeFile(s.logger, ia.relayBaseDir, ia.uuids, ia.activeRelayLog)
}

func (s *inactiveStrategy) Purging() bool {
	return s.purging.Load()
}

func (s *inactiveStrategy) Type() strategyType {
	return strategyInactive
}
