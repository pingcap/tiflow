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
	"github.com/pingcap/dm/pkg/utils"
)

// spaceArgs represents args needed by spaceStrategy.
type spaceArgs struct {
	relayBaseDir   string
	remainSpace    int64 // if remain space (GB) in @RelayBaseDir less than this, then it can be purged
	uuids          []string
	activeRelayLog *streamer.RelayLogInfo // earliest active relay log info
}

func (sa *spaceArgs) SetActiveRelayLog(active *streamer.RelayLogInfo) {
	sa.activeRelayLog = active
}

func (sa *spaceArgs) String() string {
	return fmt.Sprintf("(RelayBaseDir: %s, AllowMinRemainSpace: %dGB, UUIDs: %s, ActiveRelayLog: %s)",
		sa.relayBaseDir, sa.remainSpace, strings.Join(sa.uuids, ";"), sa.activeRelayLog)
}

// spaceStrategy represents a relay purge strategy by remain space in dm-worker node.
type spaceStrategy struct {
	purging atomic.Bool

	logger log.Logger
}

func newSpaceStrategy() PurgeStrategy {
	return &spaceStrategy{
		logger: log.With(zap.String("component", "relay purger"), zap.String("strategy", "space")),
	}
}

func (s *spaceStrategy) Check(args interface{}) (bool, error) {
	sa, ok := args.(*spaceArgs)
	if !ok {
		return false, terror.ErrRelayPurgeArgsNotValid.Generate(args, args)
	}

	storageSize, err := utils.GetStorageSize(sa.relayBaseDir)
	if err != nil {
		return false, terror.Annotatef(err, "get storage size for directory %s", sa.relayBaseDir)
	}

	requiredBytes := uint64(sa.remainSpace) * 1024 * 1024 * 1024
	return storageSize.Available < requiredBytes, nil
}

func (s *spaceStrategy) Do(args interface{}) error {
	if !s.purging.CAS(false, true) {
		return terror.ErrRelayThisStrategyIsPurging.Generate()
	}
	defer s.purging.Store(false)

	sa, ok := args.(*spaceArgs)
	if !ok {
		return terror.ErrRelayPurgeArgsNotValid.Generate(args, args)
	}

	// NOTE: we purge all inactive relay log files when available space less than @remainSpace
	// maybe we can refine this to purge only part of this files every time
	return purgeRelayFilesBeforeFile(s.logger, sa.relayBaseDir, sa.uuids, sa.activeRelayLog)
}

func (s *spaceStrategy) Purging() bool {
	return s.purging.Load()
}

func (s *spaceStrategy) Type() strategyType {
	return strategySpace
}
