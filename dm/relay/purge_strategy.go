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

package relay

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

type strategyType uint32

const (
	strategyNone strategyType = iota
	strategyInactive
	strategyFilename
	strategyTime
	strategySpace
)

func (s strategyType) String() string {
	switch s {
	case strategyInactive:
		return "inactive strategy"
	case strategyFilename:
		return "filename strategy"
	case strategyTime:
		return "time strategy"
	case strategySpace:
		return "space strategy"
	default:
		return "unknown strategy"
	}
}

// PurgeStrategy represents a relay log purge strategy
// two purge behaviors
//  1. purge in the background
//  2. do one time purge process
//
// a strategy can support both or one of them.
type PurgeStrategy interface {
	// Check checks whether need to do the purge in the background automatically
	Check(args interface{}) (bool, error)

	// Do does the purge process one time
	Do(args interface{}) error

	// Purging indicates whether is doing purge
	Purging() bool

	// Type returns the strategy type
	Type() strategyType
}

// StrategyArgs represents args needed by purge strategy.
type StrategyArgs interface {
	// SetActiveRelayLog sets active relay log info in args
	// this should be called before do the purging
	SetActiveRelayLog(active *streamer.RelayLogInfo)
}

var fakeStrategyTaskName = strategyFilename.String()

// filenameArgs represents args needed by filenameStrategy
// NOTE: should handle master-slave switch.
type filenameArgs struct {
	relayBaseDir string
	filename     string // specified end safe filename
	subDir       string // sub dir for @filename, empty indicates latest sub dir
	uuids        []string
	safeRelayLog *streamer.RelayLogInfo // all relay log files prior to this should be purged
}

func (fa *filenameArgs) SetActiveRelayLog(active *streamer.RelayLogInfo) {
	uuid := fa.subDir
	if len(uuid) == 0 && len(fa.uuids) > 0 {
		// no sub dir specified, use the latest one
		uuid = fa.uuids[len(fa.uuids)-1]
	}
	_, endSuffix, _ := utils.ParseSuffixForUUID(uuid)

	safeRelayLog := &streamer.RelayLogInfo{
		TaskName:   fakeStrategyTaskName,
		UUID:       uuid,
		UUIDSuffix: endSuffix,
		Filename:   fa.filename,
	}

	if active.Earlier(safeRelayLog) {
		safeRelayLog = active
	}

	fa.safeRelayLog = safeRelayLog

	// discard newer UUIDs
	uuids := make([]string, 0, len(fa.uuids))
	for _, uuid := range fa.uuids {
		_, suffix, _ := utils.ParseSuffixForUUID(uuid)
		if suffix > endSuffix {
			break
		}
		uuids = append(uuids, uuid)
	}
	fa.uuids = uuids
}

func (fa *filenameArgs) String() string {
	return fmt.Sprintf("(RelayBaseDir: %s, Filename: %s, SubDir: %s, UUIDs: %s, SafeRelayLog: %s)",
		fa.relayBaseDir, fa.filename, fa.subDir, strings.Join(fa.uuids, ";"), fa.safeRelayLog)
}

// filenameStrategy represents a relay purge strategy by filename
// similar to `PURGE BINARY LOGS TO`.
type filenameStrategy struct {
	purging atomic.Bool

	logger log.Logger
}

func newFilenameStrategy() PurgeStrategy {
	return &filenameStrategy{
		logger: log.With(zap.String("component", "relay purger"), zap.String("strategy", "file name")),
	}
}

func (s *filenameStrategy) Check(args interface{}) (bool, error) {
	// do not support purge in the background
	return false, nil
}

func (s *filenameStrategy) Do(args interface{}) error {
	if !s.purging.CAS(false, true) {
		return terror.ErrRelayThisStrategyIsPurging.Generate()
	}
	defer s.purging.Store(false)

	fa, ok := args.(*filenameArgs)
	if !ok {
		return terror.ErrRelayPurgeArgsNotValid.Generate(args, args)
	}

	return purgeRelayFilesBeforeFile(s.logger, fa.relayBaseDir, fa.uuids, fa.safeRelayLog)
}

func (s *filenameStrategy) Purging() bool {
	return s.purging.Load()
}

func (s *filenameStrategy) Type() strategyType {
	return strategyFilename
}

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
//   - not writing by relay unit
//   - not reading by sync unit and will not be read by any running tasks
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
