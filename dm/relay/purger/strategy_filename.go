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

var fakeTaskName = strategyFilename.String()

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
		TaskName:   fakeTaskName,
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
