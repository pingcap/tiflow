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
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// subRelayFiles represents relay log files in one sub directory.
type subRelayFiles struct {
	dir    string   // sub directory path
	files  []string // path of relay log files
	hasAll bool     // whether all relay log files in @dir are included in @files
}

// purgeRelayFilesBeforeFile purge relay log files which are older than safeRelay.
func purgeRelayFilesBeforeFile(logger log.Logger, relayBaseDir string, uuids []string, safeRelay *streamer.RelayLogInfo) error {
	files, err := getRelayFilesBeforeFile(logger, relayBaseDir, uuids, safeRelay)
	if err != nil {
		return terror.Annotatef(err, "get relay files from directory %s before file %+v with UUIDs %v", relayBaseDir, safeRelay, uuids)
	}

	return purgeRelayFiles(logger, files)
}

// purgeRelayFilesBeforeFileAndTime purge relay log files which are older than safeRelay and safeTime.
func purgeRelayFilesBeforeFileAndTime(logger log.Logger, relayBaseDir string, uuids []string, safeRelay *streamer.RelayLogInfo, safeTime time.Time) error {
	files, err := getRelayFilesBeforeFileAndTime(logger, relayBaseDir, uuids, safeRelay, safeTime)
	if err != nil {
		return terror.Annotatef(err, "get relay files from directory %s before file %+v and time %v with UUIDs %v", relayBaseDir, safeRelay, safeTime, uuids)
	}

	return purgeRelayFiles(logger, files)
}

// getRelayFilesBeforeFile gets a list of relay log files which are older than safeRelay.
func getRelayFilesBeforeFile(logger log.Logger, relayBaseDir string, uuids []string, safeRelay *streamer.RelayLogInfo) ([]*subRelayFiles, error) {
	// discard all newer UUIDs
	uuids, err := trimUUIDs(uuids, safeRelay)
	if err != nil {
		return nil, err
	}

	zeroTime := time.Unix(0, 0)
	files, err := collectRelayFilesBeforeFileAndTime(logger, relayBaseDir, uuids, safeRelay.Filename, zeroTime)
	return files, err
}

// getRelayFilesBeforeTime gets a list of relay log files which have modified time earlier than safeTime.
func getRelayFilesBeforeFileAndTime(logger log.Logger, relayBaseDir string, uuids []string, safeRelay *streamer.RelayLogInfo, safeTime time.Time) ([]*subRelayFiles, error) {
	// discard all newer UUIDs
	uuids, err := trimUUIDs(uuids, safeRelay)
	if err != nil {
		return nil, err
	}

	return collectRelayFilesBeforeFileAndTime(logger, relayBaseDir, uuids, safeRelay.Filename, safeTime)
}

// trimUUIDs trims all newer UUIDs than safeRelay.
func trimUUIDs(uuids []string, safeRelay *streamer.RelayLogInfo) ([]string, error) {
	endIdx := -1
	for i, uuid := range uuids {
		if uuid == safeRelay.UUID {
			endIdx = i
			break
		}
	}
	if endIdx < 0 {
		return nil, terror.ErrRelayTrimUUIDNotFound.Generate(safeRelay.UUID, uuids)
	}

	return uuids[:endIdx+1], nil
}

// collectRelayFilesBeforeFileAndTime collects relay log files before safeFilename (and before safeTime).
func collectRelayFilesBeforeFileAndTime(logger log.Logger, relayBaseDir string, uuids []string, safeFilename string, safeTime time.Time) ([]*subRelayFiles, error) {
	// NOTE: test performance when removing a large number of relay log files and decide whether need to limit files removed every time
	files := make([]*subRelayFiles, 0, 1)

	for i, uuid := range uuids {
		dir := filepath.Join(relayBaseDir, uuid)
		var (
			shortFiles []string
			err        error
			hasAll     bool
		)
		if i+1 == len(uuids) {
			// same sub dir, only collect relay files newer than safeRelay.filename
			shortFiles, err = CollectBinlogFilesCmp(dir, safeFilename, FileCmpLess)
			if err != nil {
				return nil, terror.Annotatef(err, "dir %s", dir)
			}
		} else {
			if !utils.IsDirExists(dir) {
				logger.Warn("relay log directory not exists", zap.String("directory", dir))
				continue
			}
			// earlier sub dir, collect all relay files
			shortFiles, err = CollectAllBinlogFiles(dir)
			if err != nil {
				return nil, terror.Annotatef(err, "dir %s", dir)
			}
			hasAll = true // collected all relay files
		}
		if len(shortFiles) == 0 {
			continue // no relay log files exist
		}
		fullFiles := make([]string, 0, len(shortFiles))
		for _, f := range shortFiles {
			fp := filepath.Join(dir, f)
			if safeTime.Unix() > 0 {
				// check modified time
				fs, err := os.Stat(fp)
				if err != nil {
					return nil, terror.ErrGetRelayLogStat.Delegate(err, fp)
				}
				if fs.ModTime().After(safeTime) {
					hasAll = false // newer found, reset to false
					logger.Debug("ignore newer relay log file in dir", zap.String("file", f), zap.String("directory", dir))
					break
				}
			}
			fullFiles = append(fullFiles, fp)
		}
		files = append(files, &subRelayFiles{
			dir:    dir,
			files:  fullFiles,
			hasAll: hasAll,
		})

		if !hasAll {
			// once newer file encountered, we think later files are newer too, so stop to collect
			break
		}
	}

	return files, nil
}

// purgeRelayFiles purges relay log files and directories if them become empty.
func purgeRelayFiles(logger log.Logger, files []*subRelayFiles) error {
	startTime := time.Now()
	defer func() {
		logger.Info("purge relay log files", zap.Duration("cost time", time.Since(startTime)))
	}()

	for _, subRelay := range files {
		for _, f := range subRelay.files {
			logger.Info("purging relay log file", zap.String("file", f))
			err := os.Remove(f)
			if err != nil {
				return terror.ErrRelayRemoveFileFail.Delegate(err, "file", f)
			}
		}
		if subRelay.hasAll {
			// if all relay log files removed, remove the directory and all other files (like relay.meta)
			logger.Info("purging relay log directory", zap.String("directory", subRelay.dir))
			err := os.RemoveAll(subRelay.dir)
			if err != nil {
				return terror.ErrRelayRemoveFileFail.Delegate(err, "dir", subRelay.dir)
			}
		}
	}
	return nil
}
