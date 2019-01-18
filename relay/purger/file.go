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
	"os"
	"path/filepath"
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/pkg/streamer"
	"github.com/pingcap/dm/pkg/utils"
)

// subRelayFiles represents relay log files in one sub directory
type subRelayFiles struct {
	dir    string   // sub directory path
	files  []string // path of relay log files
	hasAll bool     // whether all relay log files in @dir are included in @files
}

// purgeRelayFilesBeforeFile purge relay log files which are older than safeRelay
func purgeRelayFilesBeforeFile(relayBaseDir string, uuids []string, safeRelay *streamer.RelayLogInfo) error {
	files, err := getRelayFilesBeforeFile(relayBaseDir, uuids, safeRelay)
	if err != nil {
		return errors.Annotatef(err, "get relay files from directory %s before file %+v with UUIDs %v", relayBaseDir, safeRelay, uuids)
	}

	return errors.Trace(purgeRelayFiles(files))
}

// purgeRelayFilesBeforeFileAndTime purge relay log files which are older than safeRelay and safeTime
func purgeRelayFilesBeforeFileAndTime(relayBaseDir string, uuids []string, safeRelay *streamer.RelayLogInfo, safeTime time.Time) error {
	files, err := getRelayFilesBeforeFileAndTime(relayBaseDir, uuids, safeRelay, safeTime)
	if err != nil {
		return errors.Annotatef(err, "get relay files from directory %s before file %+v and time %v with UUIDs %v", relayBaseDir, safeRelay, safeTime, uuids)
	}

	return errors.Trace(purgeRelayFiles(files))
}

// getRelayFilesBeforeFile gets a list of relay log files which are older than safeRelay
func getRelayFilesBeforeFile(relayBaseDir string, uuids []string, safeRelay *streamer.RelayLogInfo) ([]*subRelayFiles, error) {
	// discard all newer UUIDs
	uuids, err := trimUUIDs(uuids, safeRelay)
	if err != nil {
		return nil, errors.Trace(err)
	}

	zeroTime := time.Unix(0, 0)
	files, err := collectRelayFilesBeforeFileAndTime(relayBaseDir, uuids, safeRelay.Filename, zeroTime)
	return files, errors.Trace(err)
}

// getRelayFilesBeforeTime gets a list of relay log files which have modified time earlier than safeTime
func getRelayFilesBeforeFileAndTime(relayBaseDir string, uuids []string, safeRelay *streamer.RelayLogInfo, safeTime time.Time) ([]*subRelayFiles, error) {
	// discard all newer UUIDs
	uuids, err := trimUUIDs(uuids, safeRelay)
	if err != nil {
		return nil, errors.Trace(err)
	}

	files, err := collectRelayFilesBeforeFileAndTime(relayBaseDir, uuids, safeRelay.Filename, safeTime)
	return files, errors.Trace(err)
}

// trimUUIDs trims all newer UUIDs than safeRelay
func trimUUIDs(uuids []string, safeRelay *streamer.RelayLogInfo) ([]string, error) {
	var endIdx = -1
	for i, uuid := range uuids {
		if uuid == safeRelay.UUID {
			endIdx = i
			break
		}
	}
	if endIdx < 0 {
		return nil, errors.NotFoundf("UUID %s in UUIDs %v", safeRelay.UUID, uuids)
	}

	return uuids[:endIdx+1], nil
}

// collectRelayFilesBeforeFileAndTime collects relay log files before safeFilename (and before safeTime)
func collectRelayFilesBeforeFileAndTime(relayBaseDir string, uuids []string, safeFilename string, safeTime time.Time) ([]*subRelayFiles, error) {
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
			shortFiles, err = streamer.CollectBinlogFilesCmp(dir, safeFilename, streamer.FileCmpLess)
			if err != nil {
				return nil, errors.Annotatef(err, "dir %s", dir)
			}
		} else {
			if !utils.IsDirExists(dir) {
				log.Warnf("[purger] relay log directory %s not exists", dir)
				continue
			}
			// earlier sub dir, collect all relay files
			shortFiles, err = streamer.CollectAllBinlogFiles(dir)
			if err != nil {
				return nil, errors.Annotatef(err, "dir %s", dir)
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
					return nil, errors.Annotatef(err, "get stat for relay log file %s", fp)
				}
				if fs.ModTime().After(safeTime) {
					hasAll = false // newer found, reset to false
					log.Debugf("[purger] ignore newer relay log file %s in dir %s", f, dir)
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

// purgeRelayFiles purges relay log files and directories if them become empty
func purgeRelayFiles(files []*subRelayFiles) error {
	startTime := time.Now()
	defer func() {
		log.Infof("[purger] purge relay log files takes %f seconds", time.Since(startTime).Seconds())
	}()

	for _, subRelay := range files {
		for _, f := range subRelay.files {
			log.Infof("[purger] purging relay log file %s", f)
			err := os.Remove(f)
			if err != nil {
				return errors.Annotatef(err, "relay log file %s", f)
			}
		}
		if subRelay.hasAll {
			// if all relay log files removed, remove the directory and all other files (like relay.meta)
			log.Infof("[purger] purging relay log directory %s", subRelay.dir)
			err := os.RemoveAll(subRelay.dir)
			if err != nil {
				return errors.Annotatef(err, "relay log dir %s", subRelay.dir)
			}
		}
	}
	return nil
}
