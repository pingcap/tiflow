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
	"sort"

	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/binlog"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/terror"
	"github.com/pingcap/ticdc/dm/pkg/utils"
)

// FileCmp is a compare condition used when collecting binlog files.
type FileCmp uint8

// FileCmpLess represents a < FileCmp condition, others are similar.
const (
	FileCmpLess FileCmp = iota + 1
	FileCmpLessEqual
	FileCmpEqual
	FileCmpBiggerEqual
	FileCmpBigger
)

// EventNotifier notifies whether there is new binlog event written to the file.
type EventNotifier interface {
	// Notified returns a channel used to check whether there is new binlog event written to the file
	Notified() chan interface{}
}

// CollectAllBinlogFiles collects all valid binlog files in dir, and returns filenames in binlog ascending order.
func CollectAllBinlogFiles(dir string) ([]string, error) {
	if dir == "" {
		return nil, terror.ErrEmptyRelayDir.Generate()
	}
	return readSortedBinlogFromDir(dir)
}

// CollectBinlogFilesCmp collects valid binlog files with a compare condition.
func CollectBinlogFilesCmp(dir, baseFile string, cmp FileCmp) ([]string, error) {
	if dir == "" {
		return nil, terror.ErrEmptyRelayDir.Generate()
	}

	if bp := filepath.Join(dir, baseFile); !utils.IsFileExists(bp) {
		return nil, terror.ErrBaseFileNotFound.Generate(baseFile, dir)
	}

	bf, err := binlog.ParseFilename(baseFile)
	if err != nil {
		return nil, terror.Annotatef(err, "filename %s", baseFile)
	}

	allFiles, err := CollectAllBinlogFiles(dir)
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(allFiles))
	for _, f := range allFiles {
		// we have parse f in `CollectAllBinlogFiles`, may be we can refine this
		parsed, err := binlog.ParseFilename(f)
		if err != nil || parsed.BaseName != bf.BaseName {
			log.L().Warn("collecting binlog file, ignore invalid file", zap.String("file", f), log.ShortError(err))
			continue
		}
		switch cmp {
		case FileCmpBigger:
			if !parsed.GreaterThan(bf) {
				log.L().Debug("ignore older or equal binlog file", zap.String("file", f), zap.String("directory", dir))
				continue
			}
		case FileCmpBiggerEqual:
			if !parsed.GreaterThanOrEqualTo(bf) {
				log.L().Debug("ignore older binlog file", zap.String("file", f), zap.String("directory", dir))
				continue
			}
		case FileCmpLess:
			if !parsed.LessThan(bf) {
				log.L().Debug("ignore newer or equal binlog file", zap.String("file", f), zap.String("directory", dir))
				continue
			}
		default:
			return nil, terror.ErrBinFileCmpCondNotSupport.Generate(cmp)
		}

		results = append(results, f)
	}

	return results, nil
}

// getFirstBinlogName gets the first binlog file in relay sub directory.
func getFirstBinlogName(baseDir, uuid string) (string, error) {
	subDir := filepath.Join(baseDir, uuid)
	files, err := readSortedBinlogFromDir(subDir)
	if err != nil {
		return "", terror.Annotatef(err, "get binlog file for dir %s", subDir)
	}

	if len(files) == 0 {
		return "", terror.ErrBinlogFilesNotFound.Generate(subDir)
	}
	return files[0], nil
}

// readSortedBinlogFromDir reads and returns all binlog files (sorted ascending by binlog filename and sequence number).
func readSortedBinlogFromDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, terror.ErrReadDir.Delegate(err, dirpath)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, terror.ErrReadDir.Delegate(err, dirpath)
	}
	if len(names) == 0 {
		return nil, nil
	}

	// sorting bin.100000, ..., bin.1000000, ..., bin.999999
	type tuple struct {
		filename string
		parsed   binlog.Filename
	}
	tmp := make([]tuple, 0, len(names)-1)

	for _, f := range names {
		p, err2 := binlog.ParseFilename(f)
		if err2 != nil {
			// may contain some file that can't be parsed, like relay meta. ignore them
			log.L().Info("collecting binlog file, ignore invalid file", zap.String("file", f))
			continue
		}
		tmp = append(tmp, tuple{
			filename: f,
			parsed:   p,
		})
	}

	sort.Slice(tmp, func(i, j int) bool {
		if tmp[i].parsed.BaseName != tmp[j].parsed.BaseName {
			return tmp[i].parsed.BaseName < tmp[j].parsed.BaseName
		}
		return tmp[i].parsed.LessThan(tmp[j].parsed)
	})

	ret := make([]string, len(tmp))
	for i := range tmp {
		ret[i] = tmp[i].filename
	}

	return ret, nil
}

// fileSizeUpdated checks whether the file's size has updated
// return
//   0: not updated
//   1: update to larger
//  -1: update to smaller, only happens in special case, for example we change
//      relay.meta manually and start task before relay log catches up.
func fileSizeUpdated(path string, latestSize int64) (int, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, terror.ErrGetRelayLogStat.Delegate(err, path)
	}
	curSize := fi.Size()
	switch {
	case curSize == latestSize:
		return 0, nil
	case curSize > latestSize:
		log.L().Debug("size of relay log file has been changed", zap.String("file", path),
			zap.Int64("old size", latestSize), zap.Int64("size", curSize))
		return 1, nil
	default:
		log.L().Error("size of relay log file has been changed", zap.String("file", path),
			zap.Int64("old size", latestSize), zap.Int64("size", curSize))
		return -1, nil
	}
}
