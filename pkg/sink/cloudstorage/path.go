// Copyright 2023 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// 3 is the length of "CDC", and the file number contains
	// at least 6 digits (e.g. CDC000001.csv).
	minFileNamePrefixLen = 9
	defaultIndexFileName = "CDC.index"
)

// GenerateSchemaFilePath generates schema file path based on the table definition.
func GenerateSchemaFilePath(def TableDefinition) string {
	return fmt.Sprintf("%s/%s/%d/schema.json", def.Schema, def.Table, def.TableVersion)
}

type indexWithDate struct {
	index              uint64
	currDate, prevDate string
}

// VersionedTable is used to wrap TableName with a version.
type VersionedTable struct {
	model.TableName
	Version uint64
}

// FilePathGenerator is used to generate data file path and index file path.
type FilePathGenerator struct {
	extension string
	config    *Config
	clock     clock.Clock
	storage   storage.ExternalStorage
	fileIndex map[VersionedTable]*indexWithDate
}

// NewFilePathGenerator creates a FilePathGenerator.
func NewFilePathGenerator(
	config *Config,
	storage storage.ExternalStorage,
	extension string,
) *FilePathGenerator {
	return &FilePathGenerator{
		config:    config,
		extension: extension,
		storage:   storage,
		clock:     clock.New(),
		fileIndex: make(map[VersionedTable]*indexWithDate),
	}
}

// Contains checks if a VersionedTable is cached by FilePathGenerator before.
func (f *FilePathGenerator) Contains(tbl VersionedTable) bool {
	_, ok := f.fileIndex[tbl]
	return ok
}

// GenerateDateStr generates a date string base on current time
// and the date-separator configuration item.
func (f *FilePathGenerator) GenerateDateStr() string {
	var dateStr string

	currTime := f.clock.Now()
	switch f.config.DateSeparator {
	case config.DateSeparatorYear.String():
		dateStr = currTime.Format("2006")
	case config.DateSeparatorMonth.String():
		dateStr = currTime.Format("2006-01")
	case config.DateSeparatorDay.String():
		dateStr = currTime.Format("2006-01-02")
	default:
	}

	return dateStr
}

func (f *FilePathGenerator) generateDataDirPath(tbl VersionedTable, date string) string {
	var elems []string

	elems = append(elems, tbl.Schema)
	elems = append(elems, tbl.Table)
	elems = append(elems, fmt.Sprintf("%d", tbl.Version))

	if f.config.EnablePartitionSeparator && tbl.TableName.IsPartition {
		elems = append(elems, fmt.Sprintf("%d", tbl.TableID))
	}

	if len(date) != 0 {
		elems = append(elems, date)
	}

	return strings.Join(elems, "/")
}

func (f *FilePathGenerator) fetchIndexFromFileName(fileName string) (uint64, error) {
	var fileIdx uint64
	var err error

	if len(fileName) < minFileNamePrefixLen+len(f.extension) ||
		!strings.HasPrefix(fileName, "CDC") ||
		!strings.HasSuffix(fileName, f.extension) {
		return 0, cerror.WrapError(cerror.ErrStorageSinkInvalidFileName,
			fmt.Errorf("'%s' is a invalid file name", fileName))
	}

	extIdx := strings.Index(fileName, f.extension)
	fileIdxStr := fileName[3:extIdx]
	if fileIdx, err = strconv.ParseUint(fileIdxStr, 10, 64); err != nil {
		return 0, cerror.WrapError(cerror.ErrStorageSinkInvalidFileName, err)
	}

	return fileIdx, nil
}

// GenerateDataFilePath generates a canonical path for data file.
func (f *FilePathGenerator) GenerateDataFilePath(
	ctx context.Context,
	tbl VersionedTable,
	date string,
) (string, error) {
	var elems []string

	elems = append(elems, f.generateDataDirPath(tbl, date))
	if idx, ok := f.fileIndex[tbl]; !ok {
		var fileIdx uint64

		indexFile := f.GenerateIndexFilePath(tbl, date)
		exist, err := f.storage.FileExists(ctx, indexFile)
		if err != nil {
			return "", err
		}
		if exist {
			data, err := f.storage.ReadFile(ctx, indexFile)
			if err != nil {
				return "", err
			}
			fileName := strings.TrimSuffix(string(data), "\n")
			maxFileIdx, err := f.fetchIndexFromFileName(fileName)
			if err != nil {
				return "", err
			}
			fileIdx = maxFileIdx
		}

		f.fileIndex[tbl] = &indexWithDate{
			currDate: date,
			index:    fileIdx,
		}
	} else {
		idx.currDate = date
	}

	// if date changed, reset the counter
	if f.fileIndex[tbl].prevDate != f.fileIndex[tbl].currDate {
		f.fileIndex[tbl].prevDate = f.fileIndex[tbl].currDate
		f.fileIndex[tbl].index = 0
	}
	f.fileIndex[tbl].index++
	elems = append(elems, fmt.Sprintf("CDC%06d%s", f.fileIndex[tbl].index, f.extension))

	return strings.Join(elems, "/"), nil
}

// GenerateIndexFilePath generates a canonical path for index file.
func (f *FilePathGenerator) GenerateIndexFilePath(tbl VersionedTable, date string) string {
	var elems []string

	elems = append(elems, f.generateDataDirPath(tbl, date))
	elems = append(elems, defaultIndexFileName)

	return strings.Join(elems, "/")
}
