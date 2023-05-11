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
	"io"
<<<<<<< HEAD
=======
	"path"
	"regexp"
>>>>>>> 7f5309eaf2 (sink(ticdc): add meta separator index path in storage sink (#8948))
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
<<<<<<< HEAD
	minFileNamePrefixLen = 9
	defaultIndexFileName = "CDC.index"
)

// GenerateSchemaFilePath generates schema file path based on the table definition.
func GenerateSchemaFilePath(def TableDefinition) string {
	return fmt.Sprintf("%s/%s/%d/schema.json", def.Schema, def.Table, def.TableVersion)
=======
	minFileNamePrefixLen = 3 + config.MinFileIndexWidth
	defaultIndexFileName = "meta/CDC.index"

	// The following constants are used to generate file paths.
	schemaFileNameFormat = "schema_%d_%010d.json"
	// The database schema is stored in the following path:
	// <schema>/meta/schema_{tableVersion}_{checksum}.json
	dbSchemaPrefix = "%s/meta/"
	// The table schema is stored in the following path:
	// <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json
	tableSchemaPrefix = "%s/%s/meta/"
)

var schemaRE = regexp.MustCompile(`meta/schema_\d+_\d{10}\.json$`)

// IsSchemaFile checks whether the file is a schema file.
func IsSchemaFile(path string) bool {
	return schemaRE.MatchString(path)
}

// mustParseSchemaName parses the version from the schema file name.
func mustParseSchemaName(path string) (uint64, uint32) {
	reportErr := func(err error) {
		log.Panic("failed to parse schema file name",
			zap.String("schemaPath", path),
			zap.Any("error", err))
	}

	// For <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json, the parts
	// should be ["<schema>/<table>/meta/schema", "{tableVersion}", "{checksum}.json"].
	parts := strings.Split(path, "_")
	if len(parts) < 3 {
		reportErr(errors.New("invalid path format"))
	}

	checksum := strings.TrimSuffix(parts[len(parts)-1], ".json")
	tableChecksum, err := strconv.ParseUint(checksum, 10, 64)
	if err != nil {
		reportErr(err)
	}
	version := parts[len(parts)-2]
	tableVersion, err := strconv.ParseUint(version, 10, 64)
	if err != nil {
		reportErr(err)
	}
	return tableVersion, uint32(tableChecksum)
}

func generateSchemaFilePath(
	schema, table string, tableVersion uint64, checksum uint32,
) string {
	if schema == "" || tableVersion == 0 {
		log.Panic("invalid schema or tableVersion",
			zap.String("schema", schema), zap.Uint64("tableVersion", tableVersion))
	}

	var dir string
	if table == "" {
		// Generate db schema file path.
		dir = fmt.Sprintf(dbSchemaPrefix, schema)
	} else {
		// Generate table schema file path.
		dir = fmt.Sprintf(tableSchemaPrefix, schema, table)
	}
	name := fmt.Sprintf(schemaFileNameFormat, tableVersion, checksum)
	return path.Join(dir, name)
}

func generateDataFileName(index uint64, extension string, fileIndexWidth int) string {
	indexFmt := "%0" + strconv.Itoa(fileIndexWidth) + "d"
	return fmt.Sprintf("CDC"+indexFmt+"%s", index, extension)
>>>>>>> 7f5309eaf2 (sink(ticdc): add meta separator index path in storage sink (#8948))
}

type indexWithDate struct {
	index              uint64
	currDate, prevDate string
}

// VersionedTableName is used to wrap TableNameWithPhysicTableID with a version.
type VersionedTableName struct {
	// Because we need to generate different file paths for different
	// tables, we need to use the physical table ID instead of the
	// logical table ID.(Especially when the table is a partitioned table).
	TableNameWithPhysicTableID model.TableName
	// TableInfoVersion is consistent with the version of TableInfo recorded in
	// schema storage. It can either be finished ts of a DDL event,
	// or be the checkpoint ts when processor is restarted.
	TableInfoVersion uint64
}

// FilePathGenerator is used to generate data file path and index file path.
type FilePathGenerator struct {
	extension string
	config    *Config
	clock     clock.Clock
	storage   storage.ExternalStorage
	fileIndex map[VersionedTableName]*indexWithDate
}

// NewFilePathGenerator creates a FilePathGenerator.
func NewFilePathGenerator(
	config *Config,
	storage storage.ExternalStorage,
	extension string,
	clock clock.Clock,
) *FilePathGenerator {
	return &FilePathGenerator{
		config:    config,
		extension: extension,
		storage:   storage,
		clock:     clock,
		fileIndex: make(map[VersionedTableName]*indexWithDate),
	}
}

// SetClock is used for unit test
func (f *FilePathGenerator) SetClock(clock clock.Clock) {
	f.clock = clock
}

// Contains checks if a VersionedTable is cached by FilePathGenerator before.
func (f *FilePathGenerator) Contains(tbl VersionedTableName) bool {
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

<<<<<<< HEAD
=======
// GenerateIndexFilePath generates a canonical path for index file.
func (f *FilePathGenerator) GenerateIndexFilePath(tbl VersionedTableName, date string) string {
	dir := f.generateDataDirPath(tbl, date)
	name := defaultIndexFileName
	return path.Join(dir, name)
}

// GenerateDataFilePath generates a canonical path for data file.
func (f *FilePathGenerator) GenerateDataFilePath(
	ctx context.Context, tbl VersionedTableName, date string,
) (string, error) {
	dir := f.generateDataDirPath(tbl, date)
	name, err := f.generateDataFileName(ctx, tbl, date)
	if err != nil {
		return "", err
	}
	return path.Join(dir, name), nil
}

>>>>>>> 7f5309eaf2 (sink(ticdc): add meta separator index path in storage sink (#8948))
func (f *FilePathGenerator) generateDataDirPath(tbl VersionedTableName, date string) string {
	var elems []string

	elems = append(elems, tbl.TableNameWithPhysicTableID.Schema)
	elems = append(elems, tbl.TableNameWithPhysicTableID.Table)
	elems = append(elems, fmt.Sprintf("%d", tbl.TableInfoVersion))

	if f.config.EnablePartitionSeparator && tbl.TableNameWithPhysicTableID.IsPartition {
		elems = append(elems, fmt.Sprintf("%d", tbl.TableNameWithPhysicTableID.TableID))
	}

	if len(date) != 0 {
		elems = append(elems, date)
	}

	return path.Join(elems...)
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
	tbl VersionedTableName,
	date string,
) (string, error) {
	var elems []string
	elems = append(elems, f.generateDataDirPath(tbl, date))
	if idx, ok := f.fileIndex[tbl]; !ok {
		fileIdx, err := f.getNextFileIdxFromIndexFile(ctx, tbl, date)
		if err != nil {
			return "", err
		}
		f.fileIndex[tbl] = &indexWithDate{
			prevDate: date,
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
func (f *FilePathGenerator) GenerateIndexFilePath(tbl VersionedTableName, date string) string {
	var elems []string

	elems = append(elems, f.generateDataDirPath(tbl, date))
	elems = append(elems, defaultIndexFileName)

	return strings.Join(elems, "/")
}

func (f *FilePathGenerator) getNextFileIdxFromIndexFile(
	ctx context.Context, tbl VersionedTableName, date string,
) (uint64, error) {
	indexFile := f.GenerateIndexFilePath(tbl, date)
	exist, err := f.storage.FileExists(ctx, indexFile)
	if err != nil {
		return 0, err
	}
	if !exist {
		return 0, nil
	}

	data, err := f.storage.ReadFile(ctx, indexFile)
	if err != nil {
		return 0, err
	}
	fileName := strings.TrimSuffix(string(data), "\n")
	maxFileIdx, err := f.fetchIndexFromFileName(fileName)
	if err != nil {
		return 0, err
	}

<<<<<<< HEAD
	lastFilePath := strings.Join([]string{
		f.generateDataDirPath(tbl, date),                  // file dir
		fmt.Sprintf("CDC%06d%s", maxFileIdx, f.extension), // file name
	}, "/")

=======
	lastFilePath := path.Join(
		f.generateDataDirPath(tbl, date),                                       // file dir
		generateDataFileName(maxFileIdx, f.extension, f.config.FileIndexWidth), // file name
	)
>>>>>>> 7f5309eaf2 (sink(ticdc): add meta separator index path in storage sink (#8948))
	var lastFileExists, lastFileIsEmpty bool
	lastFileExists, err = f.storage.FileExists(ctx, lastFilePath)
	if err != nil {
		return 0, err
	}

	if lastFileExists {
		fileReader, err := f.storage.Open(ctx, lastFilePath)
		if err != nil {
			return 0, err
		}
		readBytes, err := fileReader.Read(make([]byte, 1))
		if err != nil && err != io.EOF {
			return 0, err
		}
		lastFileIsEmpty = readBytes == 0
		if err := fileReader.Close(); err != nil {
			return 0, err
		}
	}

	var fileIdx uint64
	if lastFileExists && !lastFileIsEmpty {
		fileIdx = maxFileIdx
	} else {
		// Reuse the old index number if the last file does not exist.
		fileIdx = maxFileIdx - 1
	}
	return fileIdx, nil
}
