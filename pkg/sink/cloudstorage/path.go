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
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	// 3 is the length of "CDC", and the file number contains
	// at least 6 digits (e.g. CDC000001.csv).
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

	hasher     *hash.PositionInertia
	versionMap map[VersionedTableName]uint64
}

// NewFilePathGenerator creates a FilePathGenerator.
func NewFilePathGenerator(
	config *Config,
	storage storage.ExternalStorage,
	extension string,
	clock clock.Clock,
) *FilePathGenerator {
	return &FilePathGenerator{
		config:     config,
		extension:  extension,
		storage:    storage,
		clock:      clock,
		fileIndex:  make(map[VersionedTableName]*indexWithDate),
		hasher:     hash.NewPositionInertia(),
		versionMap: make(map[VersionedTableName]uint64),
	}
}

// CheckOrWriteSchema checks whether the schema file exists in the storage and
// write scheme.json if necessary.
func (f *FilePathGenerator) CheckOrWriteSchema(
	ctx context.Context,
	table VersionedTableName,
	tableInfo *model.TableInfo,
) error {
	if _, ok := f.versionMap[table]; ok {
		return nil
	}

	var def TableDefinition
	def.FromTableInfo(tableInfo, table.TableInfoVersion, f.config.OutputColumnID)
	if !def.IsTableSchema() {
		// only check schema for table
		log.Panic("invalid table schema", zap.Any("versionedTableName", table),
			zap.Any("tableInfo", tableInfo))
	}

	// Case 1: point check if the schema file exists.
	tblSchemaFile, err := def.GenerateSchemaFilePath()
	if err != nil {
		return err
	}
	exist, err := f.storage.FileExists(ctx, tblSchemaFile)
	if err != nil {
		return err
	}
	if exist {
		f.versionMap[table] = table.TableInfoVersion
		return nil
	}

	// walk the table meta path to find the last schema file
	_, checksum := mustParseSchemaName(tblSchemaFile)
	schemaFileCnt := 0
	lastVersion := uint64(0)
	subDir := fmt.Sprintf(tableSchemaPrefix, def.Schema, def.Table)
	checksumSuffix := fmt.Sprintf("%010d.json", checksum)
	err = f.storage.WalkDir(ctx, &storage.WalkOption{
		SubDir:    subDir, /* use subDir to prevent walk the whole storage */
		ObjPrefix: subDir + "schema_",
	}, func(path string, _ int64) error {
		schemaFileCnt++
		if !strings.HasSuffix(path, checksumSuffix) {
			return nil
		}
		version, parsedChecksum := mustParseSchemaName(path)
		if parsedChecksum != checksum {
			// TODO: parsedChecksum should be ignored, remove this panic
			// after the new path protocol is verified.
			log.Panic("invalid schema file name",
				zap.String("path", path), zap.Any("checksum", checksum))
		}
		if version > lastVersion {
			lastVersion = version
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Case 2: the table meta path is not empty.
	if schemaFileCnt != 0 && lastVersion != 0 {
		f.versionMap[table] = lastVersion
		return nil
	}

	// Case 3: the table meta path is empty, which happens when:
	//  a. the table is existed before changefeed started. We need to write schema file to external storage.
	//  b. the schema file is deleted by the consumer. We write schema file to external storage too.
	if schemaFileCnt != 0 && lastVersion == 0 {
		log.Warn("no table schema file found in an non-empty meta path",
			zap.Any("versionedTableName", table),
			zap.Uint32("checksum", checksum))
	}
	encodedDetail, err := def.MarshalWithQuery()
	if err != nil {
		return err
	}
	f.versionMap[table] = table.TableInfoVersion
	return f.storage.WriteFile(ctx, tblSchemaFile, encodedDetail)
}

// SetClock is used for unit test
func (f *FilePathGenerator) SetClock(clock clock.Clock) {
	f.clock = clock
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

func (f *FilePathGenerator) generateDataDirPath(tbl VersionedTableName, date string) string {
	var elems []string

	elems = append(elems, tbl.TableNameWithPhysicTableID.Schema)
	elems = append(elems, tbl.TableNameWithPhysicTableID.Table)
	elems = append(elems, fmt.Sprintf("%d", f.versionMap[tbl]))

	if f.config.EnablePartitionSeparator && tbl.TableNameWithPhysicTableID.IsPartition {
		elems = append(elems, fmt.Sprintf("%d", tbl.TableNameWithPhysicTableID.TableID))
	}

	if len(date) != 0 {
		elems = append(elems, date)
	}

	return path.Join(elems...)
}

func (f *FilePathGenerator) generateDataFileName(
	ctx context.Context, tbl VersionedTableName, date string,
) (string, error) {
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
	return generateDataFileName(f.fileIndex[tbl].index, f.extension, f.config.FileIndexWidth), nil
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

	lastFilePath := path.Join(
		f.generateDataDirPath(tbl, date),                                       // file dir
		generateDataFileName(maxFileIdx, f.extension, f.config.FileIndexWidth), // file name
	)
	var lastFileExists, lastFileIsEmpty bool
	lastFileExists, err = f.storage.FileExists(ctx, lastFilePath)
	if err != nil {
		return 0, err
	}

	if lastFileExists {
		fileReader, err := f.storage.Open(ctx, lastFilePath, nil)
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

func (f *FilePathGenerator) fetchIndexFromFileName(fileName string) (uint64, error) {
	var fileIdx uint64
	var err error

	if len(fileName) < minFileNamePrefixLen+len(f.extension) ||
		!strings.HasPrefix(fileName, "CDC") ||
		!strings.HasSuffix(fileName, f.extension) {
		return 0, errors.WrapError(errors.ErrStorageSinkInvalidFileName,
			fmt.Errorf("'%s' is a invalid file name", fileName))
	}

	extIdx := strings.Index(fileName, f.extension)
	fileIdxStr := fileName[3:extIdx]
	if fileIdx, err = strconv.ParseUint(fileIdxStr, 10, 64); err != nil {
		return 0, errors.WrapError(errors.ErrStorageSinkInvalidFileName, err)
	}

	return fileIdx, nil
}

var dateSeparatorDayRegexp *regexp.Regexp

// RemoveExpiredFiles removes expired files from external storage.
func RemoveExpiredFiles(
	ctx context.Context,
	_ model.ChangeFeedID,
	storage storage.ExternalStorage,
	cfg *Config,
	checkpointTs model.Ts,
) (uint64, error) {
	if cfg.DateSeparator != config.DateSeparatorDay.String() {
		return 0, nil
	}
	if dateSeparatorDayRegexp == nil {
		dateSeparatorDayRegexp = regexp.MustCompile(config.DateSeparatorDay.GetPattern())
	}

	ttl := time.Duration(cfg.FileExpirationDays) * time.Hour * 24
	currTime := oracle.GetTimeFromTS(checkpointTs).Add(-ttl)
	expiredDate := currTime.Format("2006-01-02")

	cnt := uint64(0)
	err := util.RemoveFilesIf(ctx, storage, func(path string) bool {
		// the path is like: <schema>/<table>/<tableVersion>/<partitionID>/<date>/CDC{num}.extension
		match := dateSeparatorDayRegexp.FindString(path)
		if match != "" && match < expiredDate {
			cnt++
			return true
		}
		return false
	}, nil)
	return cnt, err
}

// RemoveEmptyDirs removes empty directories from external storage.
func RemoveEmptyDirs(
	ctx context.Context,
	id model.ChangeFeedID,
	target string,
) (uint64, error) {
	cnt := uint64(0)
	err := filepath.Walk(target, func(path string, info fs.FileInfo, err error) error {
		if os.IsNotExist(err) || path == target || info == nil {
			// if path not exists, we should return nil to continue.
			return nil
		}
		if err != nil {
			return err
		}
		if info.IsDir() {
			files, err := os.ReadDir(path)
			if err == nil && len(files) == 0 {
				log.Debug("Deleting empty directory",
					zap.String("namespace", id.Namespace),
					zap.String("changeFeedID", id.ID),
					zap.String("path", path))
				os.Remove(path)
				cnt++
				return filepath.SkipDir
			}
		}
		return nil
	})

	return cnt, err
}
