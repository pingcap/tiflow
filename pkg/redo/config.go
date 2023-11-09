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

package redo

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
)

var (
	// DefaultGCIntervalInMs defines GC interval in meta manager, which can be changed in tests.
	DefaultGCIntervalInMs = 5000 // 5 seconds
	// DefaultMaxLogSize is the default max size of log file
	DefaultMaxLogSize = int64(64)
)

const (
	// DefaultTimeout is the default timeout for writing external storage.
	DefaultTimeout = 15 * time.Minute
	// CloseTimeout is the default timeout for close redo writer.
	CloseTimeout = 15 * time.Second

	// FlushWarnDuration is the warning duration for flushing external storage.
	FlushWarnDuration = time.Second * 20
	// DefaultFlushIntervalInMs is the default flush interval for redo log.
	DefaultFlushIntervalInMs = 2000
	// DefaultMetaFlushIntervalInMs is the default flush interval for redo meta.
	DefaultMetaFlushIntervalInMs = 200
	// MinFlushIntervalInMs is the minimum flush interval for redo log.
	MinFlushIntervalInMs = 50

	// DefaultEncodingWorkerNum is the default number of encoding workers.
	DefaultEncodingWorkerNum = 16
	// DefaultEncodingInputChanSize is the default size of input channel for encoding worker.
	DefaultEncodingInputChanSize = 128
	// DefaultEncodingOutputChanSize is the default size of output channel for encoding worker.
	DefaultEncodingOutputChanSize = 2048
	// DefaultFlushWorkerNum is the default number of flush workers.
	// Maximum allocated memory is flushWorkerNum*maxLogSize, which is
	// `8*64MB = 512MB` by default.
	DefaultFlushWorkerNum = 8

	// DefaultFileMode is the default mode when operation files
	DefaultFileMode = 0o644
	// DefaultDirMode is the default mode when operation dir
	DefaultDirMode = 0o755

	// TmpEXT is the file ext of log file before safely wrote to disk
	TmpEXT = ".tmp"
	// LogEXT is the file ext of log file after safely wrote to disk
	LogEXT = ".log"
	// MetaEXT is the meta file ext of meta file after safely wrote to disk
	MetaEXT = ".meta"
	// SortLogEXT is the sorted log file ext of log file after safely wrote to disk
	SortLogEXT = ".sort"

	// MinSectorSize is minimum sector size used when flushing log so that log can safely
	// distinguish between torn writes and ordinary data corruption.
	MinSectorSize = 512
	// PageBytes is the alignment for flushing records to the backing Writer.
	// It should be a multiple of the minimum sector size so that log can safely
	// distinguish between torn writes and ordinary data corruption.
	PageBytes = 8 * MinSectorSize
	// Megabyte is the size of 1MB
	Megabyte int64 = 1024 * 1024
)

const (
	// RedoMetaFileType is the default file type of meta file
	RedoMetaFileType = "meta"
	// RedoRowLogFileType is the default file type of row log file
	RedoRowLogFileType = "row"
	// RedoDDLLogFileType is the default file type of ddl log file
	RedoDDLLogFileType = "ddl"
)

// ConsistentLevelType is the level of redo log consistent level.
type ConsistentLevelType string

const (
	// ConsistentLevelNone no consistent guarantee.
	ConsistentLevelNone ConsistentLevelType = "none"
	// ConsistentLevelEventual eventual consistent.
	ConsistentLevelEventual ConsistentLevelType = "eventual"
)

// IsValidConsistentLevel checks whether a given consistent level is valid
func IsValidConsistentLevel(level string) bool {
	switch ConsistentLevelType(level) {
	case ConsistentLevelNone, ConsistentLevelEventual:
		return true
	default:
		return false
	}
}

// IsConsistentEnabled returns whether the consistent feature is enabled.
func IsConsistentEnabled(level string) bool {
	return IsValidConsistentLevel(level) && ConsistentLevelType(level) != ConsistentLevelNone
}

// ConsistentStorage is the type of consistent storage.
type ConsistentStorage string

const (
	// consistentStorageBlackhole is a blackhole storage, which will discard all data.
	consistentStorageBlackhole ConsistentStorage = "blackhole"
	// consistentStorageLocal is a local storage, which will store data in local disk.
	consistentStorageLocal ConsistentStorage = "local"
	// consistentStorageNFS is a NFS storage, which will store data in NFS.
	consistentStorageNFS ConsistentStorage = "nfs"

	// consistentStorageS3 is a S3 storage, which will store data in S3.
	consistentStorageS3 ConsistentStorage = "s3"
	// consistentStorageGCS is a GCS storage, which will store data in GCS.
	consistentStorageGCS ConsistentStorage = "gcs"
	// consistentStorageGS is an alias of GCS storage.
	consistentStorageGS ConsistentStorage = "gs"
	// consistentStorageAzblob is a Azure Blob storage, which will store data in Azure Blob.
	consistentStorageAzblob ConsistentStorage = "azblob"
	// consistentStorageAzure is an alias of Azure Blob storage.
	consistentStorageAzure ConsistentStorage = "azure"
	// consistentStorageFile is an external storage based on local files and
	// will only be used for testing.
	consistentStorageFile ConsistentStorage = "file"
	// consistentStorageNoop is a noop storage, which simply discard all data.
	consistentStorageNoop ConsistentStorage = "noop"
)

// IsValidConsistentStorage checks whether a give consistent storage is valid.
func IsValidConsistentStorage(scheme string) bool {
	return IsBlackholeStorage(scheme) ||
		IsLocalStorage(scheme) ||
		IsExternalStorage(scheme)
}

// IsExternalStorage returns whether an external storage is used.
func IsExternalStorage(scheme string) bool {
	switch ConsistentStorage(scheme) {
	case consistentStorageS3, consistentStorageGCS, consistentStorageGS,
		consistentStorageAzblob, consistentStorageAzure, consistentStorageFile,
		consistentStorageNoop:
		return true
	default:
		return false
	}
}

// IsLocalStorage returns whether a local storage is used.
func IsLocalStorage(scheme string) bool {
	switch ConsistentStorage(scheme) {
	case consistentStorageLocal, consistentStorageNFS:
		return true
	default:
		return false
	}
}

// FixLocalScheme convert local scheme to externally compatible scheme.
func FixLocalScheme(uri *url.URL) {
	if IsLocalStorage(uri.Scheme) {
		uri.Scheme = string(consistentStorageFile)
	}
}

// IsBlackholeStorage returns whether a blackhole storage is used.
func IsBlackholeStorage(scheme string) bool {
	return strings.HasPrefix(scheme, string(consistentStorageBlackhole))
}

// InitExternalStorage init an external storage.
var InitExternalStorage = func(ctx context.Context, uri url.URL) (storage.ExternalStorage, error) {
	s, err := util.GetExternalStorageWithTimeout(ctx, uri.String(), DefaultTimeout)
	if err != nil {
		return nil, errors.WrapChangefeedUnretryableErr(errors.ErrStorageInitialize, err)
	}
	return s, nil
}

func initExternalStorageForTest(ctx context.Context, uri url.URL) (storage.ExternalStorage, error) {
	if ConsistentStorage(uri.Scheme) == consistentStorageS3 && len(uri.Host) == 0 {
		// TODO: this branch is compatible with previous s3 logic and will be removed
		// in the future.
		return nil, errors.WrapChangefeedUnretryableErr(errors.ErrStorageInitialize,
			errors.Errorf("please specify the bucket for %+v", uri))
	}
	s, err := util.GetExternalStorageFromURI(ctx, uri.String())
	if err != nil {
		return nil, errors.WrapChangefeedUnretryableErr(errors.ErrStorageInitialize, err)
	}
	return s, nil
}

// ValidateStorage validates the storage used by redo.
func ValidateStorage(uri *url.URL) error {
	scheme := uri.Scheme
	if !IsValidConsistentStorage(scheme) {
		return errors.ErrConsistentStorage.GenWithStackByArgs(scheme)
	}
	if IsBlackholeStorage(scheme) {
		return nil
	}

	if IsExternalStorage(scheme) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := initExternalStorageForTest(ctx, *uri)
		return err
	}

	err := os.MkdirAll(uri.Path, DefaultDirMode)
	if err != nil {
		return errors.WrapError(errors.ErrStorageInitialize, errors.Annotate(err,
			fmt.Sprintf("can't make dir for new redo log: %+v", uri)))
	}

	file := filepath.Join(uri.Path, "file.test")
	if err := os.WriteFile(file, []byte(""), DefaultFileMode); err != nil {
		return errors.WrapError(errors.ErrStorageInitialize, errors.Annotate(err,
			fmt.Sprintf("can't write file for new redo log: %+v", uri)))
	}

	if _, err := os.ReadFile(file); err != nil {
		return errors.WrapError(errors.ErrStorageInitialize, errors.Annotate(err,
			fmt.Sprintf("can't read file for new redo log: %+v", uri)))
	}
	_ = os.Remove(file)
	return nil
}

const (
	// RedoLogFileFormatV1 was used before v6.1.0, which doesn't contain namespace information
	// layout: captureID_changefeedID_fileType_maxEventCommitTs_uuid.fileExtName
	RedoLogFileFormatV1 = "%s_%s_%s_%d_%s%s"
	// RedoLogFileFormatV2 is available since v6.1.0, which contains namespace information
	// layout: captureID_namespace_changefeedID_fileType_maxEventCommitTs_uuid.fileExtName
	RedoLogFileFormatV2 = "%s_%s_%s_%s_%d_%s%s"
	// RedoMetaFileFormat is the format of redo meta file, which contains namespace information.
	// layout: captureID_namespace_changefeedID_fileType_uuid.fileExtName
	RedoMetaFileFormat = "%s_%s_%s_%s_%s%s"
)

// logFormat2ParseFormat converts redo log file name format to the space separated
// format, which can be read and parsed by sscanf. Besides remove the suffix `%s`
// which is used as file name extension, since we will parse extension first.
func logFormat2ParseFormat(fmtStr string) string {
	return strings.TrimSuffix(strings.ReplaceAll(fmtStr, "_", " "), "%s")
}

// ParseLogFileName extract the commitTs, fileType from log fileName
func ParseLogFileName(name string) (uint64, string, error) {
	ext := filepath.Ext(name)
	if ext == MetaEXT {
		return 0, RedoMetaFileType, nil
	}

	// if .sort, the name should be like
	// fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", w.cfg.captureID,
	// w.cfg.changeFeedID.Namespace,w.cfg.changeFeedID.ID,
	// w.cfg.fileType, w.commitTS.Load(), uuid, LogEXT)+SortLogEXT
	if ext == SortLogEXT {
		name = strings.TrimSuffix(name, SortLogEXT)
		ext = filepath.Ext(name)
	}
	if ext != LogEXT && ext != TmpEXT {
		return 0, "", nil
	}

	var commitTs uint64
	var captureID, namespace, changefeedID, fileType, uid string
	// if the namespace is not default, the log looks like:
	// fmt.Sprintf("%s_%s_%s_%s_%d_%s%s", w.cfg.captureID,
	// w.cfg.changeFeedID.Namespace,w.cfg.changeFeedID.ID,
	// w.cfg.fileType, w.commitTS.Load(), uuid, redo.LogEXT)
	// otherwise it looks like:
	// fmt.Sprintf("%s_%s_%s_%d_%s%s", w.cfg.captureID,
	// w.cfg.changeFeedID.ID,
	// w.cfg.fileType, w.commitTS.Load(), uuid, redo.LogEXT)
	var (
		vars      []any
		formatStr string
	)
	if len(strings.Split(name, "_")) == 6 {
		formatStr = logFormat2ParseFormat(RedoLogFileFormatV2)
		vars = []any{&captureID, &namespace, &changefeedID, &fileType, &commitTs, &uid}
	} else {
		formatStr = logFormat2ParseFormat(RedoLogFileFormatV1)
		vars = []any{&captureID, &changefeedID, &fileType, &commitTs, &uid}
	}
	name = strings.ReplaceAll(name, "_", " ")
	_, err := fmt.Sscanf(name, formatStr, vars...)
	if err != nil {
		return 0, "", errors.Annotatef(err, "bad log name: %s", name)
	}
	return commitTs, fileType, nil
}
