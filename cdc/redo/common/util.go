//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package common

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// RedoLogFileFormatV1 was used before v6.1.0, which doesn't contain namespace information
	// layout: captureID_changefeedID_fileType_maxEventCommitTs_uuid.fileExtName
	RedoLogFileFormatV1 = "%s_%s_%s_%d_%s%s"
	// RedoLogFileFormatV2 is available since v6.1.0, which contains namespace information
	// layout: captureID_namespace_changefeedID_fileType_maxEventCommitTs_uuid.fileExtName
	RedoLogFileFormatV2 = "%s_%s_%s_%s_%d_%s%s"
)

// InitS3storage init a storage used for s3,
// s3URI should be like s3URI="s3://logbucket/test-changefeed?endpoint=http://$S3_ENDPOINT/"
var InitS3storage = func(ctx context.Context, uri url.URL) (storage.ExternalStorage, error) {
	if len(uri.Host) == 0 {
		return nil, cerror.WrapError(cerror.ErrS3StorageInitialize, errors.Errorf("please specify the bucket for s3 in %v", uri))
	}

	prefix := strings.Trim(uri.Path, "/")
	s3 := &backuppb.S3{Bucket: uri.Host, Prefix: prefix}
	options := &storage.BackendOptions{}
	storage.ExtractQueryParameters(&uri, &options.S3)
	if err := options.S3.Apply(s3); err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageInitialize, err)
	}

	// we should set this to true, since br set it by default in parseBackend
	s3.ForcePathStyle = true
	backend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_S3{S3: s3},
	}
	s3storage, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{
		SendCredentials: false,
		HTTPClient:      nil,
	})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrS3StorageInitialize, err)
	}

	return s3storage, nil
}

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
		return 0, DefaultMetaFileType, nil
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
	var s1, s2, fileType, uid string
	// the log looks like: fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileType, w.commitTS.Load(), redo.LogEXT)
	formatStr := logFormat2ParseFormat(RedoLogFileFormatV1)
	name = strings.ReplaceAll(name, "_", " ")
	_, err := fmt.Sscanf(name, formatStr, &s1, &s2, &fileType, &commitTs, &uid)
	if err != nil {
		return 0, "", errors.Annotatef(err, "bad log name: %s", name)
	}

	return commitTs, fileType, nil
}
