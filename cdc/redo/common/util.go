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

// ParseLogFileName extract the commitTs, fileType from log fileName
func ParseLogFileName(name string) (uint64, string, error) {
	ext := filepath.Ext(name)
	if ext == MetaEXT {
		return 0, DefaultMetaFileType, nil
	}

	// if .sort, the name should be like
	// fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", w.cfg.captureID,
	// w.cfg.changeFeedID.Namespace,w.cfg.changeFeedID.ID,
	// w.cfg.createTime.Unix(), w.cfg.fileType, w.commitTS.Load(), LogEXT)+SortLogEXT
	if ext == SortLogEXT {
		name = strings.TrimSuffix(name, SortLogEXT)
		ext = filepath.Ext(name)
	}
	if ext != LogEXT && ext != TmpEXT {
		return 0, "", nil
	}

	var commitTs, d1 uint64
	var s1, namespace, s2, fileType string
	// if the namespace is not default, the log looks like:
	// fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", w.cfg.captureID,
	// w.cfg.changeFeedID.Namespace,w.cfg.changeFeedID.ID,
	// w.cfg.createTime.Unix(), w.cfg.fileType, w.commitTS.Load(), redo.LogEXT)
	// otherwise it looks like:
	// fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID,
	// w.cfg.changeFeedID.ID,
	// w.cfg.createTime.Unix(), w.cfg.fileType, w.commitTS.Load(), redo.LogEXT)
	var (
		vars      []any
		formatStr string
	)
	if len(strings.Split(name, "_")) == 6 {
		formatStr = "%s %s %s %d %s %d" + LogEXT
		vars = []any{&s1, &namespace, &s2, &d1, &fileType, &commitTs}
	} else {
		formatStr = "%s %s %d %s %d" + LogEXT
		vars = []any{&s1, &s2, &d1, &fileType, &commitTs}
	}
	name = strings.ReplaceAll(name, "_", " ")
	if ext == TmpEXT {
		formatStr += TmpEXT
	}
	_, err := fmt.Sscanf(name, formatStr, vars...)
	if err != nil {
		return 0, "", errors.Annotatef(err, "bad log name: %s", name)
	}
	return commitTs, fileType, nil
}
