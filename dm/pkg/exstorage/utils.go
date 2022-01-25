// Copyright 2022 PingCAP, Inc.
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

package exstorage

import (
	"context"
	"net/url"
	"path"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// AdjustS3Path adjust s3 rawURL, add uniqueId into s3 path.
func AdjustS3Path(rawURL string, uniqueID string) (bool, string, error) {
	if rawURL == "" {
		return false, "", nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false, "", errors.Trace(err)
	}
	if u.Scheme == "s3" {
		trimPath := strings.TrimRight(u.Path, "/")
		// avoid duplicate add uniqueID
		if uniqueID != "" && !strings.HasSuffix(trimPath, uniqueID) {
			u.Path = trimPath + "." + uniqueID
			return true, u.String(), nil
		}
		return true, rawURL, nil
	}
	return false, rawURL, nil
}

// isS3Path judges if rawURL is s3 path.
func IsS3Path(rawURL string) bool {
	if rawURL == "" {
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	if u.Scheme == "s3" {
		return true
	}
	return false
}

// CreateExternalStore creates ExternalStore.
func CreateExternalStore(ctx context.Context, path string) (storage.ExternalStorage, error) {
	backen, err := storage.ParseBackend(path, nil)
	if err != nil {
		return nil, err
	}
	return storage.New(ctx, backen, &storage.ExternalStorageOptions{})
}

// CollectDirFiles gets files in path.
func CollectDirFiles(ctx context.Context, externalStore storage.ExternalStorage) (map[string]struct{}, error) {
	files := make(map[string]struct{})

	err := externalStore.WalkDir(ctx, &storage.WalkOption{ListCount: 1}, func(filePath string, size int64) error {
		name := path.Base(filePath)
		files[name] = struct{}{}
		return nil
	})

	return files, err
}

func RemoveAll(ctx context.Context, externalStore storage.ExternalStorage) error {
	err := externalStore.WalkDir(ctx, &storage.WalkOption{ListCount: 1}, func(filePath string, size int64) error {
		return externalStore.DeleteFile(ctx, filePath)
	})
	if err == nil {
		return externalStore.DeleteFile(ctx, "")
	}
	return err
}
