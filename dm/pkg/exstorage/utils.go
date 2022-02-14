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
	"os"
	"path"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// AdjustPath adjust rawURL, add uniqueId as path suffix.
func AdjustPath(rawURL string, uniqueID string) (string, error) {
	if rawURL == "" {
		return rawURL, nil
	}
	u, err := storage.ParseRawURL(rawURL)
	if err != nil {
		return "", errors.Trace(err)
	}
	trimPath := strings.TrimRight(u.Path, "/")
	// avoid duplicate add uniqueID
	if uniqueID != "" && !strings.HasSuffix(trimPath, uniqueID) {
		u.Path = trimPath + "." + uniqueID
		newURL, err := url.QueryUnescape(u.String())
		if err != nil {
			return "", errors.Trace(err)
		}
		return newURL, nil
	}

	return rawURL, nil
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
	backend, err := storage.ParseBackend(path, nil)
	if err != nil {
		return nil, err
	}
	return storage.New(ctx, backend, &storage.ExternalStorageOptions{})
}

// CollectDirFiles gets files in dir.
func CollectDirFiles(ctx context.Context, dir string, externalStore storage.ExternalStorage) (map[string]struct{}, error) {
	var err error
	if externalStore == nil {
		externalStore, err = CreateExternalStore(ctx, dir)
		if err != nil {
			return nil, err
		}
	}
	files := make(map[string]struct{})

	err = externalStore.WalkDir(ctx, &storage.WalkOption{ListCount: 1}, func(filePath string, size int64) error {
		name := path.Base(filePath)
		files[name] = struct{}{}
		return nil
	})

	return files, err
}

// RemoveAll remove files in dir.
func RemoveAll(ctx context.Context, dir string, externalStore storage.ExternalStorage) error {
	var err error
	if externalStore == nil {
		externalStore, err = CreateExternalStore(ctx, dir)
		if err != nil {
			return err
		}
	}

	err = externalStore.WalkDir(ctx, &storage.WalkOption{ListCount: 1}, func(filePath string, size int64) error {
		return externalStore.DeleteFile(ctx, filePath)
	})
	if err == nil {
		return externalStore.DeleteFile(ctx, "")
	}
	return err
}

func ReadFile(ctx context.Context, dir, fileName string, externalStore storage.ExternalStorage) ([]byte, error) {
	var err error
	if externalStore == nil {
		externalStore, err = CreateExternalStore(ctx, dir)
		if err != nil {
			return nil, err
		}
	}
	return externalStore.ReadFile(ctx, fileName)
}

func OpenFile(ctx context.Context, dir, fileName string, externalStore storage.ExternalStorage) (storage.ExternalFileReader, error) {
	var err error
	if externalStore == nil {
		externalStore, err = CreateExternalStore(ctx, dir)
		if err != nil {
			return nil, err
		}
	}
	return externalStore.Open(ctx, fileName)
}

func IsNotExistError(err error) bool {
	if err == nil {
		return false
	}
	if os.IsNotExist(err) {
		return true
	}
	if aerr, ok := errors.Cause(err).(awserr.Error); ok {
		switch aerr.Code() {
		case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey, "NotFound":
			return true
		}
	}
	return false
}
