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

package storage

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/errors"
	bstorage "github.com/pingcap/tidb/br/pkg/storage"
)

type LoaderPath struct {
	// the path in config
	Origin string
	// the path we actually use, format is 'Origin/taskName.sourceID'
	Actual string
	// the path we use in old version and should be moved to actual, format is 'Origin.taskName'
	Moved string
}

// VerifyLoaderPath verify loader path, return Origin/Actual/Moved paths.
// it should be no side effect and can be called repeatedly.
// before v6.1 loader path is like:
// s3 --> user_set_dir + / + taskName + . + sourceID
// local --> user_set_dir + . + taskName
// after v6.1 loader path is like:
// s3 --> user_set_dir + / + taskName + . + sourceID
// local --> user_set_dir + . + sourceID + . + taskName
// For upgrade:
// s3 --> do nothing
// local --> dm should automatically move files, users do nothing
// For downgrade:
// s3 --> do nothing
// local --> do nothing, because new path's suffix is still '.taskName'.
func VerifyLoaderPath(raw string, taskName string, sourceID string) (*LoaderPath, error) {
	loaderPath := &LoaderPath{}
	var err error
	if raw == "" || taskName == "" || sourceID == "" {
		return loaderPath, err
	}
	s3ActualSuffix := "/" + taskName + "." + sourceID
	actualSuffix := "/" + sourceID + "." + taskName
	movedSuffix := "." + taskName
	trimPath := ""

	u, err := bstorage.ParseRawURL(raw)
	if err != nil {
		return loaderPath, errors.Trace(err)
	}
	// local
	if u.Scheme == "" {
		// avoid duplicate add uniqueID, and trim suffix '/' like './dump_data/'
		trimPath = strings.TrimRight(raw, string(filepath.Separator))
		if strings.HasSuffix(trimPath, actualSuffix) {
			loaderPath.Origin = strings.TrimSuffix(raw, actualSuffix)
			loaderPath.Actual = raw
			loaderPath.Moved = loaderPath.Origin + movedSuffix
		} else if strings.HasSuffix(trimPath, movedSuffix) {
			loaderPath.Origin = strings.TrimSuffix(trimPath, movedSuffix)
			loaderPath.Actual = loaderPath.Origin + actualSuffix
			loaderPath.Moved = raw
		} else {
			loaderPath.Origin = raw
			loaderPath.Actual = trimPath + actualSuffix
			loaderPath.Moved = loaderPath.Origin + movedSuffix
		}
		// check Moved exist
		_, err := os.Stat(loaderPath.Actual)
		if err == nil {
			loaderPath.Moved = ""
		} else {
			if os.IsNotExist(err) {
				_, err = os.Stat(loaderPath.Moved)
				if err != nil {
					if os.IsNotExist(err) {
						loaderPath.Moved = ""
					} else {
						return nil, err
					}
				}
			} else {
				return nil, err
			}
		}
	} else {
		trimPath = strings.TrimRight(u.Path, string(filepath.Separator))
		if strings.HasSuffix(trimPath, s3ActualSuffix) {
			u.Path = strings.TrimSuffix(trimPath, actualSuffix)
			loaderPath.Origin = u.String()
			loaderPath.Actual = raw
		} else {
			u.Path = trimPath + actualSuffix
			loaderPath.Origin = raw
			loaderPath.Actual = u.String()
		}
	}
	return loaderPath, err
}

// AdjustPath adjust rawURL, add uniqueId as path suffix, returns a new path and will not change rawURL.
// This function supports both local dir or s3 path. It can be used like the following:
// 1. adjust subtask's `LoaderConfig.Dir`, uniqueID like `.test-mysql01`.
// 2. add Lightning checkpoint's fileName to rawURL, uniqueID like `/tidb_lightning_checkpoint.pb`.
func AdjustPath(rawURL string, uniqueID string) (string, error) {
	if rawURL == "" || uniqueID == "" {
		return rawURL, nil
	}
	u, err := bstorage.ParseRawURL(rawURL)
	if err != nil {
		return "", errors.Trace(err)
	}
	// not url format, we don't use url library to avoid being escaped or unescaped
	if u.Scheme == "" {
		// avoid duplicate add uniqueID, and trim suffix '/' like './dump_data/'
		trimPath := strings.TrimRight(rawURL, string(filepath.Separator))
		if !strings.HasSuffix(trimPath, uniqueID) {
			return trimPath + uniqueID, nil
		}
		return rawURL, nil
	}
	// u.Path is an unescaped string and can be used as normal
	trimPath := strings.TrimRight(u.Path, string(filepath.Separator))
	if !strings.HasSuffix(trimPath, uniqueID) {
		u.Path = trimPath + uniqueID
		// u.String will return escaped url and can be used safely in other steps
		return u.String(), err
	}
	return rawURL, nil
}

// TrimPath trims rawURL suffix which is uniqueID, supports local and s3.
func TrimPath(rawURL string, uniqueID string) (string, error) {
	if rawURL == "" || uniqueID == "" {
		return rawURL, nil
	}
	u, err := bstorage.ParseRawURL(rawURL)
	if err != nil {
		return "", errors.Trace(err)
	}
	// not url format, we don't use url library to avoid being escaped or unescaped
	if u.Scheme == "" {
		return strings.TrimSuffix(rawURL, uniqueID), nil
	}
	// u.Path is an unescaped string and can be used as normal
	u.Path = strings.TrimSuffix(u.Path, uniqueID)
	// u.String will return escaped url and can be used safely in other steps
	return u.String(), err
}

// isS3Path judges if rawURL is s3 path.
func IsS3Path(rawURL string) bool {
	if rawURL == "" {
		return false
	}
	u, err := bstorage.ParseRawURL(rawURL)
	if err != nil {
		return false
	}
	return u.Scheme == "s3"
}

// CreateStorage creates ExternalStore.
func CreateStorage(ctx context.Context, path string) (bstorage.ExternalStorage, error) {
	backend, err := bstorage.ParseBackend(path, nil)
	if err != nil {
		return nil, err
	}
	return bstorage.New(ctx, backend, &bstorage.ExternalStorageOptions{})
}

// CollectDirFiles gets files in dir.
func CollectDirFiles(ctx context.Context, dir string, storage bstorage.ExternalStorage) (map[string]struct{}, error) {
	var err error
	if storage == nil {
		storage, err = CreateStorage(ctx, dir)
		if err != nil {
			return nil, err
		}
	}
	files := make(map[string]struct{})

	err = storage.WalkDir(ctx, &bstorage.WalkOption{}, func(filePath string, size int64) error {
		name := path.Base(filePath)
		files[name] = struct{}{}
		return nil
	})

	return files, err
}

// RemoveAll remove files in dir.
func RemoveAll(ctx context.Context, dir string, storage bstorage.ExternalStorage) error {
	var err error
	if storage == nil {
		storage, err = CreateStorage(ctx, dir)
		if err != nil {
			return err
		}
	}

	err = storage.WalkDir(ctx, &bstorage.WalkOption{}, func(filePath string, size int64) error {
		return storage.DeleteFile(ctx, filePath)
	})
	if err == nil {
		return storage.DeleteFile(ctx, "")
	}
	return err
}

func ReadFile(ctx context.Context, dir, fileName string, storage bstorage.ExternalStorage) ([]byte, error) {
	var err error
	if storage == nil {
		storage, err = CreateStorage(ctx, dir)
		if err != nil {
			return nil, err
		}
	}
	return storage.ReadFile(ctx, fileName)
}

func OpenFile(ctx context.Context, dir, fileName string, storage bstorage.ExternalStorage) (bstorage.ExternalFileReader, error) {
	var err error
	if storage == nil {
		storage, err = CreateStorage(ctx, dir)
		if err != nil {
			return nil, err
		}
	}
	return storage.Open(ctx, fileName)
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
