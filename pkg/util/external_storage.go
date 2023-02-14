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

package util

import (
	"context"
	"os"
	"strings"
	"time"

	gcsStorage "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// GetExternalStorageFromURI creates a new storage.ExternalStorage from a uri.
func GetExternalStorageFromURI(
	ctx context.Context, uri string,
) (storage.ExternalStorage, error) {
	return GetExternalStorage(ctx, uri, nil, DefaultS3Retryer())
}

// GetExternalStorageWithTimeout creates a new storage.ExternalStorage from a uri
// without retry. It is the caller's responsibility to set timeout to the context.
func GetExternalStorageWithTimeout(
	ctx context.Context, uri string, timeout time.Duration,
) (storage.ExternalStorage, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	s, err := GetExternalStorage(ctx, uri, nil, nil)

	return &extStorageWithTimeout{
		ExternalStorage: s,
		timeout:         timeout,
	}, err
}

// GetExternalStorage creates a new storage.ExternalStorage based on the uri and options.
func GetExternalStorage(
	ctx context.Context, uri string,
	opts *storage.BackendOptions,
	retryer request.Retryer,
) (storage.ExternalStorage, error) {
	backEnd, err := storage.ParseBackend(uri, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret, err := storage.New(ctx, backEnd, &storage.ExternalStorageOptions{
		SendCredentials: false,
		S3Retryer:       retryer,
	})
	if err != nil {
		retErr := errors.ErrFailToCreateExternalStorage.Wrap(errors.Trace(err))
		return nil, retErr.GenWithStackByArgs("creating ExternalStorage for s3")
	}
	return ret, nil
}

// retryerWithLog wraps the client.DefaultRetryer, and logs when retrying.
type retryerWithLog struct {
	client.DefaultRetryer
}

func isDeadlineExceedError(err error) bool {
	return strings.Contains(err.Error(), "context deadline exceeded")
}

func (rl retryerWithLog) ShouldRetry(r *request.Request) bool {
	if isDeadlineExceedError(r.Error) {
		return false
	}
	return rl.DefaultRetryer.ShouldRetry(r)
}

func (rl retryerWithLog) RetryRules(r *request.Request) time.Duration {
	backoffTime := rl.DefaultRetryer.RetryRules(r)
	if backoffTime > 0 {
		log.Warn("failed to request s3, retrying",
			zap.Error(r.Error),
			zap.Duration("backoff", backoffTime))
	}
	return backoffTime
}

// DefaultS3Retryer is the default s3 retryer, maybe this function
// should be extracted to another place.
func DefaultS3Retryer() request.Retryer {
	return retryerWithLog{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    1 * time.Second,
			MinThrottleDelay: 2 * time.Second,
		},
	}
}

type extStorageWithTimeout struct {
	storage.ExternalStorage
	timeout time.Duration
}

// WriteFile writes a complete file to storage, similar to os.WriteFile,
// but WriteFile should be atomic
func (s *extStorageWithTimeout) WriteFile(ctx context.Context, name string, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.WriteFile(ctx, name, data)
}

// ReadFile reads a complete file from storage, similar to os.ReadFile
func (s *extStorageWithTimeout) ReadFile(ctx context.Context, name string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.ReadFile(ctx, name)
}

// FileExists return true if file exists
func (s *extStorageWithTimeout) FileExists(ctx context.Context, name string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.FileExists(ctx, name)
}

// DeleteFile delete the file in storage
func (s *extStorageWithTimeout) DeleteFile(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.DeleteFile(ctx, name)
}

// Open a Reader by file path. path is relative path to storage base path
func (s *extStorageWithTimeout) Open(
	ctx context.Context, path string,
) (storage.ExternalFileReader, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.Open(ctx, path)
}

// WalkDir traverse all the files in a dir.
func (s *extStorageWithTimeout) WalkDir(
	ctx context.Context, opt *storage.WalkOption, fn func(path string, size int64) error,
) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.WalkDir(ctx, opt, fn)
}

// Create opens a file writer by path. path is relative path to storage base path
func (s *extStorageWithTimeout) Create(
	ctx context.Context, path string,
) (storage.ExternalFileWriter, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.Create(ctx, path)
}

// Rename file name from oldFileName to newFileName
func (s *extStorageWithTimeout) Rename(
	ctx context.Context, oldFileName, newFileName string,
) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.Rename(ctx, oldFileName, newFileName)
}

// IsNotExistInExtStorage checks if the error is caused by the file not exist in external storage.
func IsNotExistInExtStorage(err error) bool {
	if err == nil {
		return false
	}

	if os.IsNotExist(errors.Cause(err)) {
		return true
	}

	if aerr, ok := errors.Cause(err).(awserr.Error); ok { // nolint:errorlint
		switch aerr.Code() {
		case s3.ErrCodeNoSuchBucket, s3.ErrCodeNoSuchKey, "NotFound":
			return true
		}
	}

	if errors.Cause(err) == gcsStorage.ErrObjectNotExist { // nolint:errorlint
		return true
	}

	var errResp *azblob.StorageError
	if internalErr, ok := err.(*azblob.InternalError); ok && internalErr.As(&errResp) {
		if errResp.ErrorCode == azblob.StorageErrorCodeBlobNotFound {
			return true
		}
	}
	return false
}
