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
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	gcsStorage "cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const defaultTimeout = 5 * time.Minute

// GetExternalStorageFromURI creates a new storage.ExternalStorage from a uri.
func GetExternalStorageFromURI(
	ctx context.Context, uri string,
) (storage.ExternalStorage, error) {
	return GetExternalStorage(ctx, uri, nil, DefaultS3Retryer())
}

// GetExternalStorageWithDefaultTimeout creates a new storage.ExternalStorage from a uri
// without retry. It is the caller's responsibility to set timeout to the context.
func GetExternalStorageWithDefaultTimeout(ctx context.Context, uri string) (storage.ExternalStorage, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	// total retry time is [1<<7, 1<<8] = [128, 256] + 30*6 = [308, 436] seconds
	r := NewS3Retryer(7, 1*time.Second, 2*time.Second)
	s, err := GetExternalStorage(ctx, uri, nil, r)

	return &extStorageWithTimeout{
		ExternalStorage: s,
		timeout:         defaultTimeout,
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

	// Check the connection and ignore the returned bool value, since we don't care if the file exists.
	_, err = ret.FileExists(ctx, "test")
	if err != nil {
		retErr := errors.ErrFailToCreateExternalStorage.Wrap(errors.Trace(err))
		return nil, retErr.GenWithStackByArgs("creating ExternalStorage for s3")
	}
	return ret, nil
}

// GetTestExtStorage creates a test storage.ExternalStorage from a uri.
func GetTestExtStorage(
	ctx context.Context, tmpDir string,
) (storage.ExternalStorage, *url.URL, error) {
	uriStr := fmt.Sprintf("file://%s", tmpDir)
	ret, err := GetExternalStorageFromURI(ctx, uriStr)
	if err != nil {
		return nil, nil, err
	}
	uri, err := storage.ParseRawURL(uriStr)
	if err != nil {
		return nil, nil, err
	}
	return ret, uri, nil
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

// NewS3Retryer creates a new s3 retryer.
func NewS3Retryer(maxRetries int, minRetryDelay, minThrottleDelay time.Duration) request.Retryer {
	return retryerWithLog{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries:    maxRetries,
			MinRetryDelay:    minRetryDelay,
			MinThrottleDelay: minThrottleDelay,
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
	ctx context.Context, path string, _ *storage.ReaderOption,
) (storage.ExternalFileReader, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.ExternalStorage.Open(ctx, path, nil)
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
	ctx context.Context, path string, option *storage.WriterOption,
) (storage.ExternalFileWriter, error) {
	if option.Concurrency <= 1 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	// multipart uploading spawns a background goroutine, can't set timeout
	return s.ExternalStorage.Create(ctx, path, option)
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

	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		if respErr.StatusCode == http.StatusNotFound {
			return true
		}
	}
	return false
}

// RemoveFilesIf removes files from external storage if the path matches the predicate.
func RemoveFilesIf(
	ctx context.Context,
	extStorage storage.ExternalStorage,
	pred func(path string) bool,
	opt *storage.WalkOption,
) error {
	var toRemoveFiles []string
	err := extStorage.WalkDir(ctx, opt, func(path string, _ int64) error {
		path = strings.TrimPrefix(path, "/")
		if pred(path) {
			toRemoveFiles = append(toRemoveFiles, path)
		}
		return nil
	})
	if err != nil {
		return errors.ErrExternalStorageAPI.Wrap(err).GenWithStackByArgs("RemoveTemporaryFiles")
	}

	log.Debug("Removing files", zap.Any("toRemoveFiles", toRemoveFiles))
	return DeleteFilesInExtStorage(ctx, extStorage, toRemoveFiles)
}

// DeleteFilesInExtStorage deletes files in external storage concurrently.
// TODO: Add a test for this function to cover batch delete.
func DeleteFilesInExtStorage(
	ctx context.Context, extStorage storage.ExternalStorage, toRemoveFiles []string,
) error {
	limit := make(chan struct{}, 32)
	batch := 3000
	eg, egCtx := errgroup.WithContext(ctx)
	for len(toRemoveFiles) > 0 {
		select {
		case <-egCtx.Done():
			return egCtx.Err()
		case limit <- struct{}{}:
		}

		if len(toRemoveFiles) < batch {
			batch = len(toRemoveFiles)
		}
		files := toRemoveFiles[:batch]
		eg.Go(func() error {
			defer func() { <-limit }()
			err := extStorage.DeleteFiles(egCtx, files)
			if err != nil && !IsNotExistInExtStorage(err) {
				// if fail then retry, may end up with notExit err, ignore the error
				return errors.ErrExternalStorageAPI.Wrap(err)
			}
			return nil
		})
		toRemoveFiles = toRemoveFiles[batch:]
	}
	return eg.Wait()
}
