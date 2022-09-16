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

package s3

import (
	"context"
	"fmt"
	"net/url"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
)

type ExternalStorageFactory interface {
	newS3ExternalStorageForScope(
		ctx context.Context,
		bucket BucketName,
		scope internal.ResourceScope,
	) (brStorage.ExternalStorage, error)

	newS3ExternalStorageFromURI(
		ctx context.Context,
		uri string,
	) (brStorage.ExternalStorage, error)

	scopeURI(bucket BucketName, scope internal.ResourceScope) string
}

// ExternalStorageFactoryImpl implements ExternalStorageFactory.
// It is exported for testing purposes.
type ExternalStorageFactoryImpl struct {
	// Prefix is an optional prefix in the S3 file path.
	// It can be useful when a shared bucket is used for testing purposes.
	Prefix string

	// Options provide necessary information such as endpoints and access key
	// for creating an s3 client.
	Options *brStorage.S3BackendOptions
}

func newExternalStorageFactory(
	options *brStorage.S3BackendOptions,
) *ExternalStorageFactoryImpl {
	return &ExternalStorageFactoryImpl{
		Options: options,
	}
}

// NewExternalStorageFactoryWithPrefix is exported for integration tests.
func NewExternalStorageFactoryWithPrefix(
	prefix string,
	options *brStorage.S3BackendOptions,
) *ExternalStorageFactoryImpl {
	return &ExternalStorageFactoryImpl{Prefix: prefix, Options: options}
}

func (f *ExternalStorageFactoryImpl) newS3ExternalStorageForScope(
	ctx context.Context,
	bucket BucketName,
	scope internal.ResourceScope,
) (brStorage.ExternalStorage, error) {
	uri := f.scopeURI(bucket, scope)
	return f.newS3ExternalStorageFromURI(ctx, uri)
}

func (f *ExternalStorageFactoryImpl) scopeURI(
	bucket BucketName, scope internal.ResourceScope,
) string {
	if len(f.Prefix) == 0 {
		return fmt.Sprintf("s3://%s/%s/%s",
			url.QueryEscape(bucket),
			url.QueryEscape(string(scope.Executor)),
			url.QueryEscape(scope.WorkerID))
	}
	return fmt.Sprintf("s3://%s/%s/%s/%s",
		url.QueryEscape(bucket),
		f.Prefix,
		url.QueryEscape(string(scope.Executor)),
		url.QueryEscape(scope.WorkerID))
}

func (f *ExternalStorageFactoryImpl) newS3ExternalStorageFromURI(
	ctx context.Context,
	uri string,
) (brStorage.ExternalStorage, error) {
	opts := &brStorage.BackendOptions{
		S3: *f.Options,
	}
	backEnd, err := brStorage.ParseBackend(uri, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Note that we may have network I/O here.
	ret, err := brStorage.New(ctx, backEnd, &brStorage.ExternalStorageOptions{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ret, nil
}
