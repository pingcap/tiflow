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

func newS3ExternalStorage(
	ctx context.Context, uri string, options *brStorage.S3BackendOptions,
) (brStorage.ExternalStorage, error) {
	opts := &brStorage.BackendOptions{
		S3: *options,
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

func scopeURI(bucket BucketName, scope internal.ResourceScope) string {
	return fmt.Sprintf("s3://%s/%s/%s",
		url.QueryEscape(bucket),
		url.QueryEscape(string(scope.Executor)),
		url.QueryEscape(scope.WorkerID))
}

func newS3ExternalStorageForScope(
	ctx context.Context,
	bucket BucketName,
	scope internal.ResourceScope,
	options *brStorage.S3BackendOptions,
) (brStorage.ExternalStorage, error) {
	return newS3ExternalStorage(ctx, scopeURI(bucket, scope), options)
}
