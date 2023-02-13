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

package bucket

import (
	"context"
	"fmt"
	"net/url"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
)

// BucketCreator represents a creator used to create
// brStorage.ExternalStorage.
// Implementing mock or stub BucketCreator will make
// unit testing easier.
type BucketCreator interface {
	newBucketForScope(
		ctx context.Context, scope internal.ResourceScope,
	) (brStorage.ExternalStorage, error)

	newBucketFromURI(ctx context.Context, uri string) (brStorage.ExternalStorage, error)

	baseURI() string

	resourceType() model.ResourceType
}

// BucketCreatorImpl implements BucketCreator.
// It is exported for testing purposes.
type BucketCreatorImpl struct {
	// Bucket represents a name of an s3 bucket.
	Bucket string
	// Prefix is an optional prefix in the S3 file path.
	// It can be useful when a shared bucket is used for testing purposes.
	Prefix string
	// Options provide necessary information such as endpoints and access key
	// for creating an s3 client.
	Options *brStorage.BackendOptions
	// ResourceType is the bucket type of this creator
	ResourceType model.ResourceType
}

// NewBucketCreator creates a new BucketCreator with s3 options.
func NewBucketCreator(
	bucket string, prefix string,
	options *brStorage.BackendOptions,
) *BucketCreatorImpl {
	return &BucketCreatorImpl{Prefix: prefix, Bucket: bucket, Options: options}
}

func (f *BucketCreatorImpl) newBucketForScope(
	ctx context.Context, scope internal.ResourceScope,
) (brStorage.ExternalStorage, error) {
	// full uri path is like: `s3://bucket/prefix/executorID/workerID`
	uri := fmt.Sprintf("%s/%s", f.baseURI(), scope.BuildResPath())
	return GetExternalStorageFromURI(ctx, uri, f.Options)
}

func (f *BucketCreatorImpl) baseURI() string {
	uri := fmt.Sprintf("%s://%s", string(f.ResourceType), url.QueryEscape(f.Bucket))
	if f.Prefix != "" {
		uri += "/" + url.QueryEscape(f.Prefix)
	}
	return uri
}

func (f *BucketCreatorImpl) resourceType() model.ResourceType {
	return f.ResourceType
}

func (f *BucketCreatorImpl) newBucketFromURI(
	ctx context.Context,
	uri string,
) (brStorage.ExternalStorage, error) {
	return GetExternalStorageFromURI(ctx, uri, f.Options)
}

// GetExternalStorageFromURI creates a new brStorage.ExternalStorage from a uri.
func GetExternalStorageFromURI(
	ctx context.Context, uri string, opts *brStorage.BackendOptions,
) (brStorage.ExternalStorage, error) {
	// Note that we may have network I/O here.
	ret, err := util.GetExternalStorage(ctx, uri, opts, util.DefaultS3Retryer())
	if err != nil {
		retErr := errors.ErrFailToCreateExternalStorage.Wrap(errors.Trace(err))
		return nil, retErr.GenWithStackByArgs("creating ExternalStorage for bucket")
	}
	return ret, nil
}
