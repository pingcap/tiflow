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

// resourceDescriptor is a handle for a s3-backed resource used
// internally in engine/pkg/externalresource.
//
// Note that this implementation caches a brStorage.ExternalStorage object,
// so it is not thread-safe to use. But thread-safety does not seem
// to be a necessary requirement.
type resourceDescriptor struct {
	Bucket  BucketName
	Ident   internal.ResourceIdent
	Options *brStorage.S3BackendOptions

	storage brStorage.ExternalStorage
}

func newResourceDescriptor(
	bucket BucketName,
	ident internal.ResourceIdent,
	options *brStorage.S3BackendOptions,
) *resourceDescriptor {
	return &resourceDescriptor{
		Bucket:  bucket,
		Ident:   ident,
		Options: options,
	}
}

// ExternalStorage creates the storage object if one has not been created yet, and returns the
// created storage object.
func (r *resourceDescriptor) ExternalStorage(ctx context.Context) (brStorage.ExternalStorage, error) {
	if r.storage == nil {
		storage, err := r.makeExternalStorage(ctx)
		if err != nil {
			return nil, errors.Annotate(err, "creating ExternalStorage for s3")
		}
		r.storage = storage
	}
	return r.storage, nil
}

// makeExternalStorage actually creates the storage object.
func (r *resourceDescriptor) makeExternalStorage(ctx context.Context) (brStorage.ExternalStorage, error) {
	uri := r.generateURI()
	opts := &brStorage.BackendOptions{
		S3: *r.Options,
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

func (r *resourceDescriptor) URI() string {
	return r.generateURI()
}

func (r *resourceDescriptor) generateURI() string {
	return fmt.Sprintf("s3:///%s/%s/%s/%s",
		url.QueryEscape(r.Bucket),
		url.QueryEscape(string(r.Ident.Executor)),
		url.QueryEscape(r.Ident.WorkerID),
		url.QueryEscape(r.Ident.Name))
}
