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
	Bucket BucketName
	Ident  internal.ResourceIdent

	storageFactory ExternalStorageFactory
	storage        brStorage.ExternalStorage
}

func newResourceDescriptor(
	bucket BucketName,
	ident internal.ResourceIdent,
	factory ExternalStorageFactory,
) *resourceDescriptor {
	return &resourceDescriptor{
		Bucket:         bucket,
		Ident:          ident,
		storageFactory: factory,
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
	return r.storageFactory.newS3ExternalStorageFromURI(ctx, r.URI())
}

func (r *resourceDescriptor) URI() string {
	return r.storageFactory.scopeURI(r.Bucket, r.Ident.Scope()) + "/" + r.Ident.Name
}
