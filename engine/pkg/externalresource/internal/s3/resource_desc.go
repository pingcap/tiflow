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

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
)

var _ internal.ResourceDescriptor = (*resourceDescriptor)(nil)

// resourceDescriptor is a handle for a s3-backed resource used
// internally in engine/pkg/externalresource.
//
// Note that this implementation caches a brStorage.ExternalStorage object,
// so it is not thread-safe to use. But thread-safety does not seem
// to be a necessary requirement.
type resourceDescriptor struct {
	Ident internal.ResourceIdent

	storageFactory ExternalStorageFactory
	storage        brStorage.ExternalStorage
}

func newResourceDescriptor(
	ident internal.ResourceIdent,
	factory ExternalStorageFactory,
) *resourceDescriptor {
	return &resourceDescriptor{
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
			return nil, err
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
	return fmt.Sprintf("%s/%s", r.storageFactory.baseURI(), r.Ident.BuildResPath())
}

func (r *resourceDescriptor) ResourceIdent() internal.ResourceIdent {
	return r.Ident
}

func (r *resourceDescriptor) ID() string {
	return resModel.BuildResourceID(resModel.ResourceTypeS3, r.Ident.Name)
}
