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

package local

import (
	"context"
	"path/filepath"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
)

var _ internal.ResourceDescriptor = (*FileResourceDescriptor)(nil)

// FileResourceDescriptor contains necessary data
// to access a local file resource.
type FileResourceDescriptor struct {
	BasePath string
	Ident    internal.ResourceIdent

	storage brStorage.ExternalStorage
}

// AbsolutePath returns the absolute path of the given resource
// in the local file system.
func (d *FileResourceDescriptor) AbsolutePath() string {
	encodedName := ResourceNameToFilePathName(d.Ident.Name)
	return filepath.Join(d.BasePath, d.Ident.WorkerID, encodedName)
}

// ExternalStorage creates the storage object if one has not been created yet, and returns the
// created storage object.
func (d *FileResourceDescriptor) ExternalStorage(ctx context.Context) (brStorage.ExternalStorage, error) {
	if d.storage == nil {
		storage, err := newBrStorageForLocalFile(d.AbsolutePath())
		if err != nil {
			return nil, errors.Annotate(err, "creating ExternalStorage for local file")
		}
		d.storage = storage
	}
	return d.storage, nil
}

// URI returns the URI of the local file resource.
func (d *FileResourceDescriptor) URI() string {
	return d.AbsolutePath()
}

// ID returns the resource ID of the local file resource.
func (d *FileResourceDescriptor) ID() resModel.ResourceID {
	return resModel.BuildResourceID(resModel.ResourceTypeLocalFile, d.Ident.Name)
}

// ResourceIdent returns the resource identity of the local file resource.
func (d *FileResourceDescriptor) ResourceIdent() internal.ResourceIdent {
	return d.Ident
}
