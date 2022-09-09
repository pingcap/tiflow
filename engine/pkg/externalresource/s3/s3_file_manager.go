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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"go.uber.org/zap"
)

const (
	placeholderFileName = ".keep"
)

type FileManager struct {
	bucketSelector BucketSelector
	options        *brStorage.S3BackendOptions
	index          indexManager
}

func (m *FileManager) CreateResource(
	ctx context.Context, ident internal.ResourceIdent,
) (internal.ResourceDescriptor, error) {
	bucket, err := m.bucketSelector.GetBucket(ctx, ident.Scope())
	if err != nil {
		return nil, errors.Annotate(err, "FileManager: CreateResource")
	}

	desc := newResourceDescriptor(bucket, ident, m.options)
	storage, err := desc.ExternalStorage(ctx)
	if err != nil {
		return nil, err
	}

	if err := createPlaceholderFile(ctx, storage); err != nil {
		return nil, err
	}
	return desc, nil
}

func (m *FileManager) GetPersistedResource(
	ctx context.Context, ident internal.ResourceIdent,
) (internal.ResourceDescriptor, error) {
	bucket, err := m.bucketSelector.GetBucket(ctx, ident.Scope())
	if err != nil {
		return nil, errors.Annotate(err, "FileManager: GetPersistedResource")
	}

	desc := newResourceDescriptor(bucket, ident, m.options)
	storage, err := desc.ExternalStorage(ctx)
	if err != nil {
		return nil, err
	}

	ok, err := storage.FileExists(ctx, placeholderFileName)
	if err != nil {
		return nil, errors.Annotate(err, "check placeholder file")
	}
	if !ok {
		return nil, internal.ErrResourceFilesNotFound.GenWithStack(
			&internal.ResourceFilesNotFoundError{
				Ident: ident,
			})
	}

	return desc, nil
}

func (m *FileManager) RemoveTemporaryFiles(
	ctx context.Context, scope internal.ResourceScope,
) error {
	persistedFiles, err := m.index.LoadPersistedFileSet(ctx, scope)
	if err != nil {
		return err
	}

	bucket, err := m.bucketSelector.GetBucket(ctx, scope)
	if err != nil {
		return err
	}

	storage, err := newS3ExternalStorageForScope(ctx, bucket, scope, m.options)
	if err != nil {
		return err
	}

	var toRemoveFiles map[string]struct{}
	err = storage.WalkDir(ctx, &brStorage.WalkOption{}, func(path string, _ int64) error {
		path = strings.TrimPrefix(path, "/")
		resName, _, ok := strings.Cut(path, "/")
		if !ok {
			return nil
		}

		if _, ok := persistedFiles[resName]; ok {
			// Skip persisted files
			return nil
		}

		toRemoveFiles[path] = struct{}{}
		return nil
	})
	if err != nil {
		return errors.Annotate(err, "RemoveTemporaryFiles")
	}

	log.Info("Removing temporary resources",
		zap.Any("scope", scope),
		zap.Any("file-set", toRemoveFiles))

	for name := range toRemoveFiles {
		if err := storage.DeleteFile(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (m *FileManager) RemoveResource(
	ctx context.Context, ident internal.ResourceIdent,
) error {
	// TODO implement me
	panic("implement me")
}

func (m *FileManager) SetPersisted(
	ctx context.Context, ident internal.ResourceIdent,
) error {
	ok, err := m.index.SetPersisted(ctx, ident)
	if err != nil {
		return err
	}
	if !ok {
		log.Warn("resource is already persisted",
			zap.Any("ident", ident))
	}
	return nil
}

func createPlaceholderFile(ctx context.Context, storage brStorage.ExternalStorage) error {
	writer, err := storage.Create(ctx, placeholderFileName)
	if err != nil {
		return errors.Annotate(err, "creating placeholder file")
	}

	if err := writer.Close(ctx); err != nil {
		return errors.Annotate(err, "creating placeholder file")
	}
	return nil
}
