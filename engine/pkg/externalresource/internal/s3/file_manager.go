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
	gerrors "errors"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"go.uber.org/zap"
)

const (
	placeholderFileName = ".keep"

	DummyJobID        = "dummy-job-%s"
	DummyWorkerID     = "keep-alive-worker"
	DummyResourceName = "dummy"
	DummyResourceID   = "/s3/dummy"
)

// FileManager manages resource files stored on s3.
type FileManager struct {
	executorID     model.ExecutorID
	bucketSelector BucketSelector
	storageFactory ExternalStorageFactory
	index          indexManager
	prefix         string
}

// NewFileManagerWithConfig returns a new s3 FileManager.
// Note that the lifetime of the returned object should span the whole
// lifetime of the executor.
func NewFileManagerWithConfig(
	executorID resModel.ExecutorID, config resModel.S3Config,
) *FileManager {
	bucketSelector := NewConstantBucketSelector(config.Bucket)
	factory := NewExternalStorageFactoryWithPrefix(config.Prefix, &config.S3BackendOptions)
	return NewFileManager(executorID, bucketSelector, factory)
}

// NewFileManager creates a new s3 FileManager.
func NewFileManager(
	executorID resModel.ExecutorID,
	bucketSelector BucketSelector,
	factory ExternalStorageFactory,
) *FileManager {
	return &FileManager{
		executorID:     executorID,
		bucketSelector: bucketSelector,
		storageFactory: factory,
		index:          newIndexManager(executorID, bucketSelector, factory),
	}
}

// CreateResource creates a new resource on s3.
func (m *FileManager) CreateResource(
	ctx context.Context, ident internal.ResourceIdent,
) (internal.ResourceDescriptor, error) {
	m.validateExecutor(ident.Executor, ident)
	bucket, err := m.bucketSelector.GetBucket(ctx, ident.Scope())
	if err != nil {
		return nil, errors.Annotate(err, "FileManager: CreateResource")
	}

	desc := newResourceDescriptor(bucket, ident, m.storageFactory)
	storage, err := desc.ExternalStorage(ctx)
	if err != nil {
		return nil, err
	}

	if err := createPlaceholderFile(ctx, storage); err != nil {
		return nil, err
	}
	return desc, nil
}

// GetPersistedResource returns the descriptor of a resource that has already
// been marked as persisted.
// Note that GetPersistedResource will work on any executor for any persisted resource.
func (m *FileManager) GetPersistedResource(
	ctx context.Context, ident internal.ResourceIdent,
) (internal.ResourceDescriptor, error) {
	persistedResourceSet, err := m.index.LoadPersistedFileSet(ctx, ident.Scope())
	if err != nil {
		if gerrors.Is(err, errIndexFileDoesNotExist) {
			return nil, internal.ErrResourceFilesNotFound.GenWithStack(
				&internal.ResourceFilesNotFoundError{
					Ident:   ident,
					Details: err.Error(),
				})
		}
		return nil, errors.Annotate(err, "FileManager: GetPersistedResource")
	}
	if _, ok := persistedResourceSet[ident.Name]; !ok {
		return nil, internal.ErrResourceFilesNotFound.GenWithStack(
			&internal.ResourceFilesNotFoundError{
				Ident: ident,
			})
	}

	bucket, err := m.bucketSelector.GetBucket(ctx, ident.Scope())
	if err != nil {
		return nil, errors.Annotate(err, "FileManager: GetPersistedResource")
	}

	desc := newResourceDescriptor(bucket, ident, m.storageFactory)
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

// RemoveTemporaryFiles removes all temporary resources (those that are not persisted).
// It can only be used to clean up resources created by the local executor.
func (m *FileManager) RemoveTemporaryFiles(
	ctx context.Context, scope internal.ResourceScope,
) error {
	m.validateExecutor(scope.Executor, scope)
	if scope.WorkerID == "" {
		return m.removeAllTemporaryFilesForExecutor(ctx, scope)
	}

	persistedFiles, err := m.index.LoadPersistedFileSet(ctx, scope)
	if err != nil && !gerrors.Is(err, errIndexFileDoesNotExist) {
		return err
	}

	log.Info("Removing temporary resources for single worker", zap.Any("scope", scope))

	return m.removeFilesIf(ctx, scope, func(path string) bool {
		resName, _, ok := strings.Cut(path, "/")
		if !ok {
			return false
		}
		_, ok = persistedFiles[resName]
		return !ok
	})
}

func (m *FileManager) removeAllTemporaryFilesForExecutor(
	ctx context.Context, scope internal.ResourceScope,
) error {
	// get all persisted files which is created by current executor
	persistedFileSet := make(map[resModel.ResourceName]struct{})

	persistedFileMap := m.index.GetPersistedFileMap()
	for workerID, indexFile := range persistedFileMap {
		for resName, _ := range indexFile.PersistedFileSet {
			resPath := fmt.Sprintf("%s/%s", workerID, resName)
			persistedFileSet[resPath] = struct{}{}
		}
	}

	return m.removeAllTemporaryFilesByMeta(ctx, scope, persistedFileSet)
}

// removeAllTemporaryFilesByMeta removes all temporary resources located in the given scope.
// Note that this function could be called from executor and master.
func (m *FileManager) removeAllTemporaryFilesByMeta(
	ctx context.Context,
	scope internal.ResourceScope,
	persistedResSet map[resModel.ResourceName]struct{},
) error {
	log.Info("Removing temporary resources for executor", zap.Any("scope", scope))

	return m.removeFilesIf(ctx, scope, func(path string) bool {
		workerID, filePath, ok := strings.Cut(path, "/")
		if !ok {
			return false
		}
		resName, _, ok := strings.Cut(filePath, "/")
		if !ok {
			return false
		}
		resPath := fmt.Sprintf("%s/%s", workerID, resName)
		if !ok {
			return false
		}
		_, ok = persistedResSet[resPath]
		return !ok
	})
}

// RemoveResource removes a resource from s3. It can be called
// on any executor node.
func (m *FileManager) RemoveResource(
	ctx context.Context, ident internal.ResourceIdent,
) error {
	log.Info("Removing resource",
		zap.Any("ident", ident))

	err := m.removeFilesIf(ctx, ident.Scope(), func(path string) bool {
		resName, _, ok := strings.Cut(path, "/")
		if !ok {
			return false
		}
		return resName == ident.Name
	})

	return err
}

// SetPersisted marks a resource as persisted. It can only be called
// on the creator of the resource.
func (m *FileManager) SetPersisted(
	ctx context.Context, ident internal.ResourceIdent,
) error {
	m.validateExecutor(ident.Executor, ident)
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

func (m *FileManager) validateExecutor(creator model.ExecutorID, res interface{}) {
	// Defensive verification to ensure that local resources are not accessible across nodes.
	if creator != m.executorID {
		log.Panic("inconsistent executor ID of s3 file",
			zap.Any("resource", res),
			zap.Any("creator", creator),
			zap.String("currentExecutor", string(m.executorID)))
	}
}

// Close closes the FileManager.
func (m *FileManager) Close() {
	m.index.Close()
}

func (m *FileManager) removeFilesIf(
	ctx context.Context,
	scope internal.ResourceScope,
	pred func(path string) bool,
) error {
	bucket, err := m.bucketSelector.GetBucket(ctx, scope)
	if err != nil {
		return err
	}

	storage, err := m.storageFactory.newS3ExternalStorageForScope(ctx, bucket, scope)
	if err != nil {
		return err
	}

	var toRemoveFiles []string
	err = storage.WalkDir(ctx, &brStorage.WalkOption{}, func(path string, _ int64) error {
		path = strings.TrimPrefix(path, "/")
		if pred(path) {
			toRemoveFiles = append(toRemoveFiles, path)
		}
		return nil
	})
	if err != nil {
		return errors.Annotate(err, "RemoveTemporaryFiles")
	}

	log.Info("Removing resources",
		zap.Any("scope", scope),
		zap.Any("file-set", toRemoveFiles))

	for _, path := range toRemoveFiles {
		if err := storage.DeleteFile(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

func createPlaceholderFile(ctx context.Context, storage brStorage.ExternalStorage) error {
	exists, err := storage.FileExists(ctx, placeholderFileName)
	if err != nil {
		return errors.Annotate(err, "checking placeholder file")
	}
	if exists {
		// This should not happen in production. Unless the caller of the FileManager has a bug.
		return errors.New("resource already exists")
	}

	writer, err := storage.Create(ctx, placeholderFileName)
	if err != nil {
		return errors.Annotate(err, "creating placeholder file")
	}

	_, err = writer.Write(ctx, []byte("dummy"))
	if err != nil {
		return errors.Annotate(err, "creating placeholder file")
	}

	if err := writer.Close(ctx); err != nil {
		return errors.Annotate(err, "creating placeholder file")
	}
	return nil
}

func GetDummyIdent(executorID model.ExecutorID) internal.ResourceIdent {
	return internal.ResourceIdent{
		Name: DummyResourceName,
		ResourceScope: internal.ResourceScope{
			Executor: executorID,
			WorkerID: DummyWorkerID,
		},
	}
}

// PreCheckConfig does a preflight check on the executor's storage configurations.
func PreCheckConfig(config resModel.S3Config) error {
	// TODO: use customized retry policy.
	factory := NewExternalStorageFactoryWithPrefix(config.Prefix, &config.S3BackendOptions)
	_, err := factory.newS3ExternalStorageForScope(context.Background(),
		config.Bucket, internal.ResourceScope{})
	return err
}
