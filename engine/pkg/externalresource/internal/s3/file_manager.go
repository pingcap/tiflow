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
	"strings"
	"sync"

	"github.com/pingcap/log"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// TODO: use the contents of placeholder to indicate persistent status
	placeholderFileName = ".keep"
)

type persistedResources map[resModel.ResourceName]struct{}

func newPersistedResources() persistedResources {
	return make(map[resModel.ResourceName]struct{})
}

func (f persistedResources) SetPersisted(ident internal.ResourceIdent) bool {
	if _, ok := f[ident.Name]; ok {
		return false
	}
	f[ident.Name] = struct{}{}
	return true
}

func (f persistedResources) UnsetPersisted(ident internal.ResourceIdent) bool {
	if _, ok := f[ident.Name]; !ok {
		return false
	}
	delete(f, ident.Name)
	return true
}

// FileManager manages resource files stored on s3.
type FileManager struct {
	executorID     model.ExecutorID
	storageFactory ExternalStorageFactory

	mu              sync.RWMutex
	persistedResMap map[resModel.WorkerID]persistedResources
}

// NewFileManagerWithConfig returns a new s3 FileManager.
// Note that the lifetime of the returned object should span the whole
// lifetime of the executor.
func NewFileManagerWithConfig(
	executorID resModel.ExecutorID, config resModel.S3Config,
) *FileManager {
	factory := NewExternalStorageFactory(config.Bucket,
		config.Prefix, &config.S3BackendOptions)
	return NewFileManager(executorID, factory)
}

// NewFileManager creates a new s3 FileManager.
func NewFileManager(
	executorID resModel.ExecutorID,
	factory ExternalStorageFactory,
) *FileManager {
	return &FileManager{
		executorID:      executorID,
		storageFactory:  factory,
		persistedResMap: make(map[string]persistedResources),
	}
}

// CreateResource creates a new resource on s3.
// It can only be used to create resources that belong to the current executor.
func (m *FileManager) CreateResource(
	ctx context.Context, ident internal.ResourceIdent,
) (internal.ResourceDescriptor, error) {
	m.validateExecutor(ident.Executor, ident)
	desc := newResourceDescriptor(ident, m.storageFactory)
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
	desc := newResourceDescriptor(ident, m.storageFactory)
	storage, err := desc.ExternalStorage(ctx)
	if err != nil {
		return nil, err
	}

	ok, err := storage.FileExists(ctx, placeholderFileName)
	if err != nil {
		return nil, errors.ErrExternalStorageAPI.Wrap(err).GenWithStackByArgs("check placeholder file")
	}
	if !ok {
		return nil, errors.ErrResourceFilesNotFound.GenWithStackByArgs()
	}

	return desc, nil
}

// CleanOrRecreatePersistedResource cleans the s3 directory or recreates placeholder
// file of the given resource.
// Note that CleanOrRecreatePersistedResource will work on any executor for any persisted resource.
func (m *FileManager) CleanOrRecreatePersistedResource(
	ctx context.Context, ident internal.ResourceIdent,
) (internal.ResourceDescriptor, error) {
	desc, err := m.GetPersistedResource(ctx, ident)
	if errors.Is(err, errors.ErrResourceFilesNotFound) {
		desc := newResourceDescriptor(ident, m.storageFactory)
		storage, err := desc.ExternalStorage(ctx)
		if err != nil {
			return nil, err
		}

		if err := createPlaceholderFile(ctx, storage); err != nil {
			return nil, err
		}
		return desc, nil
	}
	if err != nil {
		return nil, err
	}

	err = m.removeFilesIf(ctx, ident.Scope(), getPathPredByName(ident.Name, true))
	if err != nil {
		return nil, err
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
		return m.removeTemporaryFilesForExecutor(ctx, scope)
	}
	return m.removeTemporaryFilesForWorker(ctx, scope)
}

func (m *FileManager) removeTemporaryFilesForWorker(
	ctx context.Context, scope internal.ResourceScope,
) error {
	m.mu.RLock()
	resources, ok := m.persistedResMap[scope.WorkerID]
	// unlock here is safe because `resources` will not be changed after worker exits.
	m.mu.RUnlock()

	log.Info("Removing temporary resources for single worker", zap.Any("scope", scope))
	if !ok {
		return m.removeFilesIf(ctx, scope, getPathPredAlwaysTrue())
	}

	return m.removeFilesIf(ctx, scope, getPathPredByPersistedResources(resources, 1))
}

func (m *FileManager) removeTemporaryFilesForExecutor(
	ctx context.Context, scope internal.ResourceScope,
) error {
	// Get all persisted files which is created by current executor.
	persistedResSet := make(map[string]struct{})

	m.mu.RLock()
	for workerID, resources := range m.persistedResMap {
		for resName := range resources {
			resPath := fmt.Sprintf("%s/%s", workerID, resName)
			persistedResSet[resPath] = struct{}{}
		}
	}
	m.mu.RUnlock()

	return m.removeAllTemporaryFilesByMeta(ctx, scope, persistedResSet)
}

// removeAllTemporaryFilesByMeta removes all temporary resources located in the given scope.
// Note that this function could be called from executor and master.
func (m *FileManager) removeAllTemporaryFilesByMeta(
	ctx context.Context,
	scope internal.ResourceScope,
	persistedResSet map[string]struct{},
) error {
	log.Info("Removing temporary resources for executor", zap.Any("scope", scope))

	return m.removeFilesIf(ctx, scope, getPathPredByPersistedResources(persistedResSet, 2))
}

// RemoveResource removes a resource from s3.
// It can be called on any executor node.
func (m *FileManager) RemoveResource(
	ctx context.Context, ident internal.ResourceIdent,
) error {
	log.Info("Removing resource",
		zap.Any("ident", ident))

	err := m.removeFilesIf(ctx, ident.Scope(), getPathPredByName(ident.Name, false))
	if err != nil {
		return err
	}

	if m.executorID == ident.Executor {
		// Remove from persistedResMap
		m.mu.Lock()
		defer m.mu.Unlock()
		if resources, ok := m.persistedResMap[ident.WorkerID]; ok {
			resources.UnsetPersisted(ident)
		}
	}
	return nil
}

// SetPersisted marks a resource as persisted. It can only be called
// on the creator of the resource.
func (m *FileManager) SetPersisted(
	ctx context.Context, ident internal.ResourceIdent,
) error {
	m.validateExecutor(ident.Executor, ident)

	m.mu.Lock()
	defer m.mu.Unlock()
	resources, ok := m.persistedResMap[ident.WorkerID]
	if !ok {
		resources = newPersistedResources()
		m.persistedResMap[ident.WorkerID] = resources
	}

	ok = resources.SetPersisted(ident)
	if !ok {
		log.Warn("resource is already persisted",
			zap.Any("ident", ident))
	}
	return nil
}

func (m *FileManager) validateExecutor(creator model.ExecutorID, res interface{}) {
	if creator != m.executorID {
		log.Panic("inconsistent executor ID of s3 file",
			zap.Any("resource", res),
			zap.Any("creator", creator),
			zap.String("currentExecutor", string(m.executorID)))
	}
}

func (m *FileManager) removeFilesIf(
	ctx context.Context,
	scope internal.ResourceScope,
	pred func(path string) bool,
) error {
	// TODO: add a cache here to reuse storage.
	storage, err := m.storageFactory.newS3ExternalStorageForScope(ctx, scope)
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
		return errors.ErrExternalStorageAPI.Wrap(err).GenWithStackByArgs("RemoveTemporaryFiles")
	}

	log.Info("Removing resources",
		zap.Any("scope", scope),
		zap.Any("numOfToRemoveFiles", len(toRemoveFiles)))
	log.Debug("Removing files", zap.Any("toRemoveFiles", toRemoveFiles))

	for _, path := range toRemoveFiles {
		if err := storage.DeleteFile(ctx, path); err != nil {
			return errors.ErrExternalStorageAPI.Wrap(err)
		}
	}
	return nil
}

func createPlaceholderFile(ctx context.Context, storage brStorage.ExternalStorage) error {
	exists, err := storage.FileExists(ctx, placeholderFileName)
	if err != nil {
		return errors.ErrExternalStorageAPI.Wrap(err).GenWithStackByArgs("checking placeholder file")
	}
	if exists {
		// This should not happen in production. Unless the caller of the FileManager has a bug.
		return errors.ErrExternalStorageAPI.GenWithStackByArgs("resource already exists")
	}

	writer, err := storage.Create(ctx, placeholderFileName, nil)
	if err != nil {
		return errors.ErrExternalStorageAPI.Wrap(err).GenWithStackByArgs("creating placeholder file")
	}

	_, err = writer.Write(ctx, []byte("placeholder"))
	if err != nil {
		return errors.ErrExternalStorageAPI.Wrap(err).GenWithStackByArgs("writing placeholder file")
	}

	if err := writer.Close(ctx); err != nil {
		return errors.ErrExternalStorageAPI.Wrap(err).GenWithStackByArgs("closing placeholder file")
	}
	return nil
}

// PreCheckConfig does a preflight check on the executor's storage configurations.
func PreCheckConfig(config resModel.S3Config) error {
	// TODO: use customized retry policy.
	log.Debug("pre-checking s3Storage config", zap.Any("config", config))
	factory := NewExternalStorageFactory(config.Bucket,
		config.Prefix, &config.S3BackendOptions)
	_, err := factory.newS3ExternalStorageForScope(context.Background(), internal.ResourceScope{})
	if err != nil {
		return err
	}
	return nil
}
