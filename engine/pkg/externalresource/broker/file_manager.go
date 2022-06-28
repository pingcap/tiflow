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

package broker

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/storagecfg"
	derrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/fsutil"
)

// LocalFileManager manages the local files resources stored in
// the local file system.
type LocalFileManager struct {
	config storagecfg.LocalFileConfig

	mu                          sync.Mutex
	persistedResourcesByCreator map[frameModel.WorkerID]map[resModel.ResourceName]struct{}
}

// NewLocalFileManager returns a new NewLocalFileManager.
// Note that the lifetime of the returned object should span the whole
// lifetime of the executor.
func NewLocalFileManager(config storagecfg.LocalFileConfig) *LocalFileManager {
	return &LocalFileManager{
		config:                      config,
		persistedResourcesByCreator: make(map[frameModel.WorkerID]map[resModel.ResourceName]struct{}),
	}
}

// CreateResource makes a local directory for the given resource name,
// and returns a LocalFileResourceDescriptor.
// The resource is NOT marked as persisted by this method.
// Only use it when we are sure it is a NEW resource.
func (m *LocalFileManager) CreateResource(
	creator frameModel.WorkerID,
	resName resModel.ResourceName,
) (*LocalFileResourceDescriptor, error) {
	res := &LocalFileResourceDescriptor{
		BasePath:     m.config.BaseDir,
		Creator:      creator,
		ResourceName: resName,
	}
	if err := os.MkdirAll(res.AbsolutePath(), 0o700); err != nil {
		return nil, derrors.ErrCreateLocalFileDirectoryFailed.Wrap(err)
	}
	// TODO check for quota when we implement quota.
	return res, nil
}

// GetPersistedResource checks the given resource exists in the local
// file system and returns a LocalFileResourceDescriptor.
func (m *LocalFileManager) GetPersistedResource(
	creator frameModel.WorkerID,
	resName resModel.ResourceName,
) (*LocalFileResourceDescriptor, error) {
	res := &LocalFileResourceDescriptor{
		BasePath:     m.config.BaseDir,
		Creator:      creator,
		ResourceName: resName,
	}
	if _, err := os.Stat(res.AbsolutePath()); err != nil {
		if os.IsNotExist(err) {
			return nil, derrors.ErrResourceDoesNotExist.GenWithStackByArgs(resName)
		}
		return nil, derrors.ErrReadLocalFileDirectoryFailed.Wrap(err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	resources, exists := m.persistedResourcesByCreator[creator]
	if !exists {
		return nil, derrors.ErrResourceDoesNotExist.GenWithStackByArgs(resName)
	}
	if _, ok := resources[resName]; !ok {
		return nil, derrors.ErrResourceDoesNotExist.GenWithStackByArgs(resName)
	}

	return res, nil
}

// RemoveTemporaryFiles cleans up all temporary files (i.e., unpersisted file resources),
// created by `creator`.
func (m *LocalFileManager) RemoveTemporaryFiles(creator frameModel.WorkerID) error {
	log.L().Info("Start cleaning temporary files",
		zap.String("worker-id", creator))

	creatorResourcePath := filepath.Join(m.config.BaseDir, creator)

	if _, err := os.Stat(creatorResourcePath); err != nil {
		// The directory not existing is expected if the worker
		// has never created any local file resource.
		if os.IsNotExist(err) {
			log.L().Info("RemoveTemporaryFiles: no local files found for worker",
				zap.String("worker-id", creator))
			return nil
		}

		// Other errors need to be thrown to the caller.
		return derrors.ErrReadLocalFileDirectoryFailed.Wrap(err)
	}

	// Iterates over all resources created by `creator`.
	err := iterOverResourceDirectories(creatorResourcePath, func(filePath string) error {
		resName, err := filePathNameToResourceName(filePath)
		if err != nil {
			return err
		}

		if m.isPersisted(creator, resName) {
			// Persisted resources are skipped, as they are NOT temporary.
			return nil
		}

		fullPath := filepath.Join(
			m.config.BaseDir,
			creator,
			resourceNameToFilePathName(resName))
		if err := os.RemoveAll(fullPath); err != nil {
			return derrors.ErrCleaningLocalTempFiles.Wrap(err)
		}

		log.L().Info("temporary resource is removed",
			zap.String("resource-name", resName),
			zap.String("full-path", fullPath))
		return nil
	})

	log.L().Info("Finished cleaning temporary files",
		zap.String("worker-id", creator))
	return err
}

// RemoveResource removes a single resource from the local file system.
// NOTE the caller should handle ErrResourceDoesNotExist appropriately.
func (m *LocalFileManager) RemoveResource(creator frameModel.WorkerID, resName resModel.ResourceName) error {
	if creator == "" {
		log.L().Panic("Empty creator ID is unexpected",
			zap.String("resource-name", resName))
	}

	// filePath is the path for the resource directory on the local
	// file system. Note that the dataflow engine only manages file
	// resources on the directory level, so that business logic using
	// brStorage.ExternalStorage can be compatible.
	filePath := localPathWithEncoding(
		m.config.BaseDir,
		creator,
		resName)
	if _, err := os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			log.L().Info("Trying to remove non-existing resource",
				zap.String("creator", creator),
				zap.String("resource-name", resName))
			return derrors.ErrResourceDoesNotExist.GenWithStackByArgs(resName)
		}
		return derrors.ErrReadLocalFileDirectoryFailed.Wrap(err)
	}

	// Note that the resourcePath is actually a directory.
	if err := os.RemoveAll(filePath); err != nil {
		return derrors.ErrRemovingLocalResource.Wrap(err)
	}

	log.L().Info("Local resource has been removed",
		zap.String("resource-id", resName))

	m.mu.Lock()
	defer m.mu.Unlock()

	if resources := m.persistedResourcesByCreator[creator]; resources != nil {
		if _, ok := resources[resName]; ok {
			delete(resources, resName)
		}
	}
	return nil
}

// SetPersisted marks a file resource as persisted.
// NOTE it is only marked as persisted in memory, because
// we assume that if the executor process crashes, the
// file resources are lost.
func (m *LocalFileManager) SetPersisted(
	creator frameModel.WorkerID,
	resName resModel.ResourceName,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	persistedResourceSet, ok := m.persistedResourcesByCreator[creator]
	if !ok {
		persistedResourceSet = make(map[resModel.ResourceID]struct{})
		m.persistedResourcesByCreator[creator] = persistedResourceSet
	}

	persistedResourceSet[resName] = struct{}{}
	return
}

// isPersisted returns whether a resource has been persisted.
// DO NOT hold the mu when calling this method.
func (m *LocalFileManager) isPersisted(
	creator frameModel.WorkerID,
	resName resModel.ResourceName,
) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	persistedResourceSet, ok := m.persistedResourcesByCreator[creator]
	if !ok {
		return false
	}

	_, isPersisted := persistedResourceSet[resName]
	return isPersisted
}

// iterOverResourceDirectories iterates over all subdirectories in `path`.
func iterOverResourceDirectories(path string, fn func(relPath string) error) error {
	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return derrors.ErrReadLocalFileDirectoryFailed.Wrap(err)
	}

	for _, info := range infos {
		if !info.IsDir() {
			// We skip non-directory files
			continue
		}
		// Note that info.Name() returns the "base name", which
		// is the last part of the file's path.
		name := info.Name()
		if err := fn(name); err != nil {
			return err
		}
	}
	return nil
}

// PreCheckConfig does a preflight check on the executor's storage configurations.
func PreCheckConfig(config storagecfg.Config) error {
	baseDir := config.Local.BaseDir

	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		log.L().Info("Configured local file directory does not existing, try to create one",
			zap.String("dir", baseDir))
		if err := os.MkdirAll(baseDir, 0o700); err != nil {
			return errors.Annotate(err, "engine: failed to create local file directory")
		}
	}

	if err := fsutil.IsDirReadWritable(baseDir); err != nil {
		return derrors.ErrLocalFileDirNotWritable.GenWithStackByArgs()
	}

	diskInfo, err := fsutil.GetDiskInfo(baseDir)
	if err != nil {
		return errors.Annotate(err, "engine: check local file directory failed")
	}
	log.L().Info("Local file directory disk info", zap.Any("disk-info", diskInfo))

	// TODO implement a minimum disk space threshold.
	return nil
}
