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
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	indexFileName = ".index"
)

// indexManager is used to manage the set of all persisted file resources
// on s3.
//
// The logic is that, when a worker or executor goes offline, as
// a garbage collection mechanism, we remove all files on s3 that's NOT in
// the index.
//
// This mechanism helps keeping the garbage collection mechanism consistent
// with what we have for the local file resources.
type indexManager interface {
	SetPersisted(
		ctx context.Context, ident internal.ResourceIdent,
	) (bool, error)

	LoadPersistedFileSet(
		ctx context.Context, scope internal.ResourceScope,
	) (map[model.ResourceName]struct{}, error)

	Close()
}

type indexFile struct {
	PersistedFileSet map[model.ResourceName]struct{} `json:"persisted_file_set"`
}

func newIndexFile() *indexFile {
	return &indexFile{
		PersistedFileSet: map[model.ResourceName]struct{}{},
	}
}

func (f *indexFile) SetPersisted(ident internal.ResourceIdent) bool {
	if _, ok := f.PersistedFileSet[ident.Name]; ok {
		return false
	}
	f.PersistedFileSet[ident.Name] = struct{}{}
	return true
}

func (f *indexFile) UnsetPersisted(ident internal.ResourceIdent) bool {
	if _, ok := f.PersistedFileSet[ident.Name]; !ok {
		return false
	}
	delete(f.PersistedFileSet, ident.Name)
	return true
}

func (f *indexFile) DeserializeFrom(bytes []byte) error {
	if err := json.Unmarshal(bytes, &f.PersistedFileSet); err != nil {
		return errors.Annotate(err, "deserialize index file from s3")
	}
	return nil
}

func (f *indexFile) Serialize() ([]byte, error) {
	bytes, err := json.Marshal(f.PersistedFileSet)
	if err != nil {
		return nil, errors.Annotate(err, "serialize index file to s3")
	}
	return bytes, nil
}

func (f *indexFile) GetPersistedFileSet() map[model.ResourceName]struct{} {
	return f.PersistedFileSet
}

type indexManagerImpl struct {
	executorID     model.ExecutorID
	bucketSelector BucketSelector
	storageFactory ExternalStorageFactory

	indexMu     sync.Mutex
	indexFiles  map[model.WorkerID]*indexFile
	updateCount int

	wg                 sync.WaitGroup
	flushIndexNotifyCh chan struct {
		project  tenant.ProjectInfo
		workerID model.WorkerID
		doneCh   chan error
	}
	closeCh  chan struct{}
	isClosed atomic.Bool
}

func newIndexManager(
	executorID model.ExecutorID,
	bucketSelector BucketSelector,
	factory ExternalStorageFactory,
) *indexManagerImpl {
	ret := &indexManagerImpl{
		executorID:     executorID,
		bucketSelector: bucketSelector,
		storageFactory: factory,

		indexFiles: make(map[model.WorkerID]*indexFile),

		// a maximum of one pending notification is enough.
		flushIndexNotifyCh: make(chan struct {
			project  tenant.ProjectInfo
			workerID model.WorkerID
			doneCh   chan error
		}, 1),
		// closeCh is closed when we want the background task to exit.
		closeCh: make(chan struct{}),
	}

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		ret.backgroundTask()
	}()
	return ret
}

func (m *indexManagerImpl) SetPersisted(ctx context.Context, ident internal.ResourceIdent) (bool, error) {
	if m.isClosed.Load() {
		return false, errors.New("indexManager already closed")
	}

	if m.executorID != ident.Executor {
		log.Panic("Cannot persist resource that is not created on the local executor",
			zap.Any("ident", ident))
	}

	m.indexMu.Lock()
	index, ok := m.indexFiles[ident.WorkerID]
	if !ok {
		index = newIndexFile()
		m.indexFiles[ident.WorkerID] = index
	}

	ok = index.SetPersisted(ident)
	m.indexMu.Unlock()
	if !ok {
		return false, nil
	}

	if err := m.triggerFlush(ctx, ident.WorkerID); err != nil {
		return false, err
	}
	return true, nil
}

func (m *indexManagerImpl) LoadPersistedFileSet(
	ctx context.Context, scope internal.ResourceScope,
) (map[model.ResourceName]struct{}, error) {
	storage, err := m.createStorageForIndexFile(ctx, scope)
	if err != nil {
		return nil, err
	}

	bytes, err := storage.ReadFile(ctx, indexFileName)
	if err != nil {
		return nil, err
	}
	index := newIndexFile()
	if err := index.DeserializeFrom(bytes); err != nil {
		return nil, err
	}

	return index.GetPersistedFileSet(), nil
}

func (m *indexManagerImpl) Close() {
	if m.isClosed.Swap(true) {
		// Already closed
		return
	}
	close(m.closeCh)
}

func (m *indexManagerImpl) backgroundTask() {
	for {
		select {
		case <-m.closeCh:
			return
		case flushTask := <-m.flushIndexNotifyCh:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := m.doFlush(ctx, flushTask.project, flushTask.workerID)
			cancel()
			flushTask.doneCh <- err
		}
	}
}

func (m *indexManagerImpl) triggerFlush(ctx context.Context, workerID model.WorkerID) error {
	done := make(chan error, 1)
	flushTask := struct {
		project  tenant.ProjectInfo
		workerID model.WorkerID
		doneCh   chan error
	}{
		workerID: workerID,
		doneCh:   done,
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case m.flushIndexNotifyCh <- flushTask:
	}

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-done:
		if err != nil {
			return err
		}
		return nil
	}
}

func (m *indexManagerImpl) doFlush(
	ctx context.Context,
	project tenant.ProjectInfo,
	workerID model.WorkerID,
) error {
	m.indexMu.Lock()
	index, ok := m.indexFiles[workerID]
	if !ok {
		m.indexMu.Unlock()
		log.Info("indexManager: worker has been removed",
			zap.String("worker-id", workerID))
		return nil
	}

	bytes, err := index.Serialize()
	m.indexMu.Unlock()
	if err != nil {
		return err
	}

	storage, err := m.createStorageForIndexFile(ctx, internal.ResourceScope{
		ProjectInfo: project,
		Executor:    m.executorID,
		WorkerID:    workerID,
	})
	if err != nil {
		return err
	}

	err = storage.WriteFile(ctx, ".index", bytes)
	if err != nil {
		return err
	}

	return nil
}

func (m *indexManagerImpl) createStorageForIndexFile(
	ctx context.Context,
	scope internal.ResourceScope,
) (brStorage.ExternalStorage, error) {
	bucket, err := m.bucketSelector.GetBucket(ctx, scope)
	if err != nil {
		return nil, err
	}

	return m.storageFactory.newS3ExternalStorageForScope(ctx, bucket, scope)
}
