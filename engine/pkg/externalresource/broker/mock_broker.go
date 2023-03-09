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
	"context"
	"os"
	"sync"
	"testing"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/bucket"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var _ Broker = (*MockBroker)(nil)

// MockBroker is a broker used to testing other components that depend on a Broker
type MockBroker struct {
	*DefaultBroker

	config   *resModel.Config
	clientMu sync.Mutex
	client   *manager.MockClient

	mu            sync.Mutex
	persistedList []resModel.ResourceID
}

// NewBrokerForTesting creates a MockBroker instance for testing only
func NewBrokerForTesting(executorID resModel.ExecutorID) *MockBroker {
	dir, err := os.MkdirTemp("/tmp", "*-localfiles")
	if err != nil {
		log.Panic("failed to make tempdir", zap.Error(err))
	}
	cfg := &resModel.Config{Local: resModel.LocalFileConfig{BaseDir: dir}}
	client := manager.NewMockClient()
	broker, err := NewBrokerWithConfig(cfg, executorID, client)
	if err != nil {
		log.Panic("failed to create broker")
	}

	s3dir, err := os.MkdirTemp("/tmp", "*-s3files")
	if err != nil {
		log.Panic("failed to make tempdir", zap.Error(err))
	}
	s3FileManager, _ := bucket.NewFileManagerForUT(s3dir, executorID)
	broker.fileManagers[resModel.ResourceTypeS3] = s3FileManager

	return &MockBroker{
		DefaultBroker: broker,
		config:        cfg,
		client:        client,
	}
}

// OpenStorage wraps broker.OpenStorage
func (b *MockBroker) OpenStorage(
	ctx context.Context,
	projectInfo tenant.ProjectInfo,
	workerID resModel.WorkerID,
	jobID resModel.JobID,
	resourcePath resModel.ResourceID,
	opts ...OpenStorageOption,
) (Handle, error) {
	b.clientMu.Lock()
	defer b.clientMu.Unlock()

	b.client.On("QueryResource", mock.Anything,
		&pb.QueryResourceRequest{ResourceKey: &pb.ResourceKey{JobId: jobID, ResourceId: resourcePath}}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), errors.ErrResourceDoesNotExist.GenWithStackByArgs(resourcePath))
	defer func() {
		b.client.ExpectedCalls = nil
	}()
	h, err := b.DefaultBroker.OpenStorage(ctx, projectInfo, workerID, jobID, resourcePath)
	if err != nil {
		return nil, err
	}

	return &brExternalStorageHandleForTesting{parent: b, Handle: h}, nil
}

// AssertPersisted checks resource is in persisted list
func (b *MockBroker) AssertPersisted(t *testing.T, id resModel.ResourceID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	require.Contains(t, b.persistedList, id)
}

func (b *MockBroker) appendPersistRecord(id resModel.ResourceID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.persistedList = append(b.persistedList, id)
}

// AssertFileExists checks lock file exists
func (b *MockBroker) AssertFileExists(
	t *testing.T,
	workerID resModel.WorkerID,
	resourceID resModel.ResourceID,
	fileName string,
) {
	tp, resName, err := resModel.ParseResourceID(resourceID)
	require.NoError(t, err)
	require.Equal(t, resModel.ResourceTypeLocalFile, tp)
	local.AssertLocalFileExists(t, b.config.Local.BaseDir, workerID, resName, fileName)
}

type brExternalStorageHandleForTesting struct {
	Handle
	parent *MockBroker
}

func (h *brExternalStorageHandleForTesting) Persist(ctx context.Context) error {
	h.parent.appendPersistRecord(h.ID())
	return nil
}
