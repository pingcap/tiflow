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
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ Broker = (*LocalBroker)(nil)

// LocalBroker is a broker unit-testing other components
// that depend on a Broker.
type LocalBroker struct {
	*DefaultBroker

	config   *resModel.Config
	clientMu sync.Mutex
	client   *manager.MockClient

	mu            sync.Mutex
	persistedList []resModel.ResourceID
}

// NewBrokerForTesting creates a LocalBroker instance for testing only
func NewBrokerForTesting(executorID resModel.ExecutorID) *LocalBroker {
	dir, err := os.MkdirTemp("/tmp", "*-localfiles")
	if err != nil {
		log.Panic("failed to make tempdir")
	}
	cfg := &resModel.Config{Local: resModel.LocalFileConfig{BaseDir: dir}}
	client := manager.NewMockClient()
	broker, err := NewBroker(cfg, executorID, client)
	if err != nil {
		log.Panic("failed to create broker")
	}
	return &LocalBroker{
		DefaultBroker: broker,
		config:        cfg,
		client:        client,
	}
}

// OpenStorage wraps broker.OpenStorage
func (b *LocalBroker) OpenStorage(
	ctx context.Context,
	projectInfo tenant.ProjectInfo,
	workerID resModel.WorkerID,
	jobID resModel.JobID,
	resourcePath resModel.ResourceID,
) (Handle, error) {
	b.clientMu.Lock()
	defer b.clientMu.Unlock()

	st := status.New(codes.NotFound, "resource manager error")

	b.client.On("QueryResource", mock.Anything,
		&pb.QueryResourceRequest{ResourceKey: &pb.ResourceKey{JobId: jobID, ResourceId: resourcePath}}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), st.Err())
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
func (b *LocalBroker) AssertPersisted(t *testing.T, id resModel.ResourceID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	require.Contains(t, b.persistedList, id)
}

func (b *LocalBroker) appendPersistRecord(id resModel.ResourceID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.persistedList = append(b.persistedList, id)
}

// AssertFileExists checks lock file exists
func (b *LocalBroker) AssertFileExists(
	t *testing.T,
	workerID resModel.WorkerID,
	resourceID resModel.ResourceID,
	fileName string,
) {
	suffix := strings.TrimPrefix(resourceID, "/local/")
	local.AssertLocalFileExists(t, b.config.Local.BaseDir, workerID, suffix, fileName)
}

type brExternalStorageHandleForTesting struct {
	Handle
	parent *LocalBroker
}

func (h *brExternalStorageHandleForTesting) Persist(ctx context.Context) error {
	h.parent.appendPersistRecord(h.ID())
	return nil
}
