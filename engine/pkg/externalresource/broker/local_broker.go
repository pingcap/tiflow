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
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/gogo/status"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/storagecfg"
)

// LocalBroker is a broker unit-testing other components
// that depend on a Broker.
type LocalBroker struct {
	*Impl

	clientMu sync.Mutex
	client   *manager.MockClient

	mu            sync.Mutex
	persistedList []resourcemeta.ResourceID
}

func NewBrokerForTesting(executorID resourcemeta.ExecutorID) *LocalBroker {
	dir, err := ioutil.TempDir("/tmp", "*-localfiles")
	if err != nil {
		log.L().Panic("failed to make tempdir")
	}
	cfg := &storagecfg.Config{Local: &storagecfg.LocalFileConfig{BaseDir: dir}}
	client := manager.NewWrappedMockClient()
	return &LocalBroker{
		Impl:   NewBroker(cfg, executorID, client),
		client: client.GetLeaderClient().(*manager.MockClient),
	}
}

func (b *LocalBroker) OpenStorage(
	ctx context.Context,
	workerID resourcemeta.WorkerID,
	jobID resourcemeta.JobID,
	resourcePath resourcemeta.ResourceID,
) (Handle, error) {
	b.clientMu.Lock()
	defer b.clientMu.Unlock()

	st, err := status.New(codes.Internal, "resource manager error").WithDetails(&pb.ResourceError{
		ErrorCode: pb.ResourceErrorCode_ResourceNotFound,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	b.client.On("QueryResource", mock.Anything, &pb.QueryResourceRequest{ResourceId: resourcePath}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), st.Err())
	defer func() {
		b.client.ExpectedCalls = nil
	}()
	h, err := b.Impl.OpenStorage(ctx, workerID, jobID, resourcePath)
	if err != nil {
		return nil, err
	}

	return &brExternalStorageHandleForTesting{parent: b, Handle: h}, nil
}

func (b *LocalBroker) AssertPersisted(t *testing.T, id resourcemeta.ResourceID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	require.Contains(t, b.persistedList, id)
}

func (b *LocalBroker) appendPersistRecord(id resourcemeta.ResourceID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.persistedList = append(b.persistedList, id)
}

func (b *LocalBroker) AssertFileExists(
	t *testing.T,
	workerID resourcemeta.WorkerID,
	resourceID resourcemeta.ResourceID,
	fileName string,
) {
	suffix := strings.TrimPrefix(resourceID, "/local/")
	filePath := filepath.Join(b.config.Local.BaseDir, workerID, suffix, fileName)
	require.FileExists(t, filePath)
}

type brExternalStorageHandleForTesting struct {
	Handle
	parent *LocalBroker
}

func (h *brExternalStorageHandleForTesting) Persist(ctx context.Context) error {
	h.parent.appendPersistRecord(h.ID())
	return nil
}
