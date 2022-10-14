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

package metadata

import (
	"context"
	"testing"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/stretchr/testify/require"
)

// These constants are only used for unit testing.
const (
	jobManager = frameModel.WorkerType(iota + 1)
	fakeJobMaster
)

func TestMasterMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	metaClient, err := pkgOrm.NewMockClient()
	require.Nil(t, err)
	defer metaClient.Close()
	meta := []*frameModel.MasterMeta{
		{
			ID:   JobManagerUUID,
			Type: jobManager,
		},
		{
			ID:   "master-1",
			Type: fakeJobMaster,
		},
		{
			ID:   "master-2",
			Type: fakeJobMaster,
		},
	}
	for _, data := range meta {
		cli := NewMasterMetadataClient(data.ID, metaClient)
		err := cli.Store(ctx, data)
		require.Nil(t, err)
	}
	cli := NewMasterMetadataClient("job-manager", metaClient)
	masters, err := cli.LoadAllMasters(ctx)
	require.Nil(t, err)
	require.Len(t, masters, 3)
	require.Equal(t,
		&frameModel.MasterMeta{
			ID:   masters[0].ID,
			Type: masters[0].Type,
		}, &frameModel.MasterMeta{
			ID:   JobManagerUUID,
			Type: jobManager,
		})
	require.Equal(t,
		&frameModel.MasterMeta{
			ID:   masters[1].ID,
			Type: masters[1].Type,
		}, &frameModel.MasterMeta{
			ID:   "master-1",
			Type: fakeJobMaster,
		})
	require.Equal(t,
		&frameModel.MasterMeta{
			ID:   masters[2].ID,
			Type: masters[2].Type,
		}, &frameModel.MasterMeta{
			ID:   "master-2",
			Type: fakeJobMaster,
		})
}

func TestOperateMasterMetadata(t *testing.T) {
	t.Parallel()
	var (
		ctx   = context.Background()
		addr1 = "127.0.0.1:10000"
		addr2 = "127.0.0.1:10001"
		meta  = &frameModel.MasterMeta{
			ID:    "master-1",
			Type:  fakeJobMaster,
			Addr:  addr1,
			State: frameModel.MasterStateInit,
		}
	)
	metaClient, err := pkgOrm.NewMockClient()
	require.Nil(t, err)
	defer metaClient.Close()

	loadMeta := func() *frameModel.MasterMeta {
		cli := NewMasterMetadataClient(meta.ID, metaClient)
		meta, err := cli.Load(ctx)
		require.NoError(t, err)
		return meta
	}

	// persist master meta for the first time
	err = StoreMasterMeta(ctx, metaClient, meta)
	require.NoError(t, err)
	require.Equal(t, addr1, loadMeta().Addr)

	// overwrite master meta
	meta.Addr = addr2
	err = StoreMasterMeta(ctx, metaClient, meta)
	require.NoError(t, err)
	require.Equal(t, addr2, loadMeta().Addr)

	err = DeleteMasterMeta(ctx, metaClient, meta.ID)
	require.NoError(t, err)
	// meta is not found in metastore, load meta will return a new master meta
	require.Equal(t, "", loadMeta().Addr)
	require.Equal(t, frameModel.MasterStateUninit, loadMeta().State)
}

func TestLoadAllWorkers(t *testing.T) {
	t.Parallel()

	metaClient, err := pkgOrm.NewMockClient()
	require.Nil(t, err)
	defer metaClient.Close()
	workerMetaClient := NewWorkerStatusClient("master-1", metaClient)

	// Using context.Background() since there is no risk that
	// the mock KV might time out.
	err = workerMetaClient.Store(context.Background(), &frameModel.WorkerStatus{
		JobID:    "master-1",
		ID:       "worker-1",
		State:    frameModel.WorkerStateInit,
		ErrorMsg: "test-1",
		ExtBytes: []byte("ext-bytes-1"),
	})
	require.NoError(t, err)

	err = workerMetaClient.Store(context.Background(), &frameModel.WorkerStatus{
		JobID:    "master-1",
		ID:       "worker-2",
		State:    frameModel.WorkerStateNormal,
		ErrorMsg: "test-2",
		ExtBytes: []byte("ext-bytes-2"),
	})
	require.NoError(t, err)

	err = workerMetaClient.Store(context.Background(), &frameModel.WorkerStatus{
		JobID:    "master-1",
		ID:       "worker-3",
		State:    frameModel.WorkerStateFinished,
		ErrorMsg: "test-3",
		ExtBytes: []byte("ext-bytes-3"),
	})
	require.NoError(t, err)

	workerStatuses, err := workerMetaClient.LoadAllWorkers(context.Background())
	require.NoError(t, err)
	require.Len(t, workerStatuses, 3)
	require.Equal(t,
		map[frameModel.WorkerID]*frameModel.WorkerStatus{
			"worker-1": {
				State:    frameModel.WorkerStateInit,
				ErrorMsg: "test-1",
				ExtBytes: []byte("ext-bytes-1"),
			},
			"worker-2": {
				State:    frameModel.WorkerStateNormal,
				ErrorMsg: "test-2",
				ExtBytes: []byte("ext-bytes-2"),
			},
			"worker-3": {
				State:    frameModel.WorkerStateFinished,
				ErrorMsg: "test-3",
				ExtBytes: []byte("ext-bytes-3"),
			},
		},
		map[frameModel.WorkerID]*frameModel.WorkerStatus{
			workerStatuses["worker-1"].ID: {
				State:    workerStatuses["worker-1"].State,
				ErrorMsg: workerStatuses["worker-1"].ErrorMsg,
				ExtBytes: workerStatuses["worker-1"].ExtBytes,
			},
			workerStatuses["worker-2"].ID: {
				State:    workerStatuses["worker-2"].State,
				ErrorMsg: workerStatuses["worker-2"].ErrorMsg,
				ExtBytes: workerStatuses["worker-2"].ExtBytes,
			},
			workerStatuses["worker-3"].ID: {
				State:    workerStatuses["worker-3"].State,
				ErrorMsg: workerStatuses["worker-3"].ErrorMsg,
				ExtBytes: workerStatuses["worker-3"].ExtBytes,
			},
		},
	)
}
