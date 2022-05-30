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

	"github.com/stretchr/testify/require"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
)

// These constants are only used for unit testing.
const (
	jobManager = libModel.WorkerType(iota + 1)
	fakeJobMaster
)

func TestMasterMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	metaClient, err := pkgOrm.NewMockClient()
	require.Nil(t, err)
	meta := []*libModel.MasterMetaKVData{
		{
			ID: JobManagerUUID,
			Tp: jobManager,
		},
		{
			ID: "master-1",
			Tp: fakeJobMaster,
		},
		{
			ID: "master-2",
			Tp: fakeJobMaster,
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
		&libModel.MasterMetaKVData{
			ID: masters[0].ID,
			Tp: masters[0].Tp,
		}, &libModel.MasterMetaKVData{
			ID: JobManagerUUID,
			Tp: jobManager,
		})
	require.Equal(t,
		&libModel.MasterMetaKVData{
			ID: masters[1].ID,
			Tp: masters[1].Tp,
		}, &libModel.MasterMetaKVData{
			ID: "master-1",
			Tp: fakeJobMaster,
		})
	require.Equal(t,
		&libModel.MasterMetaKVData{
			ID: masters[2].ID,
			Tp: masters[2].Tp,
		}, &libModel.MasterMetaKVData{
			ID: "master-2",
			Tp: fakeJobMaster,
		})
}

func TestOperateMasterMetadata(t *testing.T) {
	t.Parallel()
	var (
		ctx   = context.Background()
		addr1 = "127.0.0.1:10000"
		addr2 = "127.0.0.1:10001"
		meta  = &libModel.MasterMetaKVData{
			ID:         "master-1",
			Tp:         fakeJobMaster,
			Addr:       addr1,
			StatusCode: libModel.MasterStatusInit,
		}
	)
	metaClient, err := pkgOrm.NewMockClient()
	require.Nil(t, err)

	loadMeta := func() *libModel.MasterMetaKVData {
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
	require.Equal(t, libModel.MasterStatusUninit, loadMeta().StatusCode)
}

func TestLoadAllWorkers(t *testing.T) {
	t.Parallel()

	metaClient, err := pkgOrm.NewMockClient()
	require.Nil(t, err)
	workerMetaClient := NewWorkerMetadataClient("master-1", metaClient)

	// Using context.Background() since there is no risk that
	// the mock KV might time out.
	err = workerMetaClient.Store(context.Background(), &libModel.WorkerStatus{
		JobID:        "master-1",
		ID:           "worker-1",
		Code:         libModel.WorkerStatusInit,
		ErrorMessage: "test-1",
		ExtBytes:     []byte("ext-bytes-1"),
	})
	require.NoError(t, err)

	err = workerMetaClient.Store(context.Background(), &libModel.WorkerStatus{
		JobID:        "master-1",
		ID:           "worker-2",
		Code:         libModel.WorkerStatusNormal,
		ErrorMessage: "test-2",
		ExtBytes:     []byte("ext-bytes-2"),
	})
	require.NoError(t, err)

	err = workerMetaClient.Store(context.Background(), &libModel.WorkerStatus{
		JobID:        "master-1",
		ID:           "worker-3",
		Code:         libModel.WorkerStatusFinished,
		ErrorMessage: "test-3",
		ExtBytes:     []byte("ext-bytes-3"),
	})
	require.NoError(t, err)

	workerStatuses, err := workerMetaClient.LoadAllWorkers(context.Background())
	require.NoError(t, err)
	require.Len(t, workerStatuses, 3)
	require.Equal(t,
		map[libModel.WorkerID]*libModel.WorkerStatus{
			"worker-1": {
				Code:         libModel.WorkerStatusInit,
				ErrorMessage: "test-1",
				ExtBytes:     []byte("ext-bytes-1"),
			},
			"worker-2": {
				Code:         libModel.WorkerStatusNormal,
				ErrorMessage: "test-2",
				ExtBytes:     []byte("ext-bytes-2"),
			},
			"worker-3": {
				Code:         libModel.WorkerStatusFinished,
				ErrorMessage: "test-3",
				ExtBytes:     []byte("ext-bytes-3"),
			},
		},
		map[libModel.WorkerID]*libModel.WorkerStatus{
			workerStatuses["worker-1"].ID: {
				Code:         workerStatuses["worker-1"].Code,
				ErrorMessage: workerStatuses["worker-1"].ErrorMessage,
				ExtBytes:     workerStatuses["worker-1"].ExtBytes,
			},
			workerStatuses["worker-2"].ID: {
				Code:         workerStatuses["worker-2"].Code,
				ErrorMessage: workerStatuses["worker-2"].ErrorMessage,
				ExtBytes:     workerStatuses["worker-2"].ExtBytes,
			},
			workerStatuses["worker-3"].ID: {
				Code:         workerStatuses["worker-3"].Code,
				ErrorMessage: workerStatuses["worker-3"].ErrorMessage,
				ExtBytes:     workerStatuses["worker-3"].ExtBytes,
			},
		},
	)
}
