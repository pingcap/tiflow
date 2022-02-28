package lib

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

const (
	masterName            = "my-master"
	masterNodeName        = "node-1"
	executorNodeID1       = "node-exec-1"
	executorNodeID2       = "node-exec-2"
	executorNodeID3       = "node-exec-3"
	workerTypePlaceholder = 999
	workerID1             = WorkerID("worker-1")
	workerID2             = WorkerID("worker-2")
	workerID3             = WorkerID("worker-3")
)

type dummyConfig struct {
	param int
}

func prepareMeta(ctx context.Context, t *testing.T, metaclient metadata.MetaKV) {
	masterKey := adapter.MasterMetaKey.Encode(masterName)
	masterInfo := &MasterMetaKVData{
		ID:     masterName,
		NodeID: masterNodeName,
	}
	masterInfoBytes, err := json.Marshal(masterInfo)
	require.NoError(t, err)
	_, err = metaclient.Put(ctx, masterKey, string(masterInfoBytes))
	require.NoError(t, err)
}

func TestMasterInit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := NewMockMasterImpl("", masterName)
	prepareMeta(ctx, t, master.metaKVClient)

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	rawResp, err := master.metaKVClient.Get(ctx, adapter.MasterMetaKey.Encode(masterName))
	require.NoError(t, err)
	resp := rawResp.(*clientv3.GetResponse)
	require.Len(t, resp.Kvs, 1)

	var masterData MasterMetaKVData
	err = json.Unmarshal(resp.Kvs[0].Value, &masterData)
	require.NoError(t, err)
	require.True(t, masterData.Initialized)

	master.On("CloseImpl", mock.Anything).Return(nil)
	err = master.Close(ctx)
	require.NoError(t, err)

	// Restart the master
	master.Reset()
	master.On("OnMasterRecovered", mock.Anything).Return(nil)
	err = master.Init(ctx)
	require.NoError(t, err)

	master.On("CloseImpl", mock.Anything).Return(nil)
	err = master.Close(ctx)
	require.NoError(t, err)
}

func TestMasterPollAndClose(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := NewMockMasterImpl("", masterName)
	prepareMeta(ctx, t, master.metaKVClient)

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	master.On("Tick", mock.Anything).Return(nil)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := master.Poll(ctx)
			if err != nil {
				if derror.ErrMasterClosed.Equal(err) {
					return
				}
			}
			require.NoError(t, err)
		}
	}()

	require.Eventually(t, func() bool {
		return master.TickCount() > 10
	}, time.Millisecond*2000, time.Millisecond*10)

	master.On("CloseImpl", mock.Anything).Return(nil)
	err = master.Close(ctx)
	require.NoError(t, err)

	wg.Wait()
}

func TestMasterCreateWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := NewMockMasterImpl("", masterName)
	master.timeoutConfig.workerTimeoutDuration = time.Second * 1000
	master.timeoutConfig.masterHeartbeatCheckLoopInterval = time.Millisecond * 10
	master.uuidGen = uuid.NewMock()
	prepareMeta(ctx, t, master.metaKVClient)

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	MockBaseMasterCreateWorker(
		t,
		master.DefaultBaseMaster,
		workerTypePlaceholder,
		&dummyConfig{param: 1},
		100,
		masterName,
		workerID1,
		executorNodeID1)

	workerID, err := master.CreateWorker(workerTypePlaceholder, &dummyConfig{param: 1}, 100)
	require.NoError(t, err)
	require.Equal(t, workerID1, workerID)

	master.On("OnWorkerDispatched", mock.AnythingOfType("*lib.workerHandleImpl"), nil).Return(nil)
	<-master.dispatchedWorkers
	err = <-master.dispatchedResult
	require.NoError(t, err)

	master.On("OnWorkerOnline", mock.AnythingOfType("*lib.workerHandleImpl")).Return(nil)

	MockBaseMasterWorkerHeartbeat(t, master.DefaultBaseMaster, masterName, workerID1, executorNodeID1)

	master.On("Tick", mock.Anything).Return(nil)
	err = master.Poll(ctx)
	require.NoError(t, err)

	require.Eventuallyf(t, func() bool {
		return master.onlineWorkerCount.Load() == 1
	}, time.Second*1, time.Millisecond*10, "final worker count %d", master.onlineWorkerCount.Load())

	workerList := master.GetWorkers()
	require.Len(t, workerList, 1)
	require.Contains(t, workerList, workerID)

	workerMetaClient := NewWorkerMetadataClient(masterName, master.metaKVClient)
	dummySt := &dummyStatus{Val: 4}
	ext, err := dummySt.Marshal()
	require.NoError(t, err)
	err = workerMetaClient.Store(ctx, workerID1, &WorkerStatus{
		Code:     WorkerStatusNormal,
		ExtBytes: ext,
	})
	require.NoError(t, err)

	err = master.messageHandlerManager.InvokeHandler(
		t,
		workerStatusUpdatedTopic(masterName, workerID1),
		masterName,
		&workerStatusUpdatedMessage{Epoch: master.currentEpoch.Load()})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		err := master.Poll(ctx)
		require.NoError(t, err)
		status := master.GetWorkers()[workerID1].Status()
		return status.Code == WorkerStatusNormal
	}, 1*time.Second, 10*time.Millisecond)

	status := master.GetWorkers()[workerID1].Status()
	require.Equal(t, &WorkerStatus{
		Code:     WorkerStatusNormal,
		ExtBytes: ext,
	}, status)
}

func TestMasterCreateWorkerMetError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := NewMockMasterImpl("", masterName)
	master.timeoutConfig.masterHeartbeatCheckLoopInterval = time.Millisecond * 10
	master.uuidGen = uuid.NewMock()
	prepareMeta(ctx, t, master.metaKVClient)

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	MockBaseMasterCreateWorkerMetScheduleTaskError(
		t,
		master.DefaultBaseMaster,
		workerTypePlaceholder,
		&dummyConfig{param: 1},
		100,
		masterName,
		workerID1,
		executorNodeID1)

	workerID, err := master.CreateWorker(workerTypePlaceholder, &dummyConfig{param: 1}, 100)
	require.NoError(t, err)
	require.Equal(t, workerID1, workerID)

	master.On("OnWorkerDispatched",
		mock.AnythingOfType("*lib.tombstoneWorkerHandleImpl"),
		mock.AnythingOfType("*errors.withStack")).Return(nil)
	<-master.dispatchedWorkers
	err = <-master.dispatchedResult
	require.Regexp(t, ".*ErrClusterResourceNotEnough.*", err)
}

func TestPrepareWorkerConfig(t *testing.T) {
	t.Parallel()

	master := &DefaultBaseMaster{
		uuidGen: uuid.NewMock(),
	}

	type fakeConfig struct {
		JobName     string `json:"job-name"`
		WorkerCount int    `json:"worker-count"`
	}
	fakeWorkerCfg := &fakeConfig{"fake-job", 3}
	fakeCfgBytes := []byte(`{"job-name":"fake-job","worker-count":3}`)
	fakeWorkerID := "worker-1"
	master.uuidGen.(*uuid.MockGenerator).Push(fakeWorkerID)
	testCases := []struct {
		workerType WorkerType
		config     WorkerConfig
		// expected return result
		rawConfig []byte
		workerID  string
	}{
		{
			FakeJobMaster, &MasterMetaKVData{ID: "master-1", Config: fakeCfgBytes},
			fakeCfgBytes, "master-1",
		},
		{
			FakeTask, fakeWorkerCfg,
			fakeCfgBytes, fakeWorkerID,
		},
	}
	for _, tc := range testCases {
		rawConfig, workerID, err := master.prepareWorkerConfig(tc.workerType, tc.config)
		require.NoError(t, err)
		require.Equal(t, tc.rawConfig, rawConfig)
		require.Equal(t, tc.workerID, workerID)
	}
}
