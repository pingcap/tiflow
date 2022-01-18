package lib

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
)

const (
	masterName            = "my-master"
	masterNodeName        = "node-1"
	executorNodeName      = "node-2"
	workerTypePlaceholder = 999
	workerID1             = "worker-1"
)

type dummyConfig struct {
	param int
}

func prepareMeta(ctx context.Context, t *testing.T, metaclient metadata.MetaKV) {
	masterKey := adapter.MasterInfoKey.Encode(masterName)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := newMockMasterImpl("my-master")
	prepareMeta(ctx, t, master.metaKVClient)

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	master.messageHandlerManager.AssertHasHandler(t, HeartbeatPingTopic(masterName), &HeartbeatPingMessage{})
	master.messageHandlerManager.AssertHasHandler(t, StatusUpdateTopic(masterName), &StatusUpdateMessage{})

	rawResp, err := master.metaKVClient.Get(ctx, adapter.MasterInfoKey.Encode(masterName))
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

	master.messageHandlerManager.AssertNoHandler(t, HeartbeatPingTopic(masterName))
	master.messageHandlerManager.AssertNoHandler(t, StatusUpdateTopic(masterName))

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := newMockMasterImpl("my-master")
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	master := newMockMasterImpl("my-master")
	prepareMeta(ctx, t, master.metaKVClient)

	master.On("InitImpl", mock.Anything).Return(nil)
	err := master.Init(ctx)
	require.NoError(t, err)

	expectedSchedulerReq := &pb.TaskSchedulerRequest{Tasks: []*pb.ScheduleTask{{
		Task: &pb.TaskRequest{
			Id: 0,
		},
		Cost: 10,
	}}}
	master.serverMasterClient.On(
		"ScheduleTask",
		mock.Anything,
		expectedSchedulerReq,
		mock.Anything).Return(
		&pb.TaskSchedulerResponse{
			Schedule: map[int64]*pb.ScheduleResult{
				0: {
					ExecutorId: executorNodeName,
				},
			},
		}, nil)

	mockExecutorClient := &client.MockExecutorClient{}
	err = master.executorClientManager.AddExecutorClient(executorNodeName, mockExecutorClient)
	require.NoError(t, err)

	configBytes, err := json.Marshal(&dummyConfig{param: 1})
	require.NoError(t, err)

	mockExecutorClient.On("Send",
		mock.Anything,
		&client.ExecutorRequest{
			Cmd: client.CmdDispatchTask,
			Req: &pb.DispatchTaskRequest{
				TaskTypeId: int64(workerTypePlaceholder),
				TaskConfig: configBytes,
			},
		}).Return(&client.ExecutorResponse{Resp: &pb.DispatchTaskResponse{
		ErrorCode: 1,
		WorkerId:  workerID1,
	}}, nil)

	err = master.CreateWorker(ctx, workerTypePlaceholder, &dummyConfig{param: 1})
	require.NoError(t, err)

	master.On("OnWorkerDispatched", mock.AnythingOfType("*lib.workerHandleImpl"), nil).Return(nil)
	<-master.dispatchedWorkers
}
