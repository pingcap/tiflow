package lib

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

func TestStatusSender(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool := workerpool.NewDefaultAsyncPool(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	msgSender := p2p.NewMockMessageSender()
	metaClient := metadata.NewMetaMock()

	masterClient := newMasterClient(masterName, workerID1, msgSender, metaClient, clock.MonoNow(), func() error {
		return nil
	})

	putMasterMeta(ctx, t, metaClient, &MasterMetaKVData{
		ID:          masterName,
		NodeID:      masterNodeName,
		Epoch:       1,
		Initialized: true,
	})
	err := masterClient.InitMasterInfoFromMeta(ctx)
	require.NoError(t, err)

	workerMetaClient := NewWorkerMetadataClient(masterName, workerID1, metaClient, &dummyStatus{})
	sender := NewStatusSender(
		masterClient,
		workerMetaClient,
		msgSender,
		pool,
	)

	err = sender.Tick(ctx)
	require.NoError(t, err)

	err = sender.SendStatus(ctx, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message",
		Ext:          dummyStatus{Val: 5},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		msg, ok := msgSender.TryPop(masterNodeName, workerStatusUpdatedTopic(masterName, workerID1))
		if !ok {
			return false
		}
		require.Equal(t, &workerStatusUpdatedMessage{Epoch: 1}, msg)
		return true
	}, 2*time.Second, 10*time.Millisecond)

	// simulate an RPC congestion
	msgSender.SetBlocked(true)

	err = sender.SendStatus(ctx, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message1",
		Ext:          dummyStatus{Val: 5},
	})
	require.NoError(t, err)

	barrier := make(chan struct{})
	err = pool.Go(ctx, func() {
		close(barrier)
	})
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		require.Fail(t, "context timed out")
	case <-barrier:
	}

	// Tick 1
	err = sender.Tick(ctx)
	require.NoError(t, err)

	// Tick 2
	err = sender.Tick(ctx)
	require.NoError(t, err)

	// remove RPC congestion
	msgSender.SetBlocked(false)

	// Tick 3
	err = sender.Tick(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		msg, ok := msgSender.TryPop(masterNodeName, workerStatusUpdatedTopic(masterName, workerID1))
		if !ok {
			return false
		}
		require.Equal(t, &workerStatusUpdatedMessage{Epoch: 1}, msg)
		return true
	}, 2*time.Second, 10*time.Millisecond)

	// TODO (zixiong) add coverage for error cases

	cancel()
	wg.Wait()
}

func TestStatusReceiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool := workerpool.NewDefaultAsyncPool(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = pool.Run(ctx)
	}()

	metaClient := metadata.NewMetaMock()
	workerMetaClient := NewWorkerMetadataClient(masterName, workerID1, metaClient, &dummyStatus{})
	mockMsgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockClock := clock.NewMock()
	mockClock.Set(time.Now())

	err := workerMetaClient.Store(ctx, &WorkerStatus{
		Code:         WorkerStatusInit,
		ErrorMessage: "test message",
		ExtBytes:     nil,
		Ext:          &dummyStatus{Val: 4},
	})
	require.NoError(t, err)

	receiver := NewStatusReceiver(workerMetaClient, mockMsgHandlerManager, 1, pool, mockClock)

	err = receiver.Init(ctx)
	require.NoError(t, err)

	require.Equal(t, WorkerStatus{
		Code:         WorkerStatusInit,
		ErrorMessage: "test message",
		ExtBytes:     nil,
		Ext:          &dummyStatus{Val: 4},
	}, receiver.Status())

	err = workerMetaClient.Store(ctx, &WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message1",
		ExtBytes:     nil,
		Ext:          &dummyStatus{Val: 5},
	})
	require.NoError(t, err)

	err = mockMsgHandlerManager.InvokeHandler(
		t,
		StatusUpdateTopic(masterName, workerID1),
		executorNodeID1,
		&workerStatusUpdatedMessage{Epoch: 1})
	require.NoError(t, err)

	err = receiver.Tick(ctx)
	require.NoError(t, err)

	barrier := make(chan struct{})
	err = pool.Go(ctx, func() {
		close(barrier)
	})
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		require.Fail(t, "context timed out")
	case <-barrier:
	}

	status := receiver.Status()
	require.Equal(t, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message1",
		ExtBytes:     nil,
		Ext:          &dummyStatus{Val: 5},
	}, status)

	err = workerMetaClient.Store(ctx, &WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message2",
		ExtBytes:     nil,
		Ext:          &dummyStatus{Val: 6},
	})
	require.NoError(t, err)

	err = receiver.Tick(ctx)
	require.NoError(t, err)

	status = receiver.Status()
	require.NotEqual(t, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message2",
		ExtBytes:     nil,
		Ext:          &dummyStatus{Val: 6},
	}, status)

	mockClock.Add(11 * time.Second)

	err = receiver.Tick(ctx)
	require.NoError(t, err)

	barrier = make(chan struct{})
	err = pool.Go(ctx, func() {
		close(barrier)
	})
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		require.Fail(t, "context timed out")
	case <-barrier:
	}

	status = receiver.Status()
	require.Equal(t, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message2",
		ExtBytes:     nil,
		Ext:          &dummyStatus{Val: 6},
	}, status)

	cancel()
	wg.Wait()
}
