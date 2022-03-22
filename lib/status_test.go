package lib

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/workerpool"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/clock"
	mockkv "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
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
	metaClient := mockkv.NewMetaMock()

	masterClient := newMasterClient(masterName, workerID1, msgSender, metaClient, clock.MonoNow(), func() error {
		return nil
	})

	putMasterMeta(ctx, t, metaClient, &MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: MasterStatusInit,
	})
	err := masterClient.InitMasterInfoFromMeta(ctx)
	require.NoError(t, err)

	workerMetaClient := NewWorkerMetadataClient(masterName, metaClient)
	sender := NewStatusSender(
		workerID1,
		masterClient,
		workerMetaClient,
		msgSender,
		pool,
	)

	err = sender.Tick(ctx)
	require.NoError(t, err)

	err = sender.AsyncSendStatus(ctx, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message",
		ExtBytes:     fastMarshalDummyStatus(t, 5),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		msg, ok := msgSender.TryPop(masterNodeName, WorkerStatusUpdatedTopic(masterName))
		if !ok {
			return false
		}
		require.Equal(t, &WorkerStatusUpdatedMessage{FromWorkerID: workerID1, Epoch: 1}, msg)
		return true
	}, 2*time.Second, 10*time.Millisecond)

	// simulate an RPC congestion
	msgSender.SetBlocked(true)

	err = sender.AsyncSendStatus(ctx, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message1",
		ExtBytes:     fastMarshalDummyStatus(t, 5),
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
		msg, ok := msgSender.TryPop(masterNodeName, WorkerStatusUpdatedTopic(masterName))
		if !ok {
			return false
		}
		require.Equal(t, &WorkerStatusUpdatedMessage{FromWorkerID: workerID1, Epoch: 1}, msg)
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

	metaClient := mockkv.NewMetaMock()
	workerMetaClient := NewWorkerMetadataClient(masterName, metaClient)
	mockMsgHandlerManager := p2p.NewMockMessageHandlerManager()
	mockClock := clock.NewMock()
	mockClock.Set(time.Now())

	err := workerMetaClient.Store(
		ctx,
		workerID1,
		&WorkerStatus{
			Code:         WorkerStatusInit,
			ErrorMessage: "test message",
			ExtBytes:     fastMarshalDummyStatus(t, 5),
		},
	)
	require.NoError(t, err)

	receiver := NewStatusReceiver(workerID1, workerMetaClient, mockMsgHandlerManager, 1, pool, mockClock)

	err = receiver.Init(ctx)
	require.NoError(t, err)

	require.Equal(t, WorkerStatus{
		Code:         WorkerStatusInit,
		ErrorMessage: "test message",
		ExtBytes:     fastMarshalDummyStatus(t, 5),
	}, receiver.Status())

	err = workerMetaClient.Store(ctx, workerID1, &WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message1",
		ExtBytes:     fastMarshalDummyStatus(t, 5),
	})
	require.NoError(t, err)

	receiver.OnNotification(&WorkerStatusUpdatedMessage{FromWorkerID: workerID1, Epoch: 1})

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
		ExtBytes:     fastMarshalDummyStatus(t, 5),
	}, status)

	err = workerMetaClient.Store(ctx, workerID1, &WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message2",
		ExtBytes:     fastMarshalDummyStatus(t, 6),
	})
	require.NoError(t, err)

	err = receiver.Tick(ctx)
	require.NoError(t, err)

	status = receiver.Status()
	require.NotEqual(t, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message2",
		ExtBytes:     fastMarshalDummyStatus(t, 6),
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
		ExtBytes:     fastMarshalDummyStatus(t, 6),
	}, status)

	cancel()
	wg.Wait()
}

func TestStatusSenderSafeSend(t *testing.T) {
	t.Parallel()

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
	metaClient := mockkv.NewMetaMock()

	masterClient := newMasterClient(masterName, workerID1, msgSender, metaClient, clock.MonoNow(), func() error {
		return nil
	})

	putMasterMeta(ctx, t, metaClient, &MasterMetaKVData{
		ID:         masterName,
		NodeID:     masterNodeName,
		Epoch:      1,
		StatusCode: MasterStatusInit,
	})
	err := masterClient.InitMasterInfoFromMeta(ctx)
	require.NoError(t, err)

	workerMetaClient := NewWorkerMetadataClient(masterName, metaClient)
	sender := NewStatusSender(
		workerID1,
		masterClient,
		workerMetaClient,
		msgSender,
		pool,
	)

	err = sender.Tick(ctx)
	require.NoError(t, err)

	// simulate an RPC congestion
	msgSender.SetBlocked(true)
	err = sender.AsyncSendStatus(ctx, WorkerStatus{
		Code:         WorkerStatusNormal,
		ErrorMessage: "test message1",
		ExtBytes:     fastMarshalDummyStatus(t, 5),
	})
	require.NoError(t, err)

	ctx1, cancel1 := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel1()
	// SafeSendStatus would block if there exists pending message
	err = sender.SafeSendStatus(ctx1, WorkerStatus{Code: WorkerStatusFinished})
	require.Error(t, err, context.Canceled)

	// Call tick to send the pending message
	msgSender.SetBlocked(false)
	err = sender.Tick(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		msg, ok := msgSender.TryPop(masterNodeName, WorkerStatusUpdatedTopic(masterName))
		if !ok {
			return false
		}
		require.Equal(t, &WorkerStatusUpdatedMessage{FromWorkerID: workerID1, Epoch: 1}, msg)
		return true
	}, 2*time.Second, 10*time.Millisecond)

	// SafeSendStatus fires successfully after pending message is sent
	err = sender.SafeSendStatus(ctx, WorkerStatus{Code: WorkerStatusFinished})
	require.NoError(t, err)
	err = sender.Tick(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		msg, ok := msgSender.TryPop(masterNodeName, WorkerStatusUpdatedTopic(masterName))
		if !ok {
			return false
		}
		require.Equal(t, &WorkerStatusUpdatedMessage{FromWorkerID: workerID1, Epoch: 1}, msg)
		return true
	}, 2*time.Second, 10*time.Millisecond)
}
