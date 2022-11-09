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

package master

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework/config"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/statusutil"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type workerManageTestSuite struct {
	manager       *WorkerManager
	masterNode    p2p.NodeID
	meta          pkgOrm.Client
	messageSender p2p.MessageSender
	clock         *clock.Mock

	events map[frameModel.WorkerID]*masterEvent
}

func (s *workerManageTestSuite) AdvanceClockBy(duration time.Duration) {
	s.clock.Add(duration)
}

func (s *workerManageTestSuite) SimulateHeartbeat(
	workerID frameModel.WorkerID,
	epoch frameModel.Epoch, workerEpoch frameModel.Epoch,
	node p2p.NodeID, isFinished bool,
) {
	s.manager.HandleHeartbeat(&frameModel.HeartbeatPingMessage{
		SendTime:     s.clock.Mono(),
		FromWorkerID: workerID,
		Epoch:        epoch,
		WorkerEpoch:  workerEpoch,
		IsFinished:   isFinished,
	}, node)
}

func (s *workerManageTestSuite) SimulateWorkerUpdateStatus(
	workerID frameModel.WorkerID, status *frameModel.WorkerStatus, epoch frameModel.Epoch,
) error {
	err := s.meta.UpsertWorker(context.Background(), status)
	if err != nil {
		return err
	}

	s.manager.OnWorkerStatusUpdateMessage(&statusutil.WorkerStatusMessage{
		Worker:      workerID,
		MasterEpoch: epoch,
		Status:      status,
	})
	return nil
}

func (s *workerManageTestSuite) PutMeta(workerID frameModel.WorkerID, status *frameModel.WorkerStatus) error {
	status.JobID = "master-1"
	status.ID = workerID
	return s.meta.UpsertWorker(context.Background(), status)
}

func (s *workerManageTestSuite) onWorkerOnline(ctx context.Context, handle WorkerHandle) error {
	if event, exists := s.events[handle.ID()]; exists {
		log.Warn("found unexpected event", zap.Any("event", event))
		return errors.New("unexpected event already exists")
	}
	s.events[handle.ID()] = &masterEvent{
		Tp:     workerOnlineEvent,
		Handle: handle,
	}
	return nil
}

func (s *workerManageTestSuite) onWorkerOffline(ctx context.Context, handle WorkerHandle, err error) error {
	if event, exists := s.events[handle.ID()]; exists {
		log.Warn("found unexpected event", zap.Any("event", event))
		return errors.New("unexpected event already exists")
	}
	s.events[handle.ID()] = &masterEvent{
		Tp:     workerOfflineEvent,
		Handle: handle,
		Err:    err,
	}
	return nil
}

func (s *workerManageTestSuite) onWorkerStatusUpdated(ctx context.Context, handle WorkerHandle) error {
	if event, exists := s.events[handle.ID()]; exists {
		log.Warn("found unexpected event", zap.Any("event", event))
		return errors.New("unexpected event already exists")
	}
	s.events[handle.ID()] = &masterEvent{
		Tp:     workerStatusUpdatedEvent,
		Handle: handle,
	}
	return nil
}

func (s *workerManageTestSuite) onWorkerDispatched(ctx context.Context, handle WorkerHandle, err error) error {
	if event, exists := s.events[handle.ID()]; exists {
		log.Warn("found unexpected event", zap.Any("event", event))
		return errors.New("unexpected event already exists")
	}
	s.events[handle.ID()] = &masterEvent{
		Tp:     workerDispatchFailedEvent,
		Handle: handle,
		Err:    err,
	}
	return nil
}

func (s *workerManageTestSuite) WaitForEvent(t *testing.T, workerID frameModel.WorkerID) *masterEvent {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rl := rate.NewLimiter(rate.Every(10*time.Millisecond), 1)

	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatalf("waitForEventTimed out, workerID: %s", workerID)
		default:
		}

		// The Tick should return very quickly.
		tickCtx, cancel := context.WithTimeout(timeoutCtx, 100*time.Millisecond)
		err := s.manager.Tick(tickCtx)
		cancel()
		require.NoError(t, err)

		event, exists := s.events[workerID]
		if !exists {
			err := rl.Wait(timeoutCtx)
			require.NoError(t, err)

			s.AdvanceClockBy(1 * time.Second)
			continue
		}

		require.Equal(t, workerID, event.Handle.ID())
		delete(s.events, workerID)
		return event
	}
}

func (s *workerManageTestSuite) AssertNoEvents(t *testing.T, workerID frameModel.WorkerID, waitFor time.Duration) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()

	rl := rate.NewLimiter(rate.Every(10*time.Millisecond), 1)

	for {
		select {
		case <-timeoutCtx.Done():
			return
		default:
		}

		// The Tick should return very quickly.
		tickCtx, cancel := context.WithTimeout(timeoutCtx, 100*time.Millisecond)
		err := s.manager.Tick(tickCtx)
		cancel()
		if err != nil {
			if context.DeadlineExceeded == errors.Cause(err) {
				return
			}
			require.NoError(t, err)
		}

		_, exists := s.events[workerID]
		require.False(t, exists)

		_ = rl.Wait(timeoutCtx)
	}
}

func (s *workerManageTestSuite) Close() {
	s.manager.Close()
	// Prevents SQL connection leak.
	_ = s.meta.Close()
}

func NewWorkerManageTestSuite(isInit bool) *workerManageTestSuite {
	cli, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}
	ret := &workerManageTestSuite{
		meta:          cli,
		masterNode:    "executor-0",
		messageSender: p2p.NewMockMessageSender(),
		clock:         clock.NewMock(),
		events:        make(map[frameModel.WorkerID]*masterEvent),
	}

	manager := NewWorkerManager(
		"master-1",
		1,
		ret.meta,
		ret.messageSender,
		ret.onWorkerOnline,
		ret.onWorkerOffline,
		ret.onWorkerStatusUpdated,
		ret.onWorkerDispatched,
		isInit,
		config.DefaultTimeoutConfig(),
		ret.clock)
	ret.manager = manager
	return ret
}

func TestCreateWorkerAndWorkerOnline(t *testing.T) {
	t.Parallel()

	suite := NewWorkerManageTestSuite(true)
	wEpoch := int64(2)
	suite.manager.BeforeStartingWorker("worker-1", "executor-1", wEpoch)

	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)
	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)
	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOnlineEvent, event.Tp)
	suite.Close()
}

func TestCreateWorkerAndWorkerTimesOut(t *testing.T) {
	t.Parallel()

	suite := NewWorkerManageTestSuite(true)
	suite.manager.BeforeStartingWorker("worker-1", "executor-1", 2)
	suite.AdvanceClockBy(30 * time.Second)
	suite.AdvanceClockBy(30 * time.Second)
	suite.AdvanceClockBy(30 * time.Second)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOfflineEvent, event.Tp)
	require.NotNil(t, event.Handle.GetTombstone())

	suite.AssertNoEvents(t, "worker-1", 500*time.Millisecond)
	suite.Close()
}

func TestCreateWorkerPredispatchFailed(t *testing.T) {
	t.Parallel()

	suite := NewWorkerManageTestSuite(true)
	suite.manager.AbortCreatingWorker("worker-1", errors.New("injected error"))

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerDispatchFailedEvent, event.Tp)
	require.NotNil(t, event.Handle.GetTombstone())
	require.Error(t, event.Err)
	require.Regexp(t, ".*injected error.*", event.Err)

	suite.AssertNoEvents(t, "worker-1", 500*time.Millisecond)
	suite.Close()
}

func TestCreateWorkerAndWorkerStatusUpdatedAndTimesOut(t *testing.T) {
	t.Parallel()

	suite := NewWorkerManageTestSuite(true)
	wEpoch := int64(2)
	suite.manager.BeforeStartingWorker("worker-1", "executor-1", wEpoch)

	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOnlineEvent, event.Tp)

	err := suite.SimulateWorkerUpdateStatus("worker-1", &frameModel.WorkerStatus{
		State: frameModel.WorkerStateFinished,
	}, 1)
	require.NoError(t, err)

	event = suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerStatusUpdatedEvent, event.Tp)
	require.Equal(t, frameModel.WorkerStateFinished, event.Handle.Status().State)

	suite.AdvanceClockBy(30 * time.Second)
	event = suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOfflineEvent, event.Tp)
	require.NotNil(t, event.Handle.GetTombstone())
	require.True(t, errors.Is(event.Err, errors.ErrWorkerFinish))

	suite.Close()
}

func TestRecoverAfterFailover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	suite := NewWorkerManageTestSuite(false)
	err := suite.PutMeta("worker-1", &frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
		Epoch: 11,
	})
	require.NoError(t, err)
	err = suite.PutMeta("worker-2", &frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
		Epoch: 12,
	})
	require.NoError(t, err)
	err = suite.PutMeta("worker-3", &frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
		Epoch: 13,
	})
	require.NoError(t, err)
	err = suite.PutMeta("worker-4", &frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
		Epoch: 14,
	})
	require.NoError(t, err)

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		err := suite.manager.InitAfterRecover(ctx)
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		suite.SimulateHeartbeat("worker-1", 1, 11, "executor-1", false)
		suite.SimulateHeartbeat("worker-2", 1, 12, "executor-2", false)
		suite.SimulateHeartbeat("worker-3", 1, 13, "executor-3", false)

		select {
		case <-doneCh:
			return true
		default:
		}
		suite.AdvanceClockBy(1 * time.Second)
		return false
	}, 5*time.Second, 10*time.Millisecond)

	require.True(t, suite.manager.IsInitialized())
	require.Len(t, suite.manager.GetWorkers(), 4)
	require.Contains(t, suite.manager.GetWorkers(), "worker-1")
	require.Contains(t, suite.manager.GetWorkers(), "worker-2")
	require.Contains(t, suite.manager.GetWorkers(), "worker-3")
	require.Contains(t, suite.manager.GetWorkers(), "worker-4")
	require.Nil(t, suite.manager.GetWorkers()["worker-1"].GetTombstone())
	require.Nil(t, suite.manager.GetWorkers()["worker-2"].GetTombstone())
	require.Nil(t, suite.manager.GetWorkers()["worker-3"].GetTombstone())
	require.NotNil(t, suite.manager.GetWorkers()["worker-4"].GetTombstone())
	suite.Close()
}

func TestRecoverAfterFailoverFast(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	suite := NewWorkerManageTestSuite(false)
	wEpoch := int64(100)
	err := suite.PutMeta("worker-1", &frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
		Epoch: wEpoch,
	})
	require.NoError(t, err)

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		err := suite.manager.InitAfterRecover(ctx)
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)
		select {
		case <-doneCh:
			return true
		default:
		}
		return false
	}, 1*time.Second, 10*time.Millisecond)

	require.True(t, suite.manager.IsInitialized())
	require.Len(t, suite.manager.GetWorkers(), 1)
	require.Contains(t, suite.manager.GetWorkers(), "worker-1")
	suite.Close()
}

func TestRecoverWithNoWorker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	suite := NewWorkerManageTestSuite(false)

	// Since there is no worker info in the metastore,
	// recovering should be very fast.
	// Since we are using a mock clock, and we are NOT advancing it,
	// InitAfterRecover returning at all would indicate a successful test.
	err := suite.manager.InitAfterRecover(ctx)
	require.NoError(t, err)

	suite.Close()
}

func TestCleanTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	suite := NewWorkerManageTestSuite(true)
	suite.manager.BeforeStartingWorker("worker-1", "executor-1", 2)
	suite.AdvanceClockBy(30 * time.Second)
	suite.AdvanceClockBy(30 * time.Second)
	suite.AdvanceClockBy(30 * time.Second)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOfflineEvent, event.Tp)
	require.NotNil(t, event.Handle.GetTombstone())
	err := event.Handle.GetTombstone().CleanTombstone(ctx)
	require.NoError(t, err)

	workerMetaClient := metadata.NewWorkerStatusClient("master-1", suite.meta)
	_, err = workerMetaClient.Load(ctx, "worker-1")
	// Asserts that the meta for the worker is indeed deleted.
	require.Error(t, err)
	require.Regexp(t, ".*ErrMetaEntryNotFound", err)

	// CleanTombstone should be idempotent for robustness.
	err = event.Handle.GetTombstone().CleanTombstone(ctx)
	require.NoError(t, err)

	// Recreating a worker with the same name should work fine.
	suite.manager.BeforeStartingWorker("worker-1", "executor-1", 10)

	suite.Close()
}

func TestWorkerGracefulExit(t *testing.T) {
	t.Parallel()

	suite := NewWorkerManageTestSuite(true)
	wEpoch := int64(2)
	suite.manager.BeforeStartingWorker("worker-1", "executor-1", wEpoch)

	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)
	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)
	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOnlineEvent, event.Tp)

	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", true)
	event = suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOfflineEvent, event.Tp)

	suite.Close()
}

func TestWorkerGracefulExitOnFirstHeartbeat(t *testing.T) {
	t.Parallel()

	suite := NewWorkerManageTestSuite(true)
	wEpoch := int64(2)
	suite.manager.BeforeStartingWorker("worker-1", "executor-1", wEpoch)

	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", true)

	// Now we expect there to be both workerOnlineEvent and workerOfflineEvent,
	// in that order.
	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOnlineEvent, event.Tp)
	event = suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOfflineEvent, event.Tp)

	suite.Close()
}

func TestWorkerGracefulExitAfterFailover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	suite := NewWorkerManageTestSuite(false)
	wEpoch := int64(2)
	err := suite.PutMeta("worker-1", &frameModel.WorkerStatus{
		State: frameModel.WorkerStateNormal,
		Epoch: wEpoch,
	})
	require.NoError(t, err)

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		err := suite.manager.InitAfterRecover(ctx)
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", true)
		select {
		case <-doneCh:
			return true
		default:
		}
		suite.AdvanceClockBy(1 * time.Second)
		return false
	}, 1*time.Second, 10*time.Millisecond)

	require.True(t, suite.manager.IsInitialized())
	require.Len(t, suite.manager.GetWorkers(), 1)
	require.Contains(t, suite.manager.GetWorkers(), "worker-1")
	require.NotNil(t, suite.manager.GetWorkers()["worker-1"].GetTombstone())
	suite.Close()
}

func TestWorkerSendsStaleHeartbeat(t *testing.T) {
	t.Parallel()

	suite := NewWorkerManageTestSuite(true)
	wEpoch := int64(2)
	suite.manager.BeforeStartingWorker("worker-1", "executor-1", wEpoch)

	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)
	suite.SimulateHeartbeat("worker-1", 1, wEpoch, "executor-1", false)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOnlineEvent, event.Tp)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * 20):
				suite.SimulateHeartbeat("worker-1", 1, wEpoch-1, "executor-1", false)
			}
		}
	}()

	event = suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOfflineEvent, event.Tp)

	suite.Close()
	cancel()
	wg.Wait()
}
