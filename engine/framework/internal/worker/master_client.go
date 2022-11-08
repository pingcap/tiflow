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

package worker

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework/config"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	reloadMasterInfoTimeout        = 10 * time.Second
	workerExitWaitForMasterTimeout = time.Second * 15
)

type masterClientCloseState = int32

const (
	masterClientNormal = masterClientCloseState(iota + 1)
	masterClientClosing
	masterClientClosed
)

// MasterClient is used by the BaseWorker to communicate with
type MasterClient struct {
	// infoLock protects master's information,
	// as master can fail over and so the information can change.
	infoLock    sync.RWMutex
	masterNode  p2p.NodeID
	masterEpoch frameModel.Epoch
	workerEpoch frameModel.Epoch

	lastMasterAckedPingTime atomic.Duration

	closeState atomic.Int32
	closeCh    chan struct{}

	// Immutable fields
	workerID        frameModel.WorkerID
	masterID        frameModel.MasterID
	timeoutConfig   config.TimeoutConfig
	messageSender   p2p.MessageSender
	frameMetaClient pkgOrm.Client

	clk clock.Clock
}

// NewMasterClient creates a new MasterClient.
func NewMasterClient(
	masterID frameModel.MasterID,
	workerID frameModel.WorkerID,
	messageSender p2p.MessageSender,
	metaCli pkgOrm.Client,
	initTime clock.MonotonicTime,
	clk clock.Clock,
	workerEpoch frameModel.Epoch,
) *MasterClient {
	return &MasterClient{
		masterID:                masterID,
		workerID:                workerID,
		messageSender:           messageSender,
		frameMetaClient:         metaCli,
		lastMasterAckedPingTime: *atomic.NewDuration(time.Duration(initTime)),
		closeState:              *atomic.NewInt32(masterClientNormal),
		closeCh:                 make(chan struct{}),
		timeoutConfig:           config.DefaultTimeoutConfig(),
		clk:                     clk,
		workerEpoch:             workerEpoch,
	}
}

// InitMasterInfoFromMeta reads the meta store and tries to find where
// the master is.
func (m *MasterClient) InitMasterInfoFromMeta(ctx context.Context) error {
	metaClient := metadata.NewMasterMetadataClient(m.masterID, m.frameMetaClient)
	masterMeta, err := metaClient.Load(ctx)
	if err != nil {
		return err
	}

	m.putMasterInfo(masterMeta.NodeID, masterMeta.Epoch)
	return nil
}

// SyncRefreshMasterInfo reloads the master's info. It is useful if the caller
// anticipates a master failover to have happened.
func (m *MasterClient) SyncRefreshMasterInfo(ctx context.Context) error {
	errCh := m.asyncReloadMasterInfo(ctx)
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-errCh:
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MasterClient) asyncReloadMasterInfo(ctx context.Context) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)

		timeoutCtx, cancel := context.WithTimeout(ctx, reloadMasterInfoTimeout)
		defer cancel()

		metaClient := metadata.NewMasterMetadataClient(m.masterID, m.frameMetaClient)
		masterMeta, err := metaClient.Load(timeoutCtx)
		if err != nil {
			log.Warn("async reload master info failed", zap.Error(err))
			errCh <- err
			return
		}

		m.putMasterInfo(masterMeta.NodeID, masterMeta.Epoch)
	}()
	return errCh
}

func (m *MasterClient) putMasterInfo(nodeID p2p.NodeID, epoch frameModel.Epoch) {
	m.infoLock.Lock()
	defer m.infoLock.Unlock()

	if epoch < m.masterEpoch {
		return
	}

	if epoch >= m.masterEpoch {
		m.masterEpoch = epoch
		m.masterNode = nodeID
	}
}

func (m *MasterClient) getMasterInfo() (p2p.NodeID, frameModel.Epoch) {
	m.infoLock.RLock()
	defer m.infoLock.RUnlock()

	return m.masterNode, m.masterEpoch
}

// MasterID returns the masterID.
func (m *MasterClient) MasterID() frameModel.MasterID {
	// No need to lock here as masterID is immutable.
	return m.masterID
}

// MasterNode returns the node ID of the executor
// on which the master is being run.
func (m *MasterClient) MasterNode() p2p.NodeID {
	nodeID, _ := m.getMasterInfo()
	return nodeID
}

// Epoch returns the master epoch.
// Note that the epoch is increased when the master
// restarts.
func (m *MasterClient) Epoch() frameModel.Epoch {
	_, epoch := m.getMasterInfo()
	return epoch
}

// WorkerEpoch returns the worker epoch.
// This value is a constant value for the master client of every single worker
func (m *MasterClient) WorkerEpoch() frameModel.Epoch {
	return m.workerEpoch
}

// HandleHeartbeat handles heartbeat messages received from the master.
func (m *MasterClient) HandleHeartbeat(sender p2p.NodeID, msg *frameModel.HeartbeatPongMessage) {
	if msg.ToWorkerID != m.workerID {
		log.Warn("Received heartbeat for wrong workerID",
			zap.Any("msg", msg), zap.String("actual-worker-id", m.workerID))
		return
	}

	_, epoch := m.getMasterInfo()
	if msg.Epoch < epoch {
		log.Info("epoch does not match, ignore stale heartbeat",
			zap.Any("msg", msg),
			zap.Int64("master-epoch", epoch))
		return
	}

	if msg.Epoch > epoch {
		// We received a heartbeat from a restarted master, we need to record
		// its information.
		m.putMasterInfo(sender, msg.Epoch)
	}

	if msg.IsFinished {
		oldSt := m.closeState.Swap(masterClientClosed)
		if oldSt == masterClientNormal {
			// Jumping from Normal to Closed in unexpected
			log.Panic("unexpected master client close state",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", m.workerID))
		}
		if oldSt == masterClientClosing {
			close(m.closeCh)
		}
	}

	// worker may receive stale heartbeat pong message from job master, stale
	// message doesn't contribute to job master aliveness detection and even
	// leads to false positive.
	lastAckTime := m.lastMasterAckedPingTime.Load()
	if lastAckTime > time.Duration(msg.SendTime) {
		log.Info("received stale pong heartbeat",
			zap.Any("msg", msg), zap.Int64("lastAckTime", int64(lastAckTime)))
	} else {
		m.lastMasterAckedPingTime.Store(time.Duration(msg.SendTime))
	}
}

// CheckMasterTimeout checks whether the master has timed out, i.e. we have lost
// contact with the master for a while.
func (m *MasterClient) CheckMasterTimeout() (ok bool, err error) {
	lastMasterAckedPingTime := clock.MonotonicTime(m.lastMasterAckedPingTime.Load())

	sinceLastAcked := m.clk.Mono().Sub(lastMasterAckedPingTime)
	if sinceLastAcked <= 2*m.timeoutConfig.WorkerHeartbeatInterval {
		return true, nil
	}

	if sinceLastAcked > 2*m.timeoutConfig.WorkerHeartbeatInterval &&
		sinceLastAcked < m.timeoutConfig.WorkerTimeoutDuration {

		// We ignore the error here
		_ = m.asyncReloadMasterInfo(context.Background())
		return true, nil
	}

	return false, nil
}

// SendHeartBeat sends a heartbeat to the master.
func (m *MasterClient) SendHeartBeat(ctx context.Context) error {
	nodeID, epoch := m.getMasterInfo()
	// We use the monotonic time because we would like to serialize a local timestamp.
	// The timestamp will be returned in a PONG for time-out check, so we need
	// the timestamp to be a local monotonic timestamp, which is not exposed by the
	// standard library `time`.
	sendTime := m.clk.Mono()
	isFinished := m.closeState.Load() == masterClientClosing

	heartbeatMsg := &frameModel.HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: m.workerID,
		Epoch:        epoch,
		WorkerEpoch:  m.WorkerEpoch(),
		IsFinished:   isFinished,
	}

	log.Debug("sending heartbeat", zap.String("worker", m.workerID),
		zap.String("master-id", m.masterID),
		zap.Int64("epoch", epoch), zap.Int64("worker-epoch", heartbeatMsg.WorkerEpoch),
		zap.Int64("sendTime", int64(sendTime)))
	ok, err := m.messageSender.SendToNode(ctx, nodeID, frameModel.HeartbeatPingTopic(m.masterID), heartbeatMsg)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("sending heartbeat success", zap.String("worker", m.workerID),
		zap.String("master-id", m.masterID),
		zap.Int64("epoch", epoch), zap.Int64("worker-epoch", heartbeatMsg.WorkerEpoch),
		zap.Int64("sendTime", int64(sendTime)))
	if !ok {
		// Reloads master info asynchronously.
		// Not using `ctx` because the caller might cancel unexpectedly.
		_ = m.asyncReloadMasterInfo(context.Background())
	}
	return nil
}

// IsMasterSideClosed returns whether the master has marked the worker as closed.
// It is used when the worker initiates an exit with an error, but the network
// is fine.
func (m *MasterClient) IsMasterSideClosed() bool {
	return m.closeState.Load() == masterClientClosed
}

// WaitClosed marks the current worker as exiting, and
// blocks until the master has acknowledged the exit.
// The caller should make sure that no concurrent calls to
// WaitClosed happens.
func (m *MasterClient) WaitClosed(ctx context.Context) error {
	switch m.closeState.Load() {
	case masterClientNormal:
		if !m.closeState.CAS(masterClientNormal, masterClientClosing) {
			log.Panic("Unexpected close state in master client, race?",
				zap.String("master-id", m.masterID),
				zap.String("worker-id", m.workerID))
		}
	case masterClientClosing:
		break // breaks switch
	case masterClientClosed:
		return nil
	}

	timer := m.clk.Timer(workerExitWaitForMasterTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-timer.C:
		return errors.Trace(context.DeadlineExceeded)
	case <-m.closeCh:
	}

	if m.closeState.Load() != masterClientClosed {
		log.Panic("Unexpected close state in master client, bug?",
			zap.String("master-id", m.masterID),
			zap.String("worker-id", m.workerID))
	}

	return nil
}

// used in unit test only
func (m *MasterClient) getLastMasterAckedPingTime() clock.MonotonicTime {
	return clock.MonotonicTime(m.lastMasterAckedPingTime.Load())
}
