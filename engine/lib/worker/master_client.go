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

	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/lib/config"
	"github.com/pingcap/tiflow/engine/lib/metadata"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

// MasterClient is used by the BaseWorker to communicate with
type MasterClient struct {
	// infoLock protects master's information,
	// as master can fail over and so the information can change.
	infoLock    sync.RWMutex
	masterNode  p2p.NodeID
	masterEpoch libModel.Epoch

	lastMasterAckedPingTime atomic.Duration

	// masterSideClosed records whether the master
	// has marked us as closed
	masterSideClosed atomic.Bool

	// Immutable fields
	workerID         libModel.WorkerID
	masterID         libModel.MasterID
	timeoutConfig    config.TimeoutConfig
	messageSender    p2p.MessageSender
	frameMetaClient  pkgOrm.Client
	onMasterFailOver func() error
}

// NewMasterClient creates a new MasterClient.
func NewMasterClient(
	masterID libModel.MasterID,
	workerID libModel.WorkerID,
	messageRouter p2p.MessageSender,
	metaCli pkgOrm.Client,
	initTime clock.MonotonicTime,
) *MasterClient {
	return &MasterClient{
		masterID:                masterID,
		workerID:                workerID,
		messageSender:           messageRouter,
		frameMetaClient:         metaCli,
		lastMasterAckedPingTime: *atomic.NewDuration(time.Duration(initTime)),
		timeoutConfig:           config.DefaultTimeoutConfig(),
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
		metaClient := metadata.NewMasterMetadataClient(m.masterID, m.frameMetaClient)
		masterMeta, err := metaClient.Load(ctx)
		if err != nil {
			errCh <- err
		}

		m.putMasterInfo(masterMeta.NodeID, masterMeta.Epoch)
	}()
	return errCh
}

func (m *MasterClient) putMasterInfo(nodeID p2p.NodeID, epoch libModel.Epoch) {
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

func (m *MasterClient) getMasterInfo() (p2p.NodeID, libModel.Epoch) {
	m.infoLock.RLock()
	defer m.infoLock.RUnlock()

	return m.masterNode, m.masterEpoch
}

// MasterID returns the masterID.
func (m *MasterClient) MasterID() libModel.MasterID {
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
func (m *MasterClient) Epoch() libModel.Epoch {
	_, epoch := m.getMasterInfo()
	return epoch
}

// HandleHeartbeat handles heartbeat messages received from the master.
func (m *MasterClient) HandleHeartbeat(sender p2p.NodeID, msg *libModel.HeartbeatPongMessage) {
	_, epoch := m.getMasterInfo()

	if msg.Epoch < epoch {
		log.L().Info("epoch does not match, ignore stale heartbeat",
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
		m.masterSideClosed.Store(true)
	}
	m.lastMasterAckedPingTime.Store(time.Duration(msg.SendTime))
}

// CheckMasterTimeout checks whether the master has timed out, i.e. we have lost
// contact with the master for a while.
func (m *MasterClient) CheckMasterTimeout(clk clock.Clock) (ok bool, err error) {
	lastMasterAckedPingTime := clock.MonotonicTime(m.lastMasterAckedPingTime.Load())

	sinceLastAcked := clk.Mono().Sub(lastMasterAckedPingTime)
	if sinceLastAcked <= 2*m.timeoutConfig.WorkerHeartbeatInterval {
		return true, nil
	}

	if sinceLastAcked > 2*m.timeoutConfig.WorkerHeartbeatInterval &&
		sinceLastAcked < m.timeoutConfig.WorkerTimeoutDuration {

		// We ignore the error here
		_ = m.asyncReloadMasterInfo(context.Background())
	}

	return false, nil
}

// SendHeartBeat sends a heartbeat to the master.
func (m *MasterClient) SendHeartBeat(ctx context.Context, clock clock.Clock, isFinished bool) error {
	nodeID, epoch := m.getMasterInfo()
	// We use the monotonic time because we would like to serialize a local timestamp.
	// The timestamp will be returned in a PONG for time-out check, so we need
	// the timestamp to be a local monotonic timestamp, which is not exposed by the
	// standard library `time`.
	sendTime := clock.Mono()
	heartbeatMsg := &libModel.HeartbeatPingMessage{
		SendTime:     sendTime,
		FromWorkerID: m.workerID,
		Epoch:        epoch,
		IsFinished:   isFinished,
	}

	log.L().Debug("sending heartbeat", zap.String("worker", m.workerID))
	ok, err := m.messageSender.SendToNode(ctx, nodeID, libModel.HeartbeatPingTopic(m.masterID), heartbeatMsg)
	if err != nil {
		return errors.Trace(err)
	}
	log.L().Info("sending heartbeat success", zap.String("worker", m.workerID),
		zap.String("master-id", m.masterID))
	if !ok {
		// Reloads master info asynchronously.
		_ = m.asyncReloadMasterInfo(context.Background())
	}
	return nil
}

// IsMasterSideClosed returns whether the master has marked the worker as closed.
// It is used when the worker initiates an exit with an error, but the network
// is fine.
func (m *MasterClient) IsMasterSideClosed() bool {
	return m.masterSideClosed.Load()
}

// used in unit test only
func (m *MasterClient) getLastMasterAckedPingTime() clock.MonotonicTime {
	return clock.MonotonicTime(m.lastMasterAckedPingTime.Load())
}
