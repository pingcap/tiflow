// Copyright 2021 PingCAP, Inc.
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

package processor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/tiflow/cdc/model"
	pscheduler "github.com/pingcap/tiflow/cdc/scheduler"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
)

const (
	// The purpose of these tests is to test the correctness
	// of the agent alone. So one node for the agent and the
	// other for the scheduler is enough.
	agentTestMockNodeNum = 2

	ownerCaptureID     = "capture-0"
	processorCaptureID = "capture-1"
)

// agentTestSuite is a test suite for the agent (the component of the processor).
// NOTE these tests are actually an integration test for the agent and the p2p package.
// TODO add a real unit test with mock components for the agent alone,
// which might require refactoring some existing components.
type agentTestSuite struct {
	cluster      *p2p.MockCluster
	etcdClient   *clientv3.Client
	etcdKVClient *mockEtcdKVClient

	tableExecutor      *pscheduler.MockTableExecutor
	dispatchResponseCh chan *model.DispatchTableResponseMessage
	syncCh             chan *model.SyncMessage
	checkpointCh       chan *model.CheckpointMessage

	ownerMessageClient *p2p.MessageClient

	ctx    context.Context
	cdcCtx cdcContext.Context
	cancel context.CancelFunc

	blockSyncMu   sync.Mutex
	blockSyncCond *sync.Cond
	blockSync     bool
}

func newAgentTestSuite(t *testing.T) *agentTestSuite {
	ctx, cancel := context.WithCancel(context.Background())
	etcdCli, KVCli := newMockEtcdClientForAgentTests(ctx)

	cluster := p2p.NewMockCluster(t, agentTestMockNodeNum)
	ownerMessageServer := cluster.Nodes[ownerCaptureID].Server

	ownerMessageClient := cluster.Nodes[ownerCaptureID].Router.GetClient(processorCaptureID)
	require.NotNil(t, ownerMessageClient)

	ret := &agentTestSuite{
		cluster:      cluster,
		etcdClient:   etcdCli,
		etcdKVClient: KVCli,

		// The channel sizes 1024 should be more than sufficient for these tests.
		// Full channels will result in panics to make the cases fail.
		dispatchResponseCh: make(chan *model.DispatchTableResponseMessage, 1024),
		syncCh:             make(chan *model.SyncMessage, 1024),
		checkpointCh:       make(chan *model.CheckpointMessage, 1024),

		ownerMessageClient: ownerMessageClient,

		ctx:    ctx,
		cancel: cancel,
	}

	_, err := ownerMessageServer.SyncAddHandler(ctx, model.DispatchTableResponseTopic("cf-1"),
		&model.DispatchTableResponseMessage{},
		func(senderID string, msg interface{}) error {
			require.Equal(t, processorCaptureID, senderID)
			require.IsType(t, &model.DispatchTableResponseMessage{}, msg)
			select {
			case ret.dispatchResponseCh <- msg.(*model.DispatchTableResponseMessage):
			default:
				require.FailNow(t, "full channel")
			}
			return nil
		},
	)
	require.NoError(t, err)

	_, err = ownerMessageServer.SyncAddHandler(ctx, model.SyncTopic("cf-1"),
		&model.SyncMessage{},
		func(senderID string, msg interface{}) error {
			ret.blockSyncMu.Lock()
			for ret.blockSync {
				ret.blockSyncCond.Wait()
			}
			ret.blockSyncMu.Unlock()

			require.Equal(t, processorCaptureID, senderID)
			require.IsType(t, &model.SyncMessage{}, msg)

			select {
			case ret.syncCh <- msg.(*model.SyncMessage):
			default:
				require.FailNow(t, "full channel")
			}
			return nil
		},
	)
	require.NoError(t, err)

	_, err = ownerMessageServer.SyncAddHandler(ctx, model.CheckpointTopic("cf-1"),
		&model.CheckpointMessage{},
		func(senderID string, msg interface{}) error {
			require.Equal(t, processorCaptureID, senderID)
			require.IsType(t, &model.CheckpointMessage{}, msg)

			select {
			case ret.checkpointCh <- msg.(*model.CheckpointMessage):
			default:
				require.FailNow(t, "full channel")
			}
			return nil
		},
	)
	require.NoError(t, err)
	ret.blockSyncCond = sync.NewCond(&ret.blockSyncMu)
	return ret
}

func (s *agentTestSuite) CreateAgent(t *testing.T) (*agentImpl, error) {
	cdcEtcdClient := etcd.NewCDCEtcdClient(s.ctx, s.etcdClient)
	messageServer := s.cluster.Nodes["capture-1"].Server
	messageRouter := s.cluster.Nodes["capture-1"].Router
	s.tableExecutor = pscheduler.NewMockTableExecutor(t)

	ctx := cdcContext.NewContext(s.ctx, &cdcContext.GlobalVars{
		EtcdClient:    &cdcEtcdClient,
		MessageServer: s.cluster.Nodes[processorCaptureID].Server,
		MessageRouter: s.cluster.Nodes[processorCaptureID].Router,
	})
	s.cdcCtx = ctx

	ret, err := newAgent(ctx, messageServer, messageRouter, s.tableExecutor, "cf-1")
	if err != nil {
		return nil, err
	}

	return ret.(*agentImpl), nil
}

func (s *agentTestSuite) BlockSync() {
	s.blockSyncMu.Lock()
	defer s.blockSyncMu.Unlock()

	s.blockSync = true
}

func (s *agentTestSuite) UnblockSync() {
	s.blockSyncMu.Lock()
	defer s.blockSyncMu.Unlock()

	s.blockSync = false
	s.blockSyncCond.Broadcast()
}

func (s *agentTestSuite) Close() {
	s.UnblockSync()
	s.cancel()
	s.cluster.Close()
}

// newMockEtcdClientForAgentTests returns a mock Etcd client.
// NOTE: The mock client does not have any useful internal logic.
// It only supports GET operations and any output should be supplied by
// calling the mock.Mock methods embedded in the mock client.
func newMockEtcdClientForAgentTests(ctx context.Context) (*clientv3.Client, *mockEtcdKVClient) {
	cli := clientv3.NewCtxClient(ctx)
	mockKVCli := &mockEtcdKVClient{}
	cli.KV = mockKVCli
	return cli, mockKVCli
}

type mockEtcdKVClient struct {
	clientv3.KV // embeds a null implementation of the Etcd KV client
	mock.Mock
}

func (c *mockEtcdKVClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	args := c.Called(ctx, key, opts)
	resp := (*clientv3.GetResponse)(nil)
	if args.Get(0) != nil {
		resp = args.Get(0).(*clientv3.GetResponse)
	}
	return resp, args.Error(1)
}

func TestAgentBasics(t *testing.T) {
	suite := newAgentTestSuite(t)
	defer suite.Close()

	suite.etcdKVClient.On("Get", mock.Anything,
		etcd.CaptureOwnerKey, mock.Anything).
		Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:         []byte(etcd.CaptureOwnerKey),
					Value:       []byte(ownerCaptureID),
					ModRevision: 1,
				},
			},
		}, nil)

	// Test Point 1: Create an agent.
	agent, err := suite.CreateAgent(t)
	require.NoError(t, err)

	// Test Point 2: First tick should sync the SyncMessage.
	err = agent.Tick(suite.cdcCtx)
	require.NoError(t, err)

	select {
	case <-suite.ctx.Done():
		require.Fail(t, "context should not be canceled")
	case syncMsg := <-suite.syncCh:
		require.Equal(t, &model.SyncMessage{
			ProcessorVersion: version.ReleaseSemver(),
			Epoch:            agent.CurrentEpoch(),
			Running:          nil,
			Adding:           nil,
			Removing:         nil,
		}, syncMsg)
	}

	_, err = suite.ownerMessageClient.SendMessage(suite.ctx,
		model.DispatchTableTopic("cf-1"),
		&model.DispatchTableMessage{
			OwnerRev: 1,
			Epoch:    agent.CurrentEpoch(),
			ID:       1,
			IsDelete: false,
		})
	require.NoError(t, err)

	// Test Point 3: Accept an incoming DispatchTableMessage, and the AddTable method in TableExecutor can return false.
	suite.tableExecutor.On("AddTable", mock.Anything, model.TableID(1)).
		Return(false, nil).Once()
	suite.tableExecutor.On("AddTable", mock.Anything, model.TableID(1)).
		Return(true, nil).Run(
		func(_ mock.Arguments) {
			delete(suite.tableExecutor.Adding, 1)
			suite.tableExecutor.Running[1] = struct{}{}
		}).Once()
	suite.tableExecutor.On("GetCheckpoint").
		Return(model.Ts(1000), model.Ts(1000))

	require.Eventually(t, func() bool {
		err = agent.Tick(suite.cdcCtx)
		require.NoError(t, err)
		if len(suite.tableExecutor.Running) != 1 {
			return false
		}
		select {
		case <-suite.ctx.Done():
			require.Fail(t, "context should not be canceled")
		case msg := <-suite.checkpointCh:
			require.Equal(t, &model.CheckpointMessage{
				CheckpointTs: model.Ts(1000),
				ResolvedTs:   model.Ts(1000),
			}, msg)
			return true
		default:
		}
		return false
	}, 5*time.Second, 100*time.Millisecond)

	suite.tableExecutor.AssertExpectations(t)
	suite.tableExecutor.ExpectedCalls = nil
	suite.tableExecutor.Calls = nil

	suite.tableExecutor.On("GetCheckpoint").Return(model.Ts(1000), model.Ts(1000))
	// Test Point 4: Accept an incoming DispatchTableMessage, and the AddTable method in TableExecutor can return true.
	err = agent.Tick(suite.cdcCtx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		select {
		case <-suite.ctx.Done():
			return false
		case msg := <-suite.dispatchResponseCh:
			require.Equal(t, &model.DispatchTableResponseMessage{
				ID:    1,
				Epoch: agent.CurrentEpoch(),
			}, msg)
			return true
		default:
		}

		err = agent.Tick(suite.cdcCtx)
		require.NoError(t, err)
		return false
	}, time.Second*3, time.Millisecond*10)

	// Test Point 5: Close the agent.
	err = agent.Close()
	require.NoError(t, err)

	// double close
	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentNoOwnerAtStartUp(t *testing.T) {
	suite := newAgentTestSuite(t)
	defer suite.Close()

	// Empty response implies no owner.
	suite.etcdKVClient.On("Get", mock.Anything,
		etcd.CaptureOwnerKey, mock.Anything).
		Return(&clientv3.GetResponse{}, nil)

	// Test Point 1: Create an agent.
	agent, err := suite.CreateAgent(t)
	require.NoError(t, err)

	// Test Point 2: First ticks should not panic
	for i := 0; i < 10; i++ {
		err = agent.Tick(suite.cdcCtx)
		require.NoError(t, err)
	}

	// Test Point 3: Agent should process the Announce message.
	_, err = suite.ownerMessageClient.SendMessage(suite.ctx,
		model.AnnounceTopic("cf-1"),
		&model.AnnounceMessage{
			OwnerRev:     1,
			OwnerVersion: version.ReleaseSemver(),
		})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		err := agent.Tick(suite.cdcCtx)
		require.NoError(t, err)
		select {
		case <-suite.ctx.Done():
			require.Fail(t, "context should not be canceled")
		case syncMsg := <-suite.syncCh:
			require.Equal(t, &model.SyncMessage{
				ProcessorVersion: version.ReleaseSemver(),
				Epoch:            agent.CurrentEpoch(),
				Running:          nil,
				Adding:           nil,
				Removing:         nil,
			}, syncMsg)
			return true
		default:
		}
		return false
	}, 5*time.Second, 100*time.Millisecond)

	// Test Point 4: Close the agent.
	err = agent.Close()
	require.NoError(t, err)

	// double close
	err = agent.Close()
	require.NoError(t, err)
}

func TestAgentTolerateClientClosed(t *testing.T) {
	suite := newAgentTestSuite(t)
	defer suite.Close()

	suite.etcdKVClient.On("Get", mock.Anything,
		etcd.CaptureOwnerKey, mock.Anything).
		Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:         []byte(etcd.CaptureOwnerKey),
					Value:       []byte(ownerCaptureID),
					ModRevision: 1,
				},
			},
		}, nil)

	// Test Point 1: Create an agent.
	agent, err := suite.CreateAgent(t)
	require.NoError(t, err)

	_ = failpoint.Enable("github.com/pingcap/tiflow/pkg/p2p/ClientInjectClosed", "5*return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/pkg/p2p/ClientInjectClosed")
	}()

	// Test Point 2: We should tolerate the error ErrPeerMessageClientClosed
	for i := 0; i < 6; i++ {
		err = agent.Tick(suite.cdcCtx)
		require.NoError(t, err)
	}

	select {
	case <-suite.ctx.Done():
		require.Fail(t, "context should not be canceled")
	case syncMsg := <-suite.syncCh:
		require.Equal(t, &model.SyncMessage{
			ProcessorVersion: version.ReleaseSemver(),
			Epoch:            agent.CurrentEpoch(),
			Running:          nil,
			Adding:           nil,
			Removing:         nil,
		}, syncMsg)
	}
}

func TestNoFinishOperationBeforeSyncIsReceived(t *testing.T) {
	suite := newAgentTestSuite(t)
	defer suite.Close()

	suite.etcdKVClient.On("Get", mock.Anything,
		etcd.CaptureOwnerKey, mock.Anything).
		Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:         []byte(etcd.CaptureOwnerKey),
					Value:       []byte(ownerCaptureID),
					ModRevision: 1,
				},
			},
		}, nil)

	agent, err := suite.CreateAgent(t)
	require.NoError(t, err)

	suite.BlockSync()

	err = agent.Tick(suite.cdcCtx)
	require.NoError(t, err)

	_, err = suite.ownerMessageClient.SendMessage(suite.ctx,
		model.DispatchTableTopic("cf-1"),
		&model.DispatchTableMessage{
			OwnerRev: 1,
			Epoch:    agent.CurrentEpoch(),
			ID:       1,
			IsDelete: false,
		})
	require.NoError(t, err)

	_, err = suite.ownerMessageClient.SendMessage(suite.ctx,
		model.DispatchTableTopic("cf-1"),
		&model.DispatchTableMessage{
			OwnerRev: 1,
			Epoch:    agent.CurrentEpoch(),
			ID:       2,
			IsDelete: false,
		})
	require.NoError(t, err)

	suite.tableExecutor.On("AddTable", mock.Anything, model.TableID(1)).
		Return(true, nil).
		Run(
			func(_ mock.Arguments) {
				delete(suite.tableExecutor.Adding, 1)
				suite.tableExecutor.Running[1] = struct{}{}
			}).Once()
	suite.tableExecutor.On("AddTable", mock.Anything, model.TableID(2)).
		Return(true, nil).
		Run(
			func(_ mock.Arguments) {
				delete(suite.tableExecutor.Adding, 2)
				suite.tableExecutor.Running[2] = struct{}{}
			}).Once()
	suite.tableExecutor.On("GetCheckpoint").
		Return(model.Ts(1000), model.Ts(1000))

	require.Never(t, func() bool {
		err := agent.Tick(suite.cdcCtx)
		require.NoError(t, err)

		select {
		case <-suite.ctx.Done():
			return true
		case <-suite.dispatchResponseCh:
			return true
		case <-suite.syncCh:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 1*time.Millisecond)
	suite.UnblockSync()

	require.Eventually(t, func() bool {
		select {
		case <-suite.ctx.Done():
			require.Fail(t, "context should not be canceled")
			return false
		case syncMsg := <-suite.syncCh:
			require.Equal(t, &model.SyncMessage{
				ProcessorVersion: version.ReleaseSemver(),
				Epoch:            agent.CurrentEpoch(),
				Running:          nil,
				Adding:           nil,
				Removing:         nil,
			}, syncMsg)
			return true
		default:
		}

		err := agent.Tick(suite.cdcCtx)
		require.NoError(t, err)
		return false
	}, 1*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		select {
		case <-suite.ctx.Done():
			return false
		case <-suite.dispatchResponseCh:
			return true
		default:
		}

		err := agent.Tick(suite.cdcCtx)
		require.NoError(t, err)
		return false
	}, time.Second*3, time.Millisecond*10)

	require.Eventually(t, func() bool {
		select {
		case <-suite.ctx.Done():
			return false
		case <-suite.dispatchResponseCh:
			return true
		default:
		}

		err := agent.Tick(suite.cdcCtx)
		require.NoError(t, err)
		return false
	}, time.Second*3, time.Millisecond*10)

	suite.tableExecutor.AssertExpectations(t)
}
