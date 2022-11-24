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

package dm

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/checker"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/master"
	dmpb "github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	libMetadata "github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/registry"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/checkpoint"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/runtime"
	"github.com/pingcap/tiflow/engine/pkg/client"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/deps"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/broker"
	kvmock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

func TestDMJobmasterSuite(t *testing.T) {
	suite.Run(t, new(testDMJobmasterSuite))
}

type testDMJobmasterSuite struct {
	suite.Suite
	funcBackup func(ctx context.Context, cfg *dmconfig.SourceConfig) error
}

func (t *testDMJobmasterSuite) SetupSuite() {
	taskNormalInterval = time.Hour
	taskErrorInterval = 100 * time.Millisecond
	WorkerNormalInterval = time.Hour
	WorkerErrorInterval = 100 * time.Millisecond
	runtime.HeartbeatInterval = 1 * time.Second
	require.NoError(t.T(), logutil.InitLogger(&logutil.Config{Level: "debug"}))
	t.funcBackup = master.CheckAndAdjustSourceConfigFunc
	master.CheckAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
}

func checkAndNoAdjustSourceConfigMock(ctx context.Context, cfg *dmconfig.SourceConfig) error {
	if _, err := cfg.Yaml(); err != nil {
		return err
	}
	return cfg.Verify()
}

func (t *testDMJobmasterSuite) TearDownSuite() {
	master.CheckAndAdjustSourceConfigFunc = t.funcBackup
}

type masterParamListForTest struct {
	dig.Out

	MessageHandlerManager p2p.MessageHandlerManager
	MessageSender         p2p.MessageSender
	FrameMetaClient       pkgOrm.Client
	BusinessClientConn    metaModel.ClientConn
	ExecutorGroup         client.ExecutorGroup
	ServerMasterClient    client.ServerMasterClient
	ResourceBroker        broker.Broker
}

// Init -> Poll -> Close
func (t *testDMJobmasterSuite) TestRunDMJobMaster() {
	cli, err := pkgOrm.NewMockClient()
	require.NoError(t.T(), err)
	mockServerMasterClient := client.NewMockServerMasterClient(gomock.NewController(t.T()))
	mockExecutorGroup := client.NewMockExecutorGroup()
	broker := broker.NewBrokerForTesting("test-executor-id")
	defer broker.Close()
	depsForTest := masterParamListForTest{
		MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
		MessageSender:         p2p.NewMockMessageSender(),
		FrameMetaClient:       cli,
		BusinessClientConn:    kvmock.NewMockClientConn(),
		ExecutorGroup:         mockExecutorGroup,
		ServerMasterClient:    mockServerMasterClient,
		ResourceBroker:        broker,
	}

	RegisterWorker()
	dctx := dcontext.Background()
	dp := deps.NewDeps()
	require.NoError(t.T(), dp.Provide(func() masterParamListForTest {
		return depsForTest
	}))
	dctx = dctx.WithDeps(dp)

	// submit-job
	cfgBytes, err := os.ReadFile(jobTemplatePath)
	require.NoError(t.T(), err)
	jobmaster, err := registry.GlobalWorkerRegistry().CreateWorker(
		dctx, frameModel.DMJobMaster, "dm-jobmaster", libMetadata.JobManagerUUID,
		cfgBytes, int64(1))
	require.NoError(t.T(), err)

	// Init
	verDB := conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v6.1.0"))
	_, mockDB, err := conn.InitMockDBFull()
	require.NoError(t.T(), err)
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "check pass", nil
	}
	require.NoError(t.T(), jobmaster.Init(context.Background()))

	// mock master failed and recoverd after init
	require.NoError(t.T(), jobmaster.Close(context.Background()))

	jobmaster, err = registry.GlobalWorkerRegistry().CreateWorker(
		dctx, frameModel.DMJobMaster, "dm-jobmaster", libMetadata.JobManagerUUID,
		cfgBytes, int64(2))
	require.NoError(t.T(), err)
	verDB = conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.26-log"))
	_, mockDB, err = conn.InitMockDBFull()
	require.NoError(t.T(), err)
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mockDB.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t.T(), jobmaster.Init(context.Background()))

	// Poll
	mockServerMasterClient.EXPECT().
		ScheduleTask(gomock.Any(), gomock.Any()).
		Return(&pb.ScheduleTaskResponse{}, nil).
		MinTimes(0)
	mockServerMasterClient.EXPECT().
		ScheduleTask(gomock.Any(), gomock.Any()).
		Return(&pb.ScheduleTaskResponse{}, nil).
		MinTimes(0)

	require.NoError(t.T(), jobmaster.Poll(context.Background()))
	require.NoError(t.T(), jobmaster.Poll(context.Background()))
	require.NoError(t.T(), jobmaster.Poll(context.Background()))

	// Close
	require.NoError(t.T(), jobmaster.Close(context.Background()))
}

func (t *testDMJobmasterSuite) TestDMJobmaster() {
	metaKVClient := kvmock.NewMetaMock()
	mockBaseJobmaster := &MockBaseJobmaster{t: t.T()}
	mockCheckpointAgent := &MockCheckpointAgent{}
	checkpoint.NewCheckpointAgent = func(string, *zap.Logger) checkpoint.Agent { return mockCheckpointAgent }
	mockMessageAgent := &dmpkg.MockMessageAgent{}
	dmpkg.NewMessageAgent = func(id string, commandHandler interface{}, messageHandlerManager p2p.MessageHandlerManager, pLogger *zap.Logger) dmpkg.MessageAgent {
		return mockMessageAgent
	}
	defer func() {
		checkpoint.NewCheckpointAgent = checkpoint.NewAgentImpl
		dmpkg.NewMessageAgent = dmpkg.NewMessageAgentImpl
	}()
	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	jm := &JobMaster{
		initJobCfg:    jobCfg,
		BaseJobMaster: mockBaseJobmaster,
	}

	// init
	verDB := conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v6.1.0"))
	precheckError := errors.New("precheck error")
	exitError := errors.New("exit error")
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "", precheckError
	}
	mockBaseJobmaster.On("MetaKVClient").Return(metaKVClient)
	mockBaseJobmaster.On("GetWorkers").Return(map[string]framework.WorkerHandle{}).Once()
	mockBaseJobmaster.On("Exit").Return(exitError).Once()
	require.EqualError(t.T(), jm.InitImpl(context.Background()), exitError.Error())

	jm.initJobCfg.TaskMode = dmconfig.ModeIncrement
	verDB = conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v6.1.0"))
	_, mockDB, err := conn.InitMockDBFull()
	require.NoError(t.T(), err)
	getMasterStatusError := errors.New("failed to get master status")
	mockDB.ExpectQuery(`SHOW MASTER STATUS`).WillReturnError(getMasterStatusError)
	mockBaseJobmaster.On("MetaKVClient").Return(metaKVClient)
	mockBaseJobmaster.On("GetWorkers").Return(map[string]framework.WorkerHandle{}).Once()
	mockBaseJobmaster.On("Exit").Return(exitError).Once()
	require.EqualError(t.T(), jm.InitImpl(context.Background()), exitError.Error())

	jm.initJobCfg.TaskMode = dmconfig.ModeAll
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "check pass", nil
	}
	verDB = conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.26-log"))
	mockBaseJobmaster.On("MetaKVClient").Return(metaKVClient)
	mockBaseJobmaster.On("GetWorkers").Return(map[string]framework.WorkerHandle{}).Once()
	require.NoError(t.T(), jm.InitImpl(context.Background()))

	// recover
	jm = &JobMaster{
		BaseJobMaster: mockBaseJobmaster,
	}
	mockBaseJobmaster.On("GetWorkers").Return(map[string]framework.WorkerHandle{}).Once()
	jm.OnMasterRecovered(context.Background())

	// tick
	worker1 := "worker1"
	worker2 := "worker2"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker1, nil).Once()
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker2, nil).Once()
	mockCheckpointAgent.On("IsFresh", mock.Anything).Return(true, nil).Times(6)
	require.NoError(t.T(), jm.Tick(context.Background()))
	require.NoError(t.T(), jm.Tick(context.Background()))
	require.NoError(t.T(), jm.Tick(context.Background()))
	// make sure workerHandle1 bound to task status1, workerHandle2 bound to task status2.
	taskStatus1 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMDump,
		Stage: metadata.StageRunning,
	}
	taskStatus2 := runtime.TaskStatus{
		Unit:  frameModel.WorkerDMDump,
		Stage: metadata.StageRunning,
	}
	loadStatus := &dmpb.LoadStatus{
		FinishedBytes:  4,
		TotalBytes:     100,
		Progress:       "4%",
		MetaBinlog:     "mysql-bin.000002, 8",
		MetaBinlogGTID: "1-2-3",
	}
	loadStatusBytes, err := json.Marshal(loadStatus)
	require.NoError(t.T(), err)
	finishedStatus := runtime.FinishedTaskStatus{
		Result: &dmpb.ProcessResult{IsCanceled: false},
		Status: loadStatusBytes,
	}
	jm.workerManager.workerStatusMap.Range(func(key, val interface{}) bool {
		if val.(runtime.WorkerStatus).ID == worker1 {
			taskStatus1.Task = key.(string)
		} else {
			taskStatus2.Task = key.(string)
		}
		return true
	})
	workerHandle1 := &framework.MockWorkerHandler{WorkerID: worker1}
	workerHandle2 := &framework.MockWorkerHandler{WorkerID: worker2}

	// worker1 online, worker2 dispatch error
	bytes1, err := json.Marshal(taskStatus1)
	require.NoError(t.T(), err)
	require.NoError(t.T(), jm.OnWorkerOnline(workerHandle1))
	workerHandle1.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	require.NoError(t.T(), jm.OnWorkerStatusUpdated(workerHandle1, &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal, ExtBytes: bytes1}))
	jm.OnWorkerDispatched(workerHandle2, errors.New("dispatch error"))
	worker3 := "worker3"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker3, nil).Once()
	mockCheckpointAgent.On("IsFresh", mock.Anything).Return(true, nil).Times(3)
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker1 offline, worker3 online
	workerHandle2.WorkerID = worker3

	bytes2, err := json.Marshal(taskStatus2)
	require.NoError(t.T(), err)
	workerHandle2.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes2}).Once()
	workerHandle2.On("IsTombStone").Return(false).Once()
	jm.OnWorkerStatusUpdated(workerHandle2, &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal, ExtBytes: bytes2})
	workerHandle1.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes1}).Once()
	jm.OnWorkerOffline(workerHandle1, errors.New("offline error"))
	worker4 := "worker4"
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker4, nil).Once()
	mockCheckpointAgent.On("IsFresh", mock.Anything).Return(true, nil).Times(3)
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker4 online
	workerHandle1.WorkerID = worker4
	workerHandle1.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	jm.OnWorkerStatusUpdated(workerHandle1, &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal, ExtBytes: bytes1})
	require.NoError(t.T(), jm.Tick(context.Background()))

	// worker1 finished
	taskStatus1.Stage = metadata.StageFinished
	finishedStatus.TaskStatus = taskStatus1
	bytes1, err = json.Marshal(finishedStatus)
	require.NoError(t.T(), err)
	worker5 := "worker5"
	workerHandle1.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes1}).Once()
	mockBaseJobmaster.On("CreateWorker", mock.Anything, mock.Anything, mock.Anything).Return(worker5, nil).Once()
	jm.OnWorkerOffline(workerHandle1, nil)
	require.NoError(t.T(), jm.Tick(context.Background()))
	// worker5 online
	workerHandle1.WorkerID = worker5
	taskStatus1.Stage = metadata.StageRunning
	bytes1, err = json.Marshal(taskStatus1)
	require.NoError(t.T(), err)
	workerHandle1.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	jm.OnWorkerStatusUpdated(workerHandle1, &frameModel.WorkerStatus{State: frameModel.WorkerStateNormal, ExtBytes: bytes1})
	require.NoError(t.T(), jm.Tick(context.Background()))

	// master failover
	jm = &JobMaster{
		BaseJobMaster:   mockBaseJobmaster,
		checkpointAgent: mockCheckpointAgent,
	}
	mockBaseJobmaster.On("GetWorkers").Return(map[string]framework.WorkerHandle{worker4: workerHandle1, worker3: workerHandle2}).Once()
	workerHandle1.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle2.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes2}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	workerHandle2.On("IsTombStone").Return(false).Once()
	jm.OnMasterRecovered(context.Background())
	require.NoError(t.T(), jm.Tick(context.Background()))

	workerHandle1.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle1.On("IsTombStone").Return(false).Once()
	require.NoError(t.T(), jm.OnWorkerStatusUpdated(workerHandle1, &frameModel.WorkerStatus{ExtBytes: bytes1}))
	require.NoError(t.T(), jm.OnWorkerStatusUpdated(workerHandle1, &frameModel.WorkerStatus{}))

	// placeholder
	require.NoError(t.T(), jm.OnJobManagerMessage("", ""))
	require.NoError(t.T(), jm.OnMasterMessage(context.Background(), "", ""))
	require.NoError(t.T(), jm.OnWorkerMessage(&framework.MockWorkerHandler{}, "", ""))

	// Close
	jm.CloseImpl(context.Background())

	// OnCancel
	mockMessageAgent.On("SendRequest", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&dmpkg.QueryStatusResponse{Unit: frameModel.WorkerDMSync, Stage: metadata.StageRunning, Status: bytes1}, nil).Twice()
	mockMessageAgent.On("SendMessage").Return(nil).Twice()
	mockBaseJobmaster.On("Exit").Return(nil).Once()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), jm.OnCancel(context.Background()))
	}()
	require.Eventually(t.T(), func() bool {
		mockMessageAgent.Lock()
		if len(mockMessageAgent.Calls) == 4 {
			mockMessageAgent.Unlock()
			return true
		}
		mockMessageAgent.Unlock()
		jm.workerManager.DoTick(context.Background())
		return false
	}, 10*time.Second, 1*time.Second)
	workerHandle1.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes1}).Once()
	workerHandle2.On("Status").Return(&frameModel.WorkerStatus{ExtBytes: bytes2}).Once()
	jm.OnWorkerOffline(workerHandle1, errors.New("offline error"))
	jm.OnWorkerOffline(workerHandle2, errors.New("offline error"))
	wg.Wait()

	// Stop
	jm.StopImpl(context.Background())

	mockBaseJobmaster.AssertExpectations(t.T())
	workerHandle1.AssertExpectations(t.T())
	workerHandle2.AssertExpectations(t.T())
	mockCheckpointAgent.AssertExpectations(t.T())
}

func TestDuplicateFinishedState(t *testing.T) {
	ctx := context.Background()
	metaKVClient := kvmock.NewMetaMock()
	mockBaseJobmaster := &MockBaseJobmaster{t: t}
	mockCheckpointAgent := &MockCheckpointAgent{}
	checkpoint.NewCheckpointAgent = func(string, *zap.Logger) checkpoint.Agent { return mockCheckpointAgent }
	mockMessageAgent := &dmpkg.MockMessageAgent{}
	dmpkg.NewMessageAgent = func(id string, commandHandler interface{}, messageHandlerManager p2p.MessageHandlerManager, pLogger *zap.Logger) dmpkg.MessageAgent {
		return mockMessageAgent
	}
	defer func() {
		checkpoint.NewCheckpointAgent = checkpoint.NewAgentImpl
		dmpkg.NewMessageAgent = dmpkg.NewMessageAgentImpl
	}()
	jm := &JobMaster{
		BaseJobMaster: mockBaseJobmaster,
	}

	mockBaseJobmaster.On("MetaKVClient").Return(metaKVClient)
	mockBaseJobmaster.On("GetWorkers").Return(map[string]framework.WorkerHandle{}).Once()
	err := jm.initComponents()
	require.NoError(t, err)
	loadTime, _ := time.Parse(time.RFC3339Nano, "2022-11-04T19:47:57.43382274+08:00")
	dumpDuration := time.Hour
	loadDuration := time.Hour
	state := &metadata.UnitState{
		CurrentUnitStatus: map[string]*metadata.UnitStatus{
			"task2": {
				Unit:        frameModel.WorkerDMLoad,
				Task:        "task2",
				CreatedTime: loadTime,
			},
		},
		FinishedUnitStatus: map[string][]*metadata.FinishedTaskStatus{
			"task2": {
				&metadata.FinishedTaskStatus{
					TaskStatus: metadata.TaskStatus{
						Unit:           frameModel.WorkerDMDump,
						Task:           "task2",
						Stage:          metadata.StageFinished,
						CfgModRevision: 3,
					},
					Duration: dumpDuration,
				},
				&metadata.FinishedTaskStatus{
					TaskStatus: metadata.TaskStatus{
						Unit:           frameModel.WorkerDMLoad,
						Task:           "task2",
						Stage:          metadata.StageFinished,
						CfgModRevision: 3,
					},
					Duration: loadDuration,
				},
			},
		},
	}
	err = jm.metadata.UnitStateStore().Put(ctx, state)
	require.NoError(t, err)

	// the difference is the cfgModRevision
	workerHandle := &framework.MockWorkerHandler{WorkerID: "worker"}
	finishedTaskStatus := runtime.FinishedTaskStatus{
		TaskStatus: metadata.TaskStatus{
			Unit:           frameModel.WorkerDMLoad,
			Task:           "task2",
			Stage:          metadata.StageFinished,
			CfgModRevision: 4,
		},
	}
	err = jm.onWorkerFinished(finishedTaskStatus, workerHandle)
	require.NoError(t, err)

	state2Iface, err := jm.metadata.UnitStateStore().Get(ctx)
	require.NoError(t, err)
	state2 := state2Iface.(*metadata.UnitState)
	require.Equal(t, 1, len(state2.FinishedUnitStatus))
	require.Equal(t, 2, len(state2.FinishedUnitStatus["task2"]))
	require.Equal(t, frameModel.WorkerDMLoad, state2.FinishedUnitStatus["task2"][1].Unit)
	require.Equal(t, uint64(4), state2.FinishedUnitStatus["task2"][1].CfgModRevision)
	// the correct duration is that from loadTime to now
	require.NotEqual(t, loadDuration, state2.FinishedUnitStatus["task2"][1].Duration)
}

// TODO: move to separate file
type MockBaseJobmaster struct {
	mu sync.Mutex
	mock.Mock
	t *testing.T

	framework.BaseJobMaster
}

func (m *MockBaseJobmaster) ID() frameModel.WorkerID {
	return "dm-jobmaster-id"
}

func (m *MockBaseJobmaster) GetWorkers() map[string]framework.WorkerHandle {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(map[string]framework.WorkerHandle)
}

func (m *MockBaseJobmaster) MetaKVClient() metaModel.KVClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(metaModel.KVClient)
}

func (m *MockBaseJobmaster) CreateWorker(workerType framework.WorkerType,
	config framework.WorkerConfig, opts ...framework.CreateWorkerOpt,
) (frameModel.WorkerID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(frameModel.WorkerID), args.Error(1)
}

func (m *MockBaseJobmaster) CurrentEpoch() int64 {
	return 0
}

func (m *MockBaseJobmaster) Logger() *zap.Logger {
	return log.L()
}

func (m *MockBaseJobmaster) Exit(ctx context.Context, exitReason framework.ExitReason, err error, detail []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Error(0)
}

func (m *MockBaseJobmaster) IsS3StorageEnabled() bool {
	return false
}

func (m *MockBaseJobmaster) MetricFactory() promutil.Factory {
	return promutil.NewFactory4Test(m.t.TempDir())
}

type MockCheckpointAgent struct {
	mu sync.Mutex
	mock.Mock
}

func (m *MockCheckpointAgent) Create(ctx context.Context, cfg *config.JobCfg) error {
	return nil
}

func (m *MockCheckpointAgent) Remove(ctx context.Context, cfg *config.JobCfg) error {
	return nil
}

func (m *MockCheckpointAgent) IsFresh(ctx context.Context, workerType framework.WorkerType, taskCfg *metadata.Task) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	args := m.Called()
	return args.Get(0).(bool), args.Error(1)
}

func (m *MockCheckpointAgent) Upgrade(ctx context.Context, preVer semver.Version) error {
	return nil
}
