package lib

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/client"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/model"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	"github.com/hanfei1991/microcosm/pkg/deps"
	mockkv "github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// testJobMasterImpl is a mock JobMasterImpl used to test
// the correctness of BaseJobMaster.
// TODO move testJobMasterImpl to a separate file
type testJobMasterImpl struct {
	mu sync.Mutex
	mock.Mock

	*DefaultBaseJobMaster
}

func (m *testJobMasterImpl) InitImpl(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testJobMasterImpl) Tick(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testJobMasterImpl) CloseImpl(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnMasterRecovered(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(ctx)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerStatusUpdated(worker WorkerHandle, newStatus *libModel.WorkerStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, newStatus)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerDispatched(worker WorkerHandle, result error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, result)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerOnline(worker WorkerHandle) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerOffline(worker WorkerHandle, reason error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, reason)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnWorkerMessage(worker WorkerHandle, topic p2p.Topic, message interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(worker, topic, message)
	return args.Error(0)
}

func (m *testJobMasterImpl) Workload() model.RescUnit {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called()
	return args.Get(0).(model.RescUnit)
}

func (m *testJobMasterImpl) OnJobManagerFailover(reason MasterFailoverReason) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(reason)
	return args.Error(0)
}

func (m *testJobMasterImpl) OnJobManagerMessage(topic p2p.Topic, message p2p.MessageValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(topic, message)
	return args.Error(0)
}

func (m *testJobMasterImpl) IsJobMasterImpl() {
	panic("unreachable")
}

func (m *testJobMasterImpl) Status() libModel.WorkerStatus {
	return libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	}
}

func newBaseJobMasterForTests(impl JobMasterImpl) *DefaultBaseJobMaster {
	cli, err := pkgOrm.NewMockClient()
	if err != nil {
		panic(err)
	}
	params := masterParamListForTest{
		MessageHandlerManager: p2p.NewMockMessageHandlerManager(),
		MessageSender:         p2p.NewMockMessageSender(),
		FrameMetaClient:       cli,
		UserRawKVClient:       mockkv.NewMetaMock(),
		ExecutorClientManager: client.NewClientManager(),
		ServerMasterClient:    &client.MockServerMasterClient{},
	}
	dp := deps.NewDeps()
	err = dp.Provide(func() masterParamListForTest {
		return params
	})
	if err != nil {
		panic(err)
	}

	ctx := dcontext.Background()
	ctx = ctx.WithDeps(dp)

	return NewBaseJobMaster(
		ctx,
		impl,
		masterName,
		workerID1,
	).(*DefaultBaseJobMaster)
}

func TestBaseJobMasterBasics(t *testing.T) {
	jobMaster := &testJobMasterImpl{}
	base := newBaseJobMasterForTests(jobMaster)
	jobMaster.DefaultBaseJobMaster = base

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jobMaster.mu.Lock()
	jobMaster.On("InitImpl", mock.Anything).Return(nil)
	jobMaster.mu.Unlock()

	err := jobMaster.Init(ctx)
	require.NoError(t, err)

	jobMaster.mu.Lock()
	jobMaster.AssertNumberOfCalls(t, "InitImpl", 1)

	// clean status
	jobMaster.ExpectedCalls = nil
	jobMaster.Calls = nil

	jobMaster.On("Tick", mock.Anything).Return(nil)
	jobMaster.mu.Unlock()

	err = jobMaster.Poll(ctx)
	require.NoError(t, err)

	jobMaster.mu.Lock()
	jobMaster.AssertNumberOfCalls(t, "Tick", 1)

	// clean status
	jobMaster.ExpectedCalls = nil
	jobMaster.Calls = nil

	jobMaster.On("CloseImpl", mock.Anything).Return(nil)
	jobMaster.mu.Unlock()

	err = jobMaster.Exit(ctx, jobMaster.Status(), nil)
	require.Regexp(t, ".*DFLOW:ErrWorkerFinish.*", err)

	err = jobMaster.Close(ctx)
	require.NoError(t, err)

	jobMaster.mu.Lock()
	jobMaster.AssertNumberOfCalls(t, "CloseImpl", 1)
	jobMaster.mu.Unlock()
}
