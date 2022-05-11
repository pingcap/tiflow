package manager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"

	"github.com/hanfei1991/microcosm/pb"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
)

type serviceTestSuite struct {
	service              *Service
	executorInfoProvider *MockExecutorInfoProvider
	meta                 pkgOrm.Client
}

var serviceMockData = []*resourcemeta.ResourceMeta{
	{
		ID:       "/local/test/1",
		Job:      "test-job-1",
		Worker:   "test-worker-1",
		Executor: "executor-1",
		Deleted:  false,
	},
	{
		ID:       "/local/test/2",
		Job:      "test-job-1",
		Worker:   "test-worker-1",
		Executor: "executor-1",
		Deleted:  false,
	},
	{
		ID:       "/local/test/3",
		Job:      "test-job-1",
		Worker:   "test-worker-2",
		Executor: "executor-2",
		Deleted:  false,
	},
	{
		ID:       "/local/test/4",
		Job:      "test-job-1",
		Worker:   "test-worker-2",
		Executor: "executor-2",
		Deleted:  false,
	},
	{
		ID:       "/local/test/5",
		Job:      "test-job-1",
		Worker:   "test-worker-3",
		Executor: "executor-4",
		Deleted:  true,
	},
}

func newServiceTestSuite(t *testing.T) *serviceTestSuite {
	execPro := NewMockExecutorInfoProvider()
	meta, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	id := "leader"
	leaderVal := &atomic.Value{}
	leaderVal.Store(&rpcutil.Member{Name: id})
	srvc := NewService(meta, execPro, rpcutil.NewPreRPCHook[pb.ResourceManagerClient](
		id,
		leaderVal,
		&rpcutil.LeaderClientWithLock[pb.ResourceManagerClient]{},
		atomic.NewBool(true),
		&rate.Limiter{}))
	return &serviceTestSuite{
		service:              srvc,
		executorInfoProvider: execPro,
		meta:                 meta,
	}
}

func (s *serviceTestSuite) Start() {
	s.service.StartBackgroundWorker()
}

func (s *serviceTestSuite) WaitForReady(t *testing.T) {
	require.Eventually(t, func() bool {
		return s.service.isAllLoaded.Load()
	}, 1*time.Second, 1*time.Millisecond)
}

func (s *serviceTestSuite) Stop() {
	s.service.Stop()
}

func (s *serviceTestSuite) OfflineExecutor(t *testing.T, executor resourcemeta.ExecutorID) {
	s.executorInfoProvider.RemoveExecutor(string(executor))
	err := s.service.onExecutorOffline(executor)
	require.NoError(t, err)
}

func (s *serviceTestSuite) LoadMockData() {
	for _, resource := range serviceMockData {
		_ = s.meta.UpsertResource(context.Background(), resource)
	}

	for i := 1; i <= 4; i++ {
		s.executorInfoProvider.AddExecutor(fmt.Sprintf("executor-%d", i))
	}
}

func (s *serviceTestSuite) CleanCache() {
	// Note that this is NOT thread-safe
	s.service.cache = make(map[resourcemeta.ResourceID]*resourcemeta.ResourceMeta)
}

func TestServiceBasics(t *testing.T) {
	suite := newServiceTestSuite(t)
	suite.LoadMockData()
	suite.Start()
	suite.WaitForReady(t)

	ctx := context.Background()
	_, err := suite.service.CreateResource(ctx, &pb.CreateResourceRequest{
		ResourceId:      "/local/test/6",
		CreatorExecutor: "executor-1",
		JobId:           "test-job-1",
		CreatorWorkerId: "test-worker-4",
	})
	require.NoError(t, err)

	_, err = suite.service.CreateResource(ctx, &pb.CreateResourceRequest{
		ResourceId:      "/local/test/6",
		CreatorExecutor: "executor-1",
		JobId:           "test-job-1",
		CreatorWorkerId: "test-worker-4",
	})
	require.Error(t, err)
	require.Equal(t, pb.ResourceErrorCode_ResourceIDConflict, status.Convert(err).Details()[0].(*pb.ResourceError).ErrorCode)

	execID, ok, err := suite.service.GetPlacementConstraint(ctx, "/local/test/6")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "executor-1", string(execID))

	execID, ok, err = suite.service.GetPlacementConstraint(ctx, "/local/test/1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "executor-1", string(execID))

	execID, ok, err = suite.service.GetPlacementConstraint(ctx, "/local/test/2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "executor-1", string(execID))

	resp, err := suite.service.QueryResource(ctx, &pb.QueryResourceRequest{ResourceId: "/local/test/2"})
	require.NoError(t, err)
	require.Equal(t, &pb.QueryResourceResponse{
		CreatorExecutor: "executor-1",
		JobId:           "test-job-1",
		CreatorWorkerId: "test-worker-1",
	}, resp)

	suite.OfflineExecutor(t, "executor-1")
	require.Eventually(t, func() bool {
		_, _, err := suite.service.GetPlacementConstraint(ctx, "/local/test/2")
		if err != nil {
			require.Regexp(t, ".*ErrResourceDoesNotExist.*", err.Error())
			return true
		}
		return false
	}, 1*time.Second, 1*time.Millisecond)

	_, err = suite.service.QueryResource(ctx, &pb.QueryResourceRequest{ResourceId: "/local/test/2"})
	require.Error(t, err)
	require.Equal(t, pb.ResourceErrorCode_ResourceNotFound, status.Convert(err).Details()[0].(*pb.ResourceError).ErrorCode)

	_, err = suite.service.QueryResource(ctx, &pb.QueryResourceRequest{ResourceId: "/local/test/non-existent"})
	require.Error(t, err)
	require.Equal(t, pb.ResourceErrorCode_ResourceNotFound, status.Convert(err).Details()[0].(*pb.ResourceError).ErrorCode)

	suite.Stop()
}

func TestServiceNotReady(t *testing.T) {
	suite := newServiceTestSuite(t)
	// We do not call Start()

	ctx := context.Background()
	_, err := suite.service.CreateResource(ctx, &pb.CreateResourceRequest{
		ResourceId:      "/local/test/test-resource",
		CreatorExecutor: "executor-1",
		JobId:           "test-job-1",
		CreatorWorkerId: "test-worker-4",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())

	_, _, err = suite.service.GetPlacementConstraint(ctx, "/local/test/test-resource")
	require.Error(t, err)
	require.True(t, derror.ErrResourceManagerNotReady.Equal(err))
}

func TestServiceResourceTypeNoConstraint(t *testing.T) {
	suite := newServiceTestSuite(t)
	suite.LoadMockData()
	suite.Start()
	suite.WaitForReady(t)

	_, ok, err := suite.service.GetPlacementConstraint(context.Background(), "/s3/fake-s3-resource")
	require.NoError(t, err)
	require.False(t, ok)

	suite.Stop()
}

func TestServiceCacheMiss(t *testing.T) {
	suite := newServiceTestSuite(t)
	suite.LoadMockData()
	suite.Start()
	suite.WaitForReady(t)
	suite.CleanCache()

	ctx := context.Background()
	execID, ok, err := suite.service.GetPlacementConstraint(ctx, "/local/test/2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "executor-1", string(execID))

	_, err = suite.service.CreateResource(ctx, &pb.CreateResourceRequest{
		ResourceId:      "/local/test/1",
		CreatorExecutor: "executor-1",
		JobId:           "test-job-1",
		CreatorWorkerId: "test-worker-4",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Len(t, st.Details(), 1)
	require.Equal(t, pb.ResourceErrorCode_ResourceIDConflict, st.Details()[0].(*pb.ResourceError).ErrorCode)

	resp, err := suite.service.QueryResource(ctx, &pb.QueryResourceRequest{ResourceId: "/local/test/2"})
	require.NoError(t, err)
	require.Equal(t, &pb.QueryResourceResponse{
		CreatorExecutor: "executor-1",
		JobId:           "test-job-1",
		CreatorWorkerId: "test-worker-1",
	}, resp)

	suite.Stop()
}
