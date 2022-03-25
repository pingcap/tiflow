package broker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gogo/status"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/externalresource/manager"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

func newBroker(t *testing.T) (*Impl, *manager.MockClient, string) {
	tmpDir := t.TempDir()
	client := manager.NewMockClient()
	broker := NewBroker(&storagecfg.Config{Local: &storagecfg.LocalFileConfig{BaseDir: tmpDir}},
		"executor-1",
		client)
	return broker, client, tmpDir
}

func TestBrokerOpenNewStorage(t *testing.T) {
	brk, client, dir := newBroker(t)

	st, err := status.New(codes.Internal, "resource manager error").WithDetails(&pb.ResourceError{
		ErrorCode: pb.ResourceErrorCode_ResourceNotFound,
	})
	require.NoError(t, err)

	client.On("QueryResource", mock.Anything, &pb.QueryResourceRequest{ResourceId: "/local/test-1"}, mock.Anything).
		Return((*pb.QueryResourceResponse)(nil), st.Err())
	hdl, err := brk.OpenStorage(context.Background(), "worker-1", "job-1", "/local/test-1")
	require.NoError(t, err)
	require.Equal(t, "/local/test-1", hdl.ID())

	client.AssertExpectations(t)
	client.ExpectedCalls = nil

	f, err := hdl.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)

	err = f.Close(context.Background())
	require.NoError(t, err)

	client.On("CreateResource", mock.Anything, &pb.CreateResourceRequest{
		ResourceId:      "/local/test-1",
		CreatorExecutor: "executor-1",
		JobId:           "job-1",
		CreatorWorkerId: "worker-1",
	}, mock.Anything).Return(&pb.CreateResourceResponse{}, nil)

	err = hdl.Persist(context.Background())
	require.NoError(t, err)

	client.AssertExpectations(t)

	fileName := filepath.Join(dir, "worker-1", "test-1", "1.txt")
	require.FileExists(t, fileName)
}

func TestBrokerOpenExistingStorage(t *testing.T) {
	brk, client, dir := newBroker(t)

	client.On("QueryResource", mock.Anything, &pb.QueryResourceRequest{ResourceId: "/local/test-2"}, mock.Anything).
		Return(&pb.QueryResourceResponse{
			CreatorExecutor: "executor-1",
			JobId:           "job-1",
			CreatorWorkerId: "worker-2",
		}, nil)
	hdl, err := brk.OpenStorage(context.Background(), "worker-1", "job-1", "/local/test-2")
	require.NoError(t, err)
	require.Equal(t, "/local/test-2", hdl.ID())

	client.AssertExpectations(t)

	f, err := hdl.BrExternalStorage().Create(context.Background(), "1.txt")
	require.NoError(t, err)

	err = f.Close(context.Background())
	require.NoError(t, err)

	fileName := filepath.Join(dir, "worker-2", "test-2", "1.txt")
	require.FileExists(t, fileName)
}
