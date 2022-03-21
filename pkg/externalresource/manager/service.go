package manager

import (
	"context"

	"github.com/pingcap/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/ctxmu"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
)

type Service struct {
	mu       *ctxmu.CtxMutex
	accessor *resourcemeta.MetadataAccessor
}

func (s *Service) CreateResource(
	ctx context.Context,
	request *pb.CreateResourceRequest,
) (*pb.CreateResourceResponse, error) {
	if !s.mu.Lock(ctx) {
		return nil, status.Error(codes.Canceled, ctx.Err().Error())
	}
	defer s.mu.Unlock()

	ok, err := s.accessor.CreateResource(ctx, &resourcemeta.ResourceMeta{
		ID:       request.GetResourceId(),
		Job:      request.GetJobId(),
		Worker:   request.GetCreatorWorkerId(),
		Executor: resourcemeta.ExecutorID(request.GetCreatorExecutor()),
		Deleted:  false,
	})
	if err != nil {
		st, stErr := status.New(codes.Internal, err.Error()).WithDetails(&pb.ResourceError{
			ErrorCode:  pb.ResourceErrorCode_ResourceManagerInternalError,
			StackTrace: errors.ErrorStack(err),
		})
		if stErr != nil {
			return nil, stErr
		}
		return nil, st.Err()
	}

	if !ok {
		st, stErr := status.New(codes.Internal, err.Error()).WithDetails(&pb.ResourceError{
			ErrorCode: pb.ResourceErrorCode_ResourceIDConflict,
		})
		if stErr != nil {
			return nil, stErr
		}
		return nil, st.Err()
	}

	return &pb.CreateResourceResponse{}, nil
}
