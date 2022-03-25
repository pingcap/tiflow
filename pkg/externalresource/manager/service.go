package manager

import (
	"context"
	"sync"
	"time"

	"github.com/gogo/status"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/ctxmu"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

// Service implements pb.ResourceManagerServer
// TODOs:
// (1) Refactor cache-related logic
// (2) Add RemoveResource method for explicit resource releasing
// (3) Implement automatic resource GC
type Service struct {
	mu       *ctxmu.CtxMutex
	accessor *resourcemeta.MetadataAccessor
	cache    map[resourcemeta.ResourceID]*resourcemeta.ResourceMeta

	executors ExecutorInfoProvider

	wg       sync.WaitGroup
	cancelCh chan struct{}

	offlinedExecutors chan resourcemeta.ExecutorID

	isAllLoaded atomic.Bool
}

const (
	offlineExecutorQueueSize = 1024
)

func NewService(metaclient metaclient.KV, executorInfoProvider ExecutorInfoProvider) *Service {
	return &Service{
		mu:                ctxmu.New(),
		accessor:          resourcemeta.NewMetadataAccessor(metaclient),
		cache:             make(map[resourcemeta.ResourceID]*resourcemeta.ResourceMeta),
		executors:         executorInfoProvider,
		cancelCh:          make(chan struct{}),
		offlinedExecutors: make(chan resourcemeta.ExecutorID, offlineExecutorQueueSize),
	}
}

func (s *Service) QueryResource(ctx context.Context, request *pb.QueryResourceRequest) (*pb.QueryResourceResponse, error) {
	if !s.checkAllLoaded() {
		return nil, status.Error(codes.Unavailable, "ResourceManager is initializing")
	}

	logger := log.L().WithFields(zap.String("resource-id", request.GetResourceId()))

	if !s.mu.Lock(ctx) {
		return nil, status.Error(codes.Canceled, ctx.Err().Error())
	}
	defer s.mu.Unlock()

	record, exists := s.cache[request.GetResourceId()]
	if !exists {
		logger.Info("cache miss", zap.String("resource-id", request.GetResourceId()))
		var err error

		startTime := time.Now()
		record, exists, err = s.accessor.GetResource(ctx, request.ResourceId)
		getResourceDuration := time.Since(startTime)

		logger.Info("Resource meta fetch completed", zap.Duration("duration", getResourceDuration))
		if err != nil {
			st, stErr := status.New(codes.NotFound, "resource manager error").WithDetails(&pb.ResourceError{
				ErrorCode:  pb.ResourceErrorCode_ResourceManagerInternalError,
				StackTrace: errors.ErrorStack(err),
			})
			if stErr != nil {
				return nil, stErr
			}
			return nil, st.Err()
		}
		if !exists {
			st, stErr := status.New(codes.NotFound, "resource manager error").WithDetails(&pb.ResourceError{
				ErrorCode: pb.ResourceErrorCode_ResourceNotFound,
			})
			if stErr != nil {
				return nil, stErr
			}
			return nil, st.Err()
		}
		s.cache[request.ResourceId] = record
	} else {
		log.L().Info("cache hit", zap.String("resource-id", request.GetResourceId()))
	}

	if record.Deleted {
		st, stErr := status.New(codes.NotFound, "resource manager error").WithDetails(&pb.ResourceError{
			ErrorCode: pb.ResourceErrorCode_ResourceNotFound,
		})
		if stErr != nil {
			return nil, stErr
		}
		return nil, st.Err()
	}

	return record.ToQueryResourceResponse(), nil
}

func (s *Service) CreateResource(
	ctx context.Context,
	request *pb.CreateResourceRequest,
) (*pb.CreateResourceResponse, error) {
	if !s.checkAllLoaded() {
		return nil, status.Error(codes.Unavailable, "ResourceManager is initializing")
	}

	if !s.mu.Lock(ctx) {
		return nil, status.Error(codes.Canceled, ctx.Err().Error())
	}
	defer s.mu.Unlock()

	if _, exists := s.cache[request.GetResourceId()]; exists {
		st, stErr := status.New(codes.Internal, "resource manager error").WithDetails(&pb.ResourceError{
			ErrorCode: pb.ResourceErrorCode_ResourceIDConflict,
		})
		if stErr != nil {
			return nil, stErr
		}
		return nil, st.Err()
	}

	resourceRecord := &resourcemeta.ResourceMeta{
		ID:       request.GetResourceId(),
		Job:      request.GetJobId(),
		Worker:   request.GetCreatorWorkerId(),
		Executor: resourcemeta.ExecutorID(request.GetCreatorExecutor()),
		Deleted:  false,
	}

	ok, err := s.accessor.CreateResource(ctx, resourceRecord)
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
		st, stErr := status.New(codes.Internal, "resource manager error").WithDetails(&pb.ResourceError{
			ErrorCode: pb.ResourceErrorCode_ResourceIDConflict,
		})
		if stErr != nil {
			return nil, stErr
		}
		return nil, st.Err()
	}

	s.cache[request.GetResourceId()] = resourceRecord

	// TODO: handle the case where resourceRecord.Deleted == true
	return &pb.CreateResourceResponse{}, nil
}

// GetPlacementConstraint is called by the Scheduler to determine whether
// a resource the worker relies on requires the worker running on a specific
// executor.
// Returns:
// (1) A local resource is required and the resource exists: (executorID, true, nil)
// (2) A local resource is required but the resource is not found: ("", false, ErrResourceDoesNotExist)
// (3) No placement constraint is needed: ("", false, nil)
// (4) Other errors: ("", false, err)
func (s *Service) GetPlacementConstraint(
	ctx context.Context,
	id resourcemeta.ResourceID,
) (resourcemeta.ExecutorID, bool, error) {
	if !s.checkAllLoaded() {
		return "", false, derror.ErrResourceManagerNotReady.GenWithStackByArgs()
	}

	logger := log.L().WithFields(zap.String("resource-id", id))

	rType, _, err := resourcemeta.ParseResourcePath(id)
	if err != nil {
		return "", false, err
	}

	if rType != resourcemeta.ResourceTypeLocalFile {
		logger.Info("Resource does not need a constraint",
			zap.String("resource-id", id), zap.String("type", string(rType)))
		return "", false, nil
	}

	if !s.mu.Lock(ctx) {
		return "", false, errors.Trace(ctx.Err())
	}
	defer s.mu.Unlock()

	record, exists := s.cache[id]
	if !exists {
		// Note that although we are not doing cache eviction,
		// a miss is still a possibility given that we might have
		// a successful write to metastore but due to network problem
		// we have treated that write as an error.
		logger.Info("Resource cache miss")
		var err error

		startTime := time.Now()
		record, exists, err = s.accessor.GetResource(ctx, id)
		getResourceDuration := time.Since(startTime)

		logger.Info("Resource meta fetch completed", zap.Duration("duration", getResourceDuration))
		if err != nil {
			return "", false, err
		}
		if !exists {
			return "", false, derror.ErrResourceDoesNotExist.GenWithStackByArgs(id)
		}
		s.cache[id] = record
	} else {
		logger.Info("Resource cache hit")
	}

	if record.Deleted {
		logger.Info("Resource meta is marked as deleted", zap.Any("record", record))
		return "", false, derror.ErrResourceDoesNotExist.GenWithStackByArgs(id)
	}

	if !s.executors.HasExecutor(string(record.Executor)) {
		logger.Info("Resource meta indicates a non-existent executor",
			zap.String("executor-id", string(record.Executor)))
		return "", false, derror.ErrResourceDoesNotExist.GenWithStackByArgs(id)
	}

	return record.Executor, true, nil
}

func (s *Service) OnExecutorOffline(executorID resourcemeta.ExecutorID) error {
	select {
	case s.offlinedExecutors <- executorID:
		return nil
	default:
	}
	log.L().Warn("Too many offlined executors, dropping event",
		zap.String("executor-id", string(executorID)))
	return nil
}

func (s *Service) StartBackgroundWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-s.cancelCh
		cancel()
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer log.L().Info("Resource manager's background task exited")
		s.runBackgroundWorker(ctx)
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.loadCache(ctx)
	}()
}

func (s *Service) Stop() {
	close(s.cancelCh)
	s.wg.Wait()
}

func (s *Service) runBackgroundWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case executorID := <-s.offlinedExecutors:
			s.handleExecutorOffline(ctx, executorID)
		}
	}
}

func (s *Service) handleExecutorOffline(ctx context.Context, executorID resourcemeta.ExecutorID) {
	if !s.mu.Lock(ctx) {
		return
	}
	defer s.mu.Unlock()

	for _, record := range s.cache {
		if record.Executor != executorID {
			continue
		}
		record.Deleted = true
		log.L().Info("Mark record as deleted", zap.Any("record", record))
		// TODO asynchronously delete these records from the metastore.
	}
}

func (s *Service) checkAllLoaded() bool {
	return s.isAllLoaded.Load()
}

func (s *Service) loadCache(ctx context.Context) {
	rl := rate.NewLimiter(rate.Every(time.Second), 1)
	for {
		select {
		case <-ctx.Done():
			log.L().Info("loadCache is exiting", zap.Error(ctx.Err()))
			return
		default:
		}

		if err := rl.Wait(ctx); err != nil {
			log.L().Info("loadCache is exiting", zap.Error(err))
			return
		}

		if err := s.doLoadCache(ctx); err != nil {
			if errors.Cause(err) == context.Canceled {
				log.L().Info("loadCache is exiting", zap.Error(err))
				return
			}
			log.L().Warn("loadCache encountered error. Try again.", zap.Error(err))
			continue
		}

		old := s.isAllLoaded.Swap(true)
		if old {
			log.L().Panic("unexpected isAllLoaded == true")
		}
		return
	}
}

func (s *Service) doLoadCache(ctx context.Context) error {
	if !s.mu.Lock(ctx) {
		return errors.Trace(ctx.Err())
	}
	defer s.mu.Unlock()

	all, err := s.accessor.GetAllResources(ctx)
	if err != nil {
		return err
	}

	for _, resource := range all {
		s.cache[resource.ID] = resource
	}

	log.L().Info("Loaded resource records to cache", zap.Int("count", len(all)))
	return nil
}
