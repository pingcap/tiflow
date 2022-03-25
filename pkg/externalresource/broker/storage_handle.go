package broker

import (
	"context"
	"path/filepath"

	"github.com/gogo/status"
	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

type Handle interface {
	ID() resourcemeta.ResourceID
	BrExternalStorage() brStorage.ExternalStorage
	Persist(ctx context.Context) error
	Discard(ctx context.Context) error
}

// BrExternalStorageHandle contains a brStorage.ExternalStorage.
// It helps Dataflow Engine reuse the external storage facilities
// implemented in Br.
type BrExternalStorageHandle struct {
	id         resourcemeta.ResourceID
	jobID      resourcemeta.JobID
	workerID   resourcemeta.WorkerID
	executorID resourcemeta.ExecutorID

	inner  brStorage.ExternalStorage
	client pb.ResourceManagerClient
}

func (h *BrExternalStorageHandle) ID() resourcemeta.ResourceID {
	return h.id
}

func (h *BrExternalStorageHandle) BrExternalStorage() brStorage.ExternalStorage {
	return h.inner
}

func (h *BrExternalStorageHandle) Persist(ctx context.Context) error {
	_, err := h.client.CreateResource(ctx, &pb.CreateResourceRequest{
		ResourceId:      h.id,
		CreatorExecutor: string(h.executorID),
		JobId:           h.jobID,
		CreatorWorkerId: h.workerID,
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (h *BrExternalStorageHandle) Discard(ctx context.Context) error {
	// TODO implement me
	return nil
}

type Factory struct {
	config     *storagecfg.Config
	client     pb.ResourceManagerClient
	executorID resourcemeta.ExecutorID
}

func (f *Factory) NewHandleForLocalFile(
	ctx context.Context,
	jobID resourcemeta.JobID,
	workerID resourcemeta.WorkerID,
	resourceID resourcemeta.ResourceID,
) (Handle, error) {
	tp, suffix, err := resourcemeta.ParseResourcePath(resourceID)
	if err != nil {
		return nil, err
	}
	if tp != resourcemeta.ResourceTypeLocalFile {
		log.L().Panic("unexpected resource type", zap.String("type", string(tp)))
	}

	record, exists, err := f.CheckForExistingResource(ctx, resourceID)
	if err != nil {
		return nil, err
	}

	var creatorWorkerID string
	if exists {
		creatorWorkerID = record.Worker
	} else {
		creatorWorkerID = workerID
	}
	filePath := filepath.Join(getWorkerDir(f.config, creatorWorkerID), suffix)
	log.L().Info("Using local storage with path", zap.String("path", filePath))

	backend, err := brStorage.ParseBackend(filePath, nil)
	if err != nil {
		return nil, err
	}
	ls, err := brStorage.New(ctx, backend, nil)
	if err != nil {
		return nil, err
	}

	return &BrExternalStorageHandle{
		inner:  ls,
		client: f.client,

		id:         resourceID,
		jobID:      jobID,
		workerID:   creatorWorkerID,
		executorID: f.executorID,
	}, nil
}

func (f *Factory) CheckForExistingResource(
	ctx context.Context,
	resourceID resourcemeta.ResourceID,
) (*resourcemeta.ResourceMeta, bool, error) {
	resp, err := f.client.QueryResource(ctx, &pb.QueryResourceRequest{ResourceId: resourceID})
	if err == nil {
		return &resourcemeta.ResourceMeta{
			ID:       resourceID,
			Job:      resp.GetJobId(),
			Worker:   resp.GetCreatorWorkerId(),
			Executor: resourcemeta.ExecutorID(resp.GetCreatorExecutor()),
			Deleted:  false,
		}, true, nil
	}

	// TODO perhaps we need a grpcutil package to put all this stuff?
	st, ok := status.FromError(err)
	if !ok {
		// If the error is not derived from a grpc status, we should throw it.
		return nil, false, errors.Trace(err)
	}
	if len(st.Details()) != 1 {
		// The resource manager only generates status with ONE detail.
		return nil, false, errors.Trace(err)
	}
	resourceErr, ok := st.Details()[0].(*pb.ResourceError)
	if !ok {
		return nil, false, errors.Trace(err)
	}

	log.L().Info("Got ResourceError",
		zap.String("resource-id", resourceID),
		zap.Any("resource-err", resourceErr))
	switch resourceErr.ErrorCode {
	case pb.ResourceErrorCode_ResourceNotFound:
		// Indicates that there is no existing resource with the same name.
		return nil, false, nil
	default:
		log.L().Warn("Unexpected ResourceError",
			zap.String("code", resourceErr.ErrorCode.String()),
			zap.String("stack-trace", resourceErr.StackTrace))
		return nil, false, errors.Trace(err)
	}
}

func getWorkerDir(config *storagecfg.Config, workerID resourcemeta.WorkerID) string {
	return filepath.Join(config.Local.BaseDir, workerID)
}
