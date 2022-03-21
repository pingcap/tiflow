package broker

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pb"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
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
	config *storagecfg.Config
	client pb.ResourceManagerClient
}

func (f *Factory) NewHandleForLocalFile(
	ctx context.Context,
	jobID resourcemeta.JobID,
	workerID resourcemeta.WorkerID,
	resourceID resourcemeta.ResourceID,
) (Handle, error) {
	pathSuffix, err := getPathSuffix(resourcemeta.ResourceTypeLocalFile, resourceID)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(getWorkerDir(f.config, workerID), pathSuffix)
	log.L().Info("Using local storage with path", zap.String("path", filePath))

	ls, err := brStorage.NewLocalStorage(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &BrExternalStorageHandle{
		inner:  ls,
		client: f.client,

		id:       resourceID,
		jobID:    jobID,
		workerID: workerID,
	}, nil
}

func getPathSuffix(prefix resourcemeta.ResourceType, path resourcemeta.ResourceID) (string, error) {
	if !strings.HasPrefix(path, "/"+string(prefix)+"/") {
		return "", derror.ErrUnexpectedResourcePath.GenWithStackByArgs(path)
	}
	return strings.TrimPrefix(path, "/"+string(prefix)+"/"), nil
}

func getWorkerDir(config *storagecfg.Config, workerID resourcemeta.WorkerID) string {
	return filepath.Join(config.Local.BaseDir, workerID)
}
