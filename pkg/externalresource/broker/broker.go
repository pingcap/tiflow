package broker

import (
	"context"
	"strings"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	"github.com/hanfei1991/microcosm/pkg/externalresource/storagecfg"
)

type Broker interface {
	OpenStorage(
		ctx context.Context,
		workerID resourcemeta.WorkerID,
		jobID resourcemeta.JobID,
		resourcePath resourcemeta.ResourceID,
	) (Handle, error)
	OnWorkerClosed(
		ctx context.Context,
		workerID resourcemeta.WorkerID,
		jobID resourcemeta.JobID,
	)
}

type Impl struct {
	config     *storagecfg.Config
	executorID resourcemeta.ExecutorID

	factory *Factory
}

func NewBroker(config *storagecfg.Config, executorID resourcemeta.ExecutorID, client pb.ResourceManagerClient) *Impl {
	return &Impl{
		config:     config,
		executorID: executorID,
		factory: &Factory{
			config: config,
			client: client,
		},
	}
}

func (i *Impl) OpenStorage(
	ctx context.Context,
	workerID resourcemeta.WorkerID,
	jobID resourcemeta.JobID,
	resourcePath resourcemeta.ResourceID,
) (Handle, error) {
	if strings.HasPrefix(resourcePath, "/"+string(resourcemeta.ResourceTypeLocalFile)+"/") {
		return i.factory.NewHandleForLocalFile(ctx, jobID, workerID, resourcePath)
	}
	log.L().Panic("unsupported resource type", zap.String("resource-path", resourcePath))
	// TODO implement S3 support
	panic("unreachable")
}

func (i *Impl) OnWorkerClosed(ctx context.Context, workerID resourcemeta.WorkerID, jobID resourcemeta.JobID) {
	panic("implement me")
}
