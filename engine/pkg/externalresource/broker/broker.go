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

package broker

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	derrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DefaultBroker must implement Broker.
var _ Broker = (*DefaultBroker)(nil)

// DefaultBroker implements the Broker interface
type DefaultBroker struct {
	config     *resModel.Config
	executorID resModel.ExecutorID
	client     client.ResourceManagerClient

	fileManagers map[resModel.ResourceType]internal.FileManager
}

// NewBroker creates a new Impl instance
func NewBroker(
	config *resModel.Config,
	executorID resModel.ExecutorID,
	client client.ResourceManagerClient,
) *DefaultBroker {
	log.Info("Create new resource broker",
		zap.String("executor-id", string(executorID)),
		zap.Any("config", config))

	fileManagers := make(map[resModel.ResourceType]internal.FileManager)
	fileManagers[resModel.ResourceTypeLocalFile] = local.NewLocalFileManager(executorID, config.Local)
	if config.S3Enabled() {
		log.Info("S3 is enabled")
		// TODO：add s3 file manager
		// fileManagers[resModel.ResourceTypeS3] = s3.NewFileManagerWithConfig(executorID, config.S3)
	} else {
		log.Info("S3 config is not complete, will not use s3 as external storage")
	}

	return &DefaultBroker{
		config:       config,
		executorID:   executorID,
		client:       client,
		fileManagers: fileManagers,
	}
}

// OpenStorage implements Broker.OpenStorage
func (b *DefaultBroker) OpenStorage(
	ctx context.Context,
	projectInfo tenant.ProjectInfo,
	workerID resModel.WorkerID,
	jobID resModel.JobID,
	resID resModel.ResourceID,
) (Handle, error) {
	// Note the semantics of ParseResourcePath:
	// If resourceID is `/local/my-resource`, then tp == resModel.ResourceTypeLocalFile
	// and resName == "my-resource".
	tp, resName, err := resModel.GenResourcePath(resID)
	if err != nil {
		return nil, err
	}

	fm, ok := b.fileManagers[tp]
	if !ok {
		log.Panic("unexpected resource type", zap.String("type", string(tp)))
	}

	record, exists, err := b.checkForExistingResource(ctx,
		resModel.ResourceKey{JobID: jobID, ID: resID})
	if err != nil {
		return nil, err
	}

	var desc internal.ResourceDescriptor
	if !exists {
		desc, err = b.createResource(ctx, fm, projectInfo, workerID, resName)
	} else {
		desc, err = b.getPersistResource(ctx, fm, record, resName)
	}
	if err != nil {
		return nil, err
	}

	log.Info(fmt.Sprintf("Using %s storage with path", string(tp)),
		zap.String("path", desc.URI()))
	return newResourceHandle(jobID, b.executorID, fm, desc, exists, b.client)
}

func (b *DefaultBroker) createResource(
	ctx context.Context, fm internal.FileManager,
	projectInfo tenant.ProjectInfo, workerID resModel.WorkerID,
	resName resModel.ResourceName,
) (internal.ResourceDescriptor, error) {
	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			ProjectInfo: projectInfo,
			Executor:    b.executorID, /* executor id where resource is created */
			WorkerID:    workerID,     /* creator id*/
		},
	}
	desc, err := fm.CreateResource(ctx, ident)
	if err != nil {
		//nolint:errcheck
		_ = fm.RemoveResource(ctx, ident)
		return nil, err
	}
	return desc, nil
}

// OnWorkerClosed implements Broker.OnWorkerClosed
func (b *DefaultBroker) OnWorkerClosed(ctx context.Context, workerID resModel.WorkerID, jobID resModel.JobID) {
	scope := internal.ResourceScope{
		Executor: b.executorID,
		WorkerID: workerID,
	}
	for _, fm := range b.fileManagers {
		err := fm.RemoveTemporaryFiles(ctx, scope)
		if err != nil {
			// TODO when we have a cloud-based error collection service, we need
			// to report this.
			// However, since an error here is unlikely to indicate a correctness
			// problem, we do not take further actions.
			log.Warn("Failed to remove temporary files for worker",
				zap.String("worker-id", workerID),
				zap.String("job-id", jobID),
				zap.Error(err))
		}
	}
}

// RemoveResource implements pb.BrokerServiceServer.
func (b *DefaultBroker) RemoveResource(
	ctx context.Context,
	request *pb.RemoveLocalResourceRequest,
) (*pb.RemoveLocalResourceResponse, error) {
	tp, resName, err := resModel.GenResourcePath(request.GetResourceId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if tp != resModel.ResourceTypeLocalFile {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Sprintf("unexpected resource type %s", tp))
	}

	fm := b.fileManagers[tp]
	if request.GetCreatorId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty creatorID")
	}

	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			Executor: b.executorID,
			WorkerID: request.GetCreatorId(),
		},
	}
	err = fm.RemoveResource(ctx, ident)
	if err != nil {
		if derrors.ErrResourceDoesNotExist.Equal(err) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &pb.RemoveLocalResourceResponse{}, nil
}

func (b *DefaultBroker) checkForExistingResource(
	ctx context.Context,
	resourceKey resModel.ResourceKey,
) (*resModel.ResourceMeta, bool, error) {
	request := &pb.QueryResourceRequest{
		ResourceKey: &pb.ResourceKey{
			JobId:      resourceKey.JobID,
			ResourceId: resourceKey.ID,
		},
	}
	resp, err := b.client.QueryResource(ctx, request)
	if err == nil {
		return &resModel.ResourceMeta{
			ID:       resourceKey.ID,
			Job:      resp.GetJobId(),
			Worker:   resp.GetCreatorWorkerId(),
			Executor: resModel.ExecutorID(resp.GetCreatorExecutor()),
			Deleted:  false,
		}, true, nil
	}

	code, ok := rpcerror.GRPCStatusCode(err)
	if !ok {
		// If the error is not derived from a grpc status, we should throw it.
		return nil, false, errors.Trace(err)
	}

	switch code {
	case codes.NotFound:
		// Indicates that there is no existing resource with the same name.
		return nil, false, nil
	default:
		return nil, false, errors.Trace(err)
	}
}

func (b *DefaultBroker) getPersistResource(
	ctx context.Context, fm internal.FileManager,
	record *resModel.ResourceMeta,
	resName resModel.ResourceName,
) (internal.ResourceDescriptor, error) {
	ident := internal.ResourceIdent{
		Name: resName,
		ResourceScope: internal.ResourceScope{
			ProjectInfo: tenant.NewProjectInfo("", record.ProjectID),
			Executor:    record.Executor, /* executor id where the resource is persisted */
			WorkerID:    record.Worker,   /* creator id*/
		},
	}
	return fm.GetPersistedResource(ctx, ident)
}

// PreCheckConfig does a preflight check on the executor's storage configurations.
func PreCheckConfig(config resModel.Config) error {
	return local.PreCheckConfig(config)
}
