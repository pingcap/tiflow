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

	"github.com/gogo/status"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/storagecfg"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	derrors "github.com/pingcap/tiflow/pkg/errors"
)

// ResourceManagerClient is a type alias for a client connecting to
// the resource manager (which is part of the Servermaster).
type ResourceManagerClient = *rpcutil.FailoverRPCClients[pb.ResourceManagerClient]

// DefaultBroker implements the Broker interface
type DefaultBroker struct {
	config     *storagecfg.Config
	executorID resModel.ExecutorID
	client     ResourceManagerClient

	fileManager FileManager
}

// NewBroker creates a new Impl instance
func NewBroker(
	config *storagecfg.Config,
	executorID resModel.ExecutorID,
	client ResourceManagerClient,
) *DefaultBroker {
	fm := NewLocalFileManager(config.Local)
	return &DefaultBroker{
		config:      config,
		executorID:  executorID,
		client:      client,
		fileManager: fm,
	}
}

// OpenStorage implements Broker.OpenStorage
func (b *DefaultBroker) OpenStorage(
	ctx context.Context,
	workerID resModel.WorkerID,
	jobID resModel.JobID,
	resourcePath resModel.ResourceID,
) (Handle, error) {
	tp, _, err := resModel.ParseResourcePath(resourcePath)
	if err != nil {
		return nil, err
	}

	switch tp {
	case resModel.ResourceTypeLocalFile:
		return b.newHandleForLocalFile(ctx, jobID, workerID, resourcePath)
	case resModel.ResourceTypeS3:
		log.L().Panic("resource type s3 is not supported for now")
	default:
	}

	log.L().Panic("unsupported resource type", zap.String("resource-path", resourcePath))
	panic("unreachable")
}

// OnWorkerClosed implements Broker.OnWorkerClosed
func (b *DefaultBroker) OnWorkerClosed(ctx context.Context, workerID resModel.WorkerID, jobID resModel.JobID) {
	err := b.fileManager.RemoveTemporaryFiles(workerID)
	if err != nil {
		// TODO when we have a cloud-based error collection service, we need
		// to report this.
		// However, since an error here is unlikely to indicate a correctness
		// problem, we do not take further actions.
		log.L().Warn("Failed to remove temporary files for worker",
			zap.String("worker-id", workerID),
			zap.String("job-id", jobID),
			zap.Error(err))
	}
}

// RemoveResource implements pb.BrokerServiceServer.
func (b *DefaultBroker) RemoveResource(
	_ context.Context,
	request *pb.RemoveLocalResourceRequest,
) (*pb.RemoveLocalResourceResponse, error) {
	tp, resName, err := resModel.ParseResourcePath(request.GetResourceId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if tp != resModel.ResourceTypeLocalFile {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Sprintf("unexpected resource type %s", tp))
	}

	if request.GetCreatorId() == "" {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Sprintf("empty creatorID"))
	}

	err = b.fileManager.RemoveResource(request.GetCreatorId(), resName)
	if err != nil {
		if derrors.ErrResourceDoesNotExist.Equal(err) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Unknown, err.Error())
	}

	return &pb.RemoveLocalResourceResponse{}, nil
}

func (b *DefaultBroker) newHandleForLocalFile(
	ctx context.Context,
	jobID resModel.JobID,
	workerID resModel.WorkerID,
	resourceID resModel.ResourceID,
) (hdl Handle, retErr error) {
	// Note the semantics of ParseResourcePath:
	// If resourceID is `/local/my-resource`, then tp == resModel.ResourceTypeLocalFile
	// and resName == "my-resource".
	tp, resName, err := resModel.ParseResourcePath(resourceID)
	if err != nil {
		return nil, err
	}
	if tp != resModel.ResourceTypeLocalFile {
		log.L().Panic("unexpected resource type", zap.String("type", string(tp)))
	}

	record, exists, err := b.checkForExistingResource(ctx, resourceID)
	if err != nil {
		return nil, err
	}

	var desc *LocalFileResourceDescriptor

	if !exists {
		desc, err = b.fileManager.CreateResource(workerID, resName)
		if err != nil {
			return nil, err
		}
		defer func() {
			if retErr != nil {
				//nolint:errcheck
				_ = b.fileManager.RemoveResource(workerID, resName)
			}
		}()
	} else {
		desc, err = b.fileManager.GetPersistedResource(record.Worker, resName)
		if err != nil {
			return nil, err
		}
	}

	filePath := desc.AbsolutePath()
	log.L().Info("Using local storage with path", zap.String("path", filePath))

	return newLocalResourceHandle(resourceID, jobID, b.executorID, b.fileManager, desc, b.client)
}

func (b *DefaultBroker) checkForExistingResource(
	ctx context.Context,
	resourceID resModel.ResourceID,
) (*resModel.ResourceMeta, bool, error) {
	resp, err := rpcutil.DoFailoverRPC(
		ctx,
		b.client,
		&pb.QueryResourceRequest{ResourceId: resourceID},
		pb.ResourceManagerClient.QueryResource,
	)
	if err == nil {
		return &resModel.ResourceMeta{
			ID:       resourceID,
			Job:      resp.GetJobId(),
			Worker:   resp.GetCreatorWorkerId(),
			Executor: resModel.ExecutorID(resp.GetCreatorExecutor()),
			Deleted:  false,
		}, true, nil
	}

	// TODO perhaps we need a grpcutil package to put all this stuff?
	st, ok := status.FromError(err)
	if !ok {
		// If the error is not derived from a grpc status, we should throw it.
		return nil, false, errors.Trace(err)
	}

	switch st.Code() {
	case codes.NotFound:
		// Indicates that there is no existing resource with the same name.
		return nil, false, nil
	default:
		return nil, false, errors.Trace(err)
	}
}
