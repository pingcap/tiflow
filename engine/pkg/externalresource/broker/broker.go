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

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/pb"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/storagecfg"
)

type Broker interface {
	OpenStorage(
		ctx context.Context,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
		resourcePath resModel.ResourceID,
	) (Handle, error)
	OnWorkerClosed(
		ctx context.Context,
		workerID resModel.WorkerID,
		jobID resModel.JobID,
	)
}

type Impl struct {
	config     *storagecfg.Config
	executorID resModel.ExecutorID

	factory *Factory
}

func NewBroker(
	config *storagecfg.Config,
	executorID resModel.ExecutorID,
	client *rpcutil.FailoverRPCClients[pb.ResourceManagerClient],
) *Impl {
	return &Impl{
		config:     config,
		executorID: executorID,
		factory: &Factory{
			config:     config,
			client:     client,
			executorID: executorID,
		},
	}
}

func (i *Impl) OpenStorage(
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
		return i.factory.NewHandleForLocalFile(ctx, jobID, workerID, resourcePath)
	case resModel.ResourceTypeS3:
		log.L().Panic("resource type s3 is not supported for now")
	default:
	}

	log.L().Panic("unsupported resource type", zap.String("resource-path", resourcePath))
	panic("unreachable")
}

func (i *Impl) OnWorkerClosed(ctx context.Context, workerID resModel.WorkerID, jobID resModel.JobID) {
	panic("implement me")
}
