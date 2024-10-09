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

package master

import (
	"context"
	"time"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"go.uber.org/zap"
)

const genEpochTimeout = 5 * time.Second

type createWorkerOpts struct {
	Resources []resModel.ResourceID
	Selectors []*label.Selector
}

// CreateWorkerOpt represents an option of creating a worker.
type CreateWorkerOpt func(opts *createWorkerOpts)

// CreateWorkerWithResourceRequirements specifies the resource requirement of a worker.
func CreateWorkerWithResourceRequirements(resources ...resModel.ResourceID) CreateWorkerOpt {
	return func(opts *createWorkerOpts) {
		opts.Resources = append(opts.Resources, resources...)
	}
}

// CreateWorkerWithSelectors specifies the selectors used to dispatch the worker.
func CreateWorkerWithSelectors(selectors ...*label.Selector) CreateWorkerOpt {
	return func(opts *createWorkerOpts) {
		opts.Selectors = append(opts.Selectors, selectors...)
	}
}

// StartWorkerCallbackType is the type for a callback that's called before the
// executor launches the worker. It is useful in the 2-phase task submission process.
type StartWorkerCallbackType = func(
	workerID frameModel.WorkerID,
	executorID model.ExecutorID,
	epoch frameModel.Epoch,
)

// WorkerCreationHooks contains hooks to be called at specifc points of the
// worker creation process.
type WorkerCreationHooks struct {
	// BeforeStartingWorker will be called AFTER the executor has received the task
	// but BEFORE the worker is actually started.
	BeforeStartingWorker StartWorkerCallbackType
}

// WorkerCreator implements the worker creation logic.
type WorkerCreator struct {
	masterID           frameModel.MasterID
	executorGroup      client.ExecutorGroup
	serverMasterClient client.ServerMasterClient
	frameMetaClient    pkgOrm.Client
	logger             *zap.Logger
	hook               *WorkerCreationHooks

	inheritedSelectors []*label.Selector
}

// CreateWorker creates a worker synchronously.
func (c *WorkerCreator) CreateWorker(
	ctx context.Context,
	projectInfo tenant.ProjectInfo,
	workerType frameModel.WorkerType,
	workerID frameModel.WorkerID,
	rawConfig []byte,
	opts ...CreateWorkerOpt,
) error {
	options := new(createWorkerOpts)
	for _, op := range opts {
		op(options)
	}

	req, err := c.buildScheduleTaskRequest(c.masterID, workerID, options)
	if err != nil {
		return err
	}

	resp, err := c.serverMasterClient.ScheduleTask(ctx, req)
	if err != nil {
		c.logger.Warn("ScheduleTask returned error", zap.Error(err))
		return err
	}
	c.logger.Info("ScheduleTask succeeded", zap.Any("response", resp))

	executorID := model.ExecutorID(resp.ExecutorId)
	executorClient, err := c.executorGroup.GetExecutorClientB(ctx, executorID)
	if err != nil {
		return errors.Annotate(err, "CreateWorker")
	}

	genEpochCtx, cancel := context.WithTimeout(ctx, genEpochTimeout)
	defer cancel()
	epoch, err := c.frameMetaClient.GenEpoch(genEpochCtx)
	if err != nil {
		return errors.Annotate(err, "CreateWorker")
	}

	dispatchArgs := &client.DispatchTaskArgs{
		ProjectInfo:  projectInfo,
		WorkerID:     workerID,
		MasterID:     c.masterID,
		WorkerType:   int64(workerType),
		WorkerConfig: rawConfig,
		WorkerEpoch:  epoch,
	}

	err = executorClient.DispatchTask(ctx, dispatchArgs, func() {
		c.hook.BeforeStartingWorker(workerID, executorID, epoch)
	})
	if err != nil {
		c.logger.Info("DispatchTask failed",
			zap.Error(err))
		return err
	}

	c.logger.Info("Dispatch Worker succeeded",
		zap.Any("args", dispatchArgs))
	return nil
}

func (c *WorkerCreator) buildScheduleTaskRequest(
	masterID frameModel.MasterID,
	workerID frameModel.WorkerID,
	opts *createWorkerOpts,
) (*pb.ScheduleTaskRequest, error) {
	finalSelectors := make([]*label.Selector, 0, len(c.inheritedSelectors)+len(opts.Selectors))
	finalSelectors = append(finalSelectors, c.inheritedSelectors...)
	finalSelectors = append(finalSelectors, opts.Selectors...)

	selectors, err := toPBSelectors(finalSelectors...)
	if err != nil {
		return nil, err
	}

	return &pb.ScheduleTaskRequest{
		TaskId:    workerID,
		Resources: resModel.ToResourceRequirement(masterID, opts.Resources...),
		Selectors: selectors,
	}, nil
}

func toPBSelectors(sels ...*label.Selector) ([]*pb.Selector, error) {
	if len(sels) == 0 {
		return nil, nil
	}

	ret := make([]*pb.Selector, 0, len(sels))
	for _, sel := range sels {
		pbSel, err := schedModel.SelectorToPB(sel)
		if err != nil {
			return nil, err
		}
		ret = append(ret, pbSel)
	}

	return ret, nil
}

// WorkerCreatorBuilder is a helper for building a WorkerCreator.
type WorkerCreatorBuilder struct {
	creator *WorkerCreator
}

// NewWorkerCreatorBuilder returns a new WorkerCreatorBuilder.
func NewWorkerCreatorBuilder() *WorkerCreatorBuilder {
	return &WorkerCreatorBuilder{creator: new(WorkerCreator)}
}

// WithMasterID specifies the ID of the master that the WorkerCreator belongs to.
func (b *WorkerCreatorBuilder) WithMasterID(masterID frameModel.MasterID) *WorkerCreatorBuilder {
	b.creator.masterID = masterID
	return b
}

// WithExecutorGroup passes an ExecutorGroup.
func (b *WorkerCreatorBuilder) WithExecutorGroup(executorGroup client.ExecutorGroup) *WorkerCreatorBuilder {
	b.creator.executorGroup = executorGroup
	return b
}

// WithServerMasterClient passes a ServerMasterClient.
func (b *WorkerCreatorBuilder) WithServerMasterClient(serverMasterClient client.ServerMasterClient) *WorkerCreatorBuilder {
	b.creator.serverMasterClient = serverMasterClient
	return b
}

// WithFrameMetaClient passes a frameMetaClient.
func (b *WorkerCreatorBuilder) WithFrameMetaClient(frameMetaClient pkgOrm.Client) *WorkerCreatorBuilder {
	b.creator.frameMetaClient = frameMetaClient
	return b
}

// WithLogger passes a logger.
func (b *WorkerCreatorBuilder) WithLogger(logger *zap.Logger) *WorkerCreatorBuilder {
	b.creator.logger = logger
	return b
}

// WithHooks passes a WorkerCreationHooks.
func (b *WorkerCreatorBuilder) WithHooks(hooks *WorkerCreationHooks) *WorkerCreatorBuilder {
	b.creator.hook = hooks
	return b
}

// WithInheritedSelectors passes the selectors that will be inherited from the master.
func (b *WorkerCreatorBuilder) WithInheritedSelectors(selectors ...*label.Selector) *WorkerCreatorBuilder {
	b.creator.inheritedSelectors = selectors
	return b
}

// Build returns the resulted WorkerCreator.
func (b *WorkerCreatorBuilder) Build() *WorkerCreator {
	return b.creator
}
