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

package manager

import (
	"context"
	"time"

	"github.com/pingcap/log"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/s3"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
)

var _ GCCoordinator = &DefaultGCCoordinator{}

// DefaultGCCoordinator implements interface GCCoordinator.
// It is responsible for triggering file resource garbage collection.
type DefaultGCCoordinator struct {
	executorInfos ExecutorInfoProvider
	jobInfos      JobStatusProvider
	metaClient    pkgOrm.ResourceClient
	gcRunner      GCRunner
}

// NewGCCoordinator creates a new DefaultGCCoordinator.
func NewGCCoordinator(
	executorInfos ExecutorInfoProvider,
	jobInfos JobStatusProvider,
	metaClient pkgOrm.ResourceClient,
	gcRunner GCRunner,
) *DefaultGCCoordinator {
	return &DefaultGCCoordinator{
		executorInfos: executorInfos,
		jobInfos:      jobInfos,
		metaClient:    metaClient,
		gcRunner:      gcRunner,
	}
}

// Run runs the DefaultGCCoordinator.
func (c *DefaultGCCoordinator) Run(ctx context.Context) error {
	defer func() {
		log.Info("default gc coordinator exited")
	}()
	// We run a retry loop at the max frequency of once per second.
	rl := ratelimit.New(1 /* once per second */)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		// add unit test for initializeGC
		jobReceiver, executorReceiver, err := c.initializeGC(ctx)
		if err != nil {
			log.Warn("GC error", zap.Error(err))
			rl.Take()
			continue
		}

		err = c.runGCEventLoop(ctx, jobReceiver.C, executorReceiver.C)
		jobReceiver.Close()
		executorReceiver.Close()

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return errors.Trace(err)
		}

		// TODO collect the error for observability.
		log.Warn("Error running GC coordinator. Retrying...", zap.Error(err))

		rl.Take()
	}
}

// OnKeepAlive is not implemented for now.
func (c *DefaultGCCoordinator) OnKeepAlive(resourceID resModel.ResourceID, workerID frameModel.WorkerID) {
	// TODO implement me
	panic("implement me")
}

func (c *DefaultGCCoordinator) runGCEventLoop(
	ctx context.Context,
	jobWatchCh <-chan JobStatusChangeEvent,
	executorWatchCh <-chan model.ExecutorStatusChange,
) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case jobStatusChange := <-jobWatchCh:
			if jobStatusChange.EventType != JobRemovedEvent {
				continue
			}
			err := c.gcByOfflineJobIDs(ctx, jobStatusChange.JobID)
			if err != nil {
				return err
			}
		case executorEvent := <-executorWatchCh:
			if executorEvent.Tp != model.EventExecutorOffline {
				continue
			}
			err := c.gcByOfflineExecutorIDs(ctx, executorEvent.ID)
			if err != nil {
				return err
			}
		}
	}
}

func (c *DefaultGCCoordinator) initializeGC(
	ctx context.Context,
) (*notifier.Receiver[JobStatusChangeEvent], *notifier.Receiver[model.ExecutorStatusChange], error) {
	// we must query meta before get snapshot, otherwise temporary resources created
	// by a newly added executor or all resources created by a newly added job may
	// be cleaned incorrectly.
	resources, err := c.metaClient.QueryResources(ctx)
	if err != nil {
		return nil, nil, err
	}

	jobSnapshot, jobWatchCh, err := c.jobInfos.WatchJobStatuses(ctx)
	if err != nil {
		return nil, nil, err
	}

	executorSnapshot, executorReceiver, err := c.executorInfos.WatchExecutors(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err := c.gcByStatusSnapshots(ctx, resources, jobSnapshot, executorSnapshot); err != nil {
		return nil, nil, err
	}

	return jobWatchCh, executorReceiver, nil
}

func (c *DefaultGCCoordinator) gcByStatusSnapshots(
	ctx context.Context,
	resources []*resModel.ResourceMeta,
	jobSnapshot JobStatusesSnapshot,
	executorSnapshot map[model.ExecutorID]string,
) error {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		log.Info("gcByStatusSnapshots finished",
			zap.Duration("duration", duration))
	}()

	executorSet := make(map[model.ExecutorID]struct{}, len(executorSnapshot))
	for id := range executorSnapshot {
		executorSet[id] = struct{}{}
	}

	toGCJobSet := make(map[model.JobID]struct{})
	toGCExecutorSet := make(map[model.ExecutorID]struct{})
	for _, resMeta := range resources {
		if _, exists := jobSnapshot[resMeta.Job]; !exists {
			// The resource belongs to a deleted job.
			toGCJobSet[resMeta.Job] = struct{}{}
			continue
		}

		if _, exists := executorSet[resMeta.Executor]; !exists {
			// The resource belongs to an offlined executor.
			tp, resName, err := resModel.ParseResourceID(resMeta.ID)
			if err != nil {
				return err
			}
			if tp == resModel.ResourceTypeLocalFile ||
				tp == resModel.ResourceTypeS3 && resName == s3.GetDummyResourceName() {
				toGCExecutorSet[resMeta.Executor] = struct{}{}
			}
			continue
		}
	}

	toGCJobs := make([]model.JobID, 0, len(toGCJobSet))
	for jobID := range toGCJobSet {
		toGCJobs = append(toGCJobs, jobID)
	}
	if err := c.gcByOfflineJobIDs(ctx, toGCJobs...); err != nil {
		return err
	}

	toGCExecutors := make([]model.ExecutorID, 0, len(toGCExecutorSet))
	for executorID := range toGCExecutorSet {
		toGCExecutors = append(toGCExecutors, executorID)
	}
	return c.gcByOfflineExecutorIDs(ctx, toGCExecutors...)
}

func (c *DefaultGCCoordinator) gcByOfflineJobIDs(
	ctx context.Context, jobIDs ...string,
) error {
	if len(jobIDs) == 0 {
		return nil
	}

	log.Info("Added resources to GC queue since job offline", zap.Any("jobIDs", jobIDs))
	if err := c.metaClient.SetGCPendingByJobs(ctx, jobIDs...); err != nil {
		return err
	}

	c.gcRunner.GCNotify()
	return nil
}

func (c *DefaultGCCoordinator) gcByOfflineExecutorIDs(
	ctx context.Context, executorIDs ...model.ExecutorID,
) error {
	if len(executorIDs) == 0 {
		return nil
	}

	log.Info("Clean up offlined executors", zap.Any("executorIDs", executorIDs))
	return c.gcRunner.GCExecutors(ctx, executorIDs...)
}
