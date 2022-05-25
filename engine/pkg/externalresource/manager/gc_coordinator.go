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
	gerrors "errors"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
)

// DefaultGCCoordinator implements interface GCCoordinator.
// It is responsible for triggering file resource garbage collection.
type DefaultGCCoordinator struct {
	executorInfos ExecutorInfoProvider
	jobInfos      JobStatusProvider
	metaClient    pkgOrm.ResourceClient
	notifier      GCNotifier
}

// GCNotifier exposes a GCNotify method which is called
// when a new object is marked as needing GC.
type GCNotifier interface {
	GCNotify()
}

// NewGCCoordinator creates a new DefaultGCCoordinator.
func NewGCCoordinator(
	executorInfos ExecutorInfoProvider,
	jobInfos JobStatusProvider,
	metaClient pkgOrm.ResourceClient,
	gcNotifier GCNotifier,
) *DefaultGCCoordinator {
	return &DefaultGCCoordinator{
		executorInfos: executorInfos,
		jobInfos:      jobInfos,
		metaClient:    metaClient,
		notifier:      gcNotifier,
	}
}

// Run runs the DefaultGCCoordinator.
func (c *DefaultGCCoordinator) Run(ctx context.Context) error {
	// We run a retry loop at the max frequency of once per second.
	rl := ratelimit.New(1 /* once per second */)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}

		jobReceiver, executorReceiver, err := c.initializeGC(ctx)
		if err != nil {
			log.L().Warn("GC error", zap.Error(err))
			rl.Take()
			continue
		}

		err = c.runGCEventLoop(ctx, jobReceiver.C, executorReceiver.C)
		jobReceiver.Close()
		executorReceiver.Close()

		if gerrors.Is(err, context.Canceled) || gerrors.Is(err, context.DeadlineExceeded) {
			return errors.Trace(err)
		}

		// TODO collect the error for observability.
		log.L().Warn("Error running GC coordinator. Retrying...", zap.Error(err))

		rl.Take()
	}
}

// OnKeepAlive is not implemented for now.
func (c *DefaultGCCoordinator) OnKeepAlive(resourceID resModel.ResourceID, workerID libModel.WorkerID) {
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
			err := c.gcByOfflineJobID(ctx, jobStatusChange.JobID)
			if err != nil {
				return err
			}
		case executorEvent := <-executorWatchCh:
			if executorEvent.Tp != model.EventExecutorOffline {
				continue
			}
			err := c.gcByOfflineExecutorID(ctx, executorEvent.ID)
			if err != nil {
				return err
			}
		}
	}
}

func (c *DefaultGCCoordinator) initializeGC(
	ctx context.Context,
) (*notifier.Receiver[JobStatusChangeEvent], *notifier.Receiver[model.ExecutorStatusChange], error) {
	jobSnapshot, jobWatchCh, err := c.jobInfos.WatchJobStatuses(ctx)
	if err != nil {
		return nil, nil, err
	}

	executorSnapshot, executorReceiver, err := c.executorInfos.WatchExecutors(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err := c.gcByStatusSnapshots(ctx, jobSnapshot, executorSnapshot); err != nil {
		return nil, nil, err
	}

	return jobWatchCh, executorReceiver, nil
}

func (c *DefaultGCCoordinator) gcByStatusSnapshots(
	ctx context.Context,
	jobSnapshot JobStatusesSnapshot,
	executorSnapshot []model.ExecutorID,
) error {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		log.L().Info("gcByStatusSnapshots finished",
			zap.Duration("duration", duration))
	}()

	resources, err := c.metaClient.QueryResources(ctx)
	if err != nil {
		return err
	}

	executorSet := make(map[model.ExecutorID]struct{}, len(executorSnapshot))
	for _, id := range executorSnapshot {
		executorSet[id] = struct{}{}
	}

	var (
		toGC []resModel.ResourceID

		// toRemove is used to remove meta records when
		// the associated executors are offline.
		// TODO adjust the mechanism when we implement S3 support.
		toRemove []resModel.ResourceID
	)
	for _, resMeta := range resources {
		if _, exists := jobSnapshot[resMeta.Job]; !exists {
			// The resource belongs to a deleted job.
			toGC = append(toGC, resMeta.ID)
			continue
		}

		if _, exists := executorSet[resMeta.Executor]; !exists {
			// The resource belongs to an offlined executor.
			toRemove = append(toGC, resMeta.ID)
			continue
		}
	}

	if len(toGC) > 0 {
		log.L().Info("Adding resources to GC queue",
			zap.Any("resource-ids", toGC))
		if err := c.metaClient.SetGCPending(ctx, toGC); err != nil {
			return err
		}
		c.notifier.GCNotify()
	}

	if len(toRemove) > 0 {
		log.L().Info("Removing stale resources for offlined executors",
			zap.Any("resource-ids", toRemove))
		// Note: soft delete has not been implemented for resources yet.
		if _, err := c.metaClient.DeleteResources(ctx, toRemove); err != nil {
			return err
		}
	}

	return nil
}

func (c *DefaultGCCoordinator) gcByOfflineJobID(ctx context.Context, jobID string) error {
	resources, err := c.metaClient.QueryResourcesByJobID(ctx, jobID)
	if err != nil {
		return err
	}

	if len(resources) == 0 {
		// If there is no resource associated to the job,
		// we return early.
		return nil
	}

	toGC := make([]resModel.ResourceID, 0, len(resources))
	for _, resMeta := range resources {
		toGC = append(toGC, resMeta.ID)
	}

	log.L().Info("Added resources to GC queue",
		zap.Any("resource-ids", toGC))

	if err := c.metaClient.SetGCPending(ctx, toGC); err != nil {
		return err
	}

	c.notifier.GCNotify()
	return nil
}

func (c *DefaultGCCoordinator) gcByOfflineExecutorID(ctx context.Context, executorID model.ExecutorID) error {
	log.L().Info("Cleaning up resources meta for offlined executor",
		zap.String("executor-id", string(executorID)))

	// Currently, we only support local files, so the resources are bound to
	// the executors. Hence, executors going offline means that the resource is
	// already gone.
	// TODO Trigger GC for all resources and let the GCRunner decide whether to
	// perform any action, or just remove the meta record.
	return c.metaClient.DeleteResourcesByExecutorID(ctx, string(executorID))
}
