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
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/local"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal/s3"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/ratelimit"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	gcCheckInterval          = 10 * time.Second
	gcTimeout                = 10 * time.Second
	gcOnceRetryMinIntervalMs = int64(100)
	gcOnceRetryMaxIntervalMs = int64(100)

	gcExecutorsTimeout       = 600 * time.Second
	gcExecutorsRateLimit     = 1 /* once per second*/
	gcExecutorsMinIntervalMs = int64(100)
	gcExecutorsMaxIntervalMs = int64(30 * time.Second)
)

var _ GCRunner = (*DefaultGCRunner)(nil)

// DefaultGCRunner implements GCRunner.
type DefaultGCRunner struct {
	client     pkgOrm.ResourceClient
	gcHandlers map[resModel.ResourceType]internal.ResourceController
	notifyCh   chan struct{}

	clock clock.Clock
}

// NewGCRunner returns a new GCRunner.
func NewGCRunner(
	resClient pkgOrm.ResourceClient,
	executorClients client.ExecutorGroup,
	config *resModel.Config,
) *DefaultGCRunner {
	gcRunner := &DefaultGCRunner{
		client:     resClient,
		gcHandlers: map[resModel.ResourceType]internal.ResourceController{},
		notifyCh:   make(chan struct{}, 1),
		clock:      clock.New(),
	}
	if executorClients != nil {
		localType := resModel.ResourceTypeLocalFile
		gcRunner.gcHandlers[localType] = local.NewFileResourceController(executorClients)
	}
	if config != nil && config.S3Enabled() {
		gcRunner.gcHandlers[resModel.ResourceTypeS3] = s3.NewResourceController(config.S3)
	}
	return gcRunner
}

// Run runs the GCRunner. It blocks until ctx is canceled.
func (r *DefaultGCRunner) Run(ctx context.Context) error {
	defer func() {
		log.Info("default gc runner exited")
	}()
	// TODO this will result in DB queries every 10 seconds.
	// This is a very naive strategy, we will modify the
	// algorithm after doing enough system testing.
	ticker := r.clock.Ticker(gcCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
		case <-r.notifyCh:
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, gcTimeout)
		err := r.gcOnceWithRetry(timeoutCtx)
		cancel()

		if err != nil {
			log.Warn("resource GC encountered error", zap.Error(err))
		}
	}
}

// GCNotify is used to ask GCRunner to GC the next resource immediately.
// It is used when we have just marked a resource as gc_pending.
func (r *DefaultGCRunner) GCNotify() {
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

func (r *DefaultGCRunner) gcOnceWithRetry(ctx context.Context) error {
	return retry.Do(ctx, func() error {
		return r.gcOnce(ctx)
	},
		retry.WithBackoffBaseDelay(gcOnceRetryMinIntervalMs),
		retry.WithBackoffMaxDelay(gcOnceRetryMaxIntervalMs),
	)
}

func (r *DefaultGCRunner) gcOnce(
	ctx context.Context,
) error {
	res, err := r.client.GetOneResourceForGC(ctx)
	if pkgOrm.IsNotFoundError(err) {
		// It is expected that sometimes we have
		// nothing to GC.
		return nil
	}
	if err != nil {
		return err
	}

	log.Info("start gc'ing resource", zap.Any("resource", res))
	if !res.GCPending {
		log.Panic("unexpected gc_pending = false")
	}

	tp, _, err := resModel.ParseResourceID(res.ID)
	if err != nil {
		return err
	}

	handler, exists := r.gcHandlers[tp]
	if !exists {
		log.Warn("no gc handler is found for given resource type",
			zap.Any("resource-id", res.ID))
		// Return nil here for potential backward compatibility when we do
		// rolling upgrades online.
		return nil
	}

	if err := handler.GCSingleResource(ctx, res); err != nil {
		st := status.Convert(err)
		if st.Code() != codes.NotFound {
			return err
		}
		// remove resource rpc returns resource not found, ignore this error and
		// continue to delete resource from resourcemeta
		log.Info("remove resource rpc returns resource not found, which is ignorable", zap.Error(err))
	}

	result, err := r.client.DeleteResource(ctx, pkgOrm.ResourceKey{JobID: res.Job, ID: res.ID})
	if err != nil {
		log.Warn("Failed to delete resource meta after GC",
			zap.Any("resource", res),
			zap.Error(err))
		return err
	}
	if result.RowsAffected() == 0 {
		log.Warn("Resource is deleted unexpectedly", zap.Any("resource", res))
	}

	return nil
}

// GCExecutors is used to GC executors.
//
// For local file resource, we need to remove the meta record, since executors
// going offline means that the resource is already gone.
//
// For s3 resource, we need to remove all temporary resources created by the
// offline exectors to avoid resource leaks. Note dummy meta record created by
// such exectors should be removed after temporary files are cleared.
//
// FIXME: we should a periodic background cleaning policy to avoid affecting
// normal services.
func (r *DefaultGCRunner) GCExecutors(ctx context.Context, executors ...model.ExecutorID) error {
	// The total retry time is set to 10min to alleviate the impact to normal request.
	// Note that if this function returns an error, the leader will exit.
	ctx, cancel := context.WithTimeout(ctx, gcExecutorsTimeout)
	defer cancel()

	if err := r.mustCleanupLocalExecutors(ctx, executors); err != nil {
		return err
	}
	return r.mustCleanupS3Executors(ctx, executors)
}

func (r *DefaultGCRunner) mustCleanupLocalExecutors(
	ctx context.Context, executors []model.ExecutorID,
) error {
	metaCtx, cancel := context.WithTimeout(ctx, gcTimeout)
	defer cancel()
	// Remove the meta record for local file resource.
	return retry.Do(metaCtx, func() error {
		// Note: soft delete has not been implemented for resources yet.
		_, err := r.client.DeleteResourcesByTypeAndExecutorIDs(ctx,
			resModel.ResourceTypeLocalFile, executors...)
		if err != nil {
			return err
		}
		log.Info("local file meta records are removed", zap.Any("executors", executors))
		return nil
	}, retry.WithBackoffBaseDelay(gcExecutorsMinIntervalMs),
		retry.WithBackoffMaxDelay(gcExecutorsMaxIntervalMs))
}

func (r *DefaultGCRunner) mustCleanupS3Executors(
	ctx context.Context, executors []model.ExecutorID,
) error {
	s3Handler, exists := r.gcHandlers[resModel.ResourceTypeS3]
	if !exists {
		return nil
	}

	gcOnce := func(id model.ExecutorID) (err error) {
		defer func() {
			if err != nil {
				log.Warn("failed to cleanup s3 temporary resources for executor",
					zap.Any("executor-id", id), zap.Error(err))
			}
		}()
		log.Info("start to clean up executor", zap.Any("executor", id))
		// Get persistent s3 resource
		resources, err := r.client.QueryResourcesByExecutorIDs(ctx, id)
		if err != nil {
			return err
		}
		if err := s3Handler.GCExecutor(ctx, resources, id); err != nil {
			return err
		}

		// Remove s3 dummy meta record
		_, err = r.client.DeleteResource(ctx, s3.GetDummyResourceKey(id))
		if err != nil {
			return err
		}
		log.Info("finish cleaning up single executor", zap.Any("executor", id))
		return nil
	}

	// Cleanup one executor per second for avoiding too many requests to s3.
	// The rate limit takes effect only when initialing gcCoordinator.
	rl := ratelimit.New(gcExecutorsRateLimit)
	for _, executor := range executors {
		rl.Take()
		err := retry.Do(ctx, func() error {
			return gcOnce(executor)
		}, retry.WithBackoffBaseDelay(gcExecutorsMinIntervalMs),
			retry.WithBackoffMaxDelay(gcExecutorsMaxIntervalMs))
		if err != nil {
			return err
		}
	}
	log.Info("all executores' s3 temporary files are removed", zap.Any("executors", executors))
	return nil
}
