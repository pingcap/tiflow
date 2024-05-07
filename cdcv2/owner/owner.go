// Copyright 2023 PingCAP, Inc.
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

package owner

import (
	"context"
	"database/sql"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	msql "github.com/pingcap/tiflow/cdcv2/metadata/sql"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Owner implements the owner interface.
type Owner struct {
	upstreamManager    *upstream.Manager
	captureObservation *msql.CaptureOb[*gorm.DB]
	cfg                *config.SchedulerConfig
	storage            *sql.DB

	liveness *model.Liveness

	ownerJobQueue struct {
		sync.Mutex
		queue []*ownerJob
	}
	closed int32

	querier metadata.Querier
}

// UpdateChangefeedAndUpstream updates the changefeed info and upstream info.
func (o *Owner) UpdateChangefeedAndUpstream(ctx context.Context,
	upstreamInfo *model.UpstreamInfo,
	changeFeedInfo *model.ChangeFeedInfo,
) error {
	panic("implement me")
}

// UpdateChangefeed updates the changefeed info.
func (o *Owner) UpdateChangefeed(ctx context.Context,
	changeFeedInfo *model.ChangeFeedInfo,
) error {
	panic("implement me")
}

// EnqueueJob enqueues a job to the owner.
func (o *Owner) EnqueueJob(adminJob model.AdminJob,
	done chan<- error,
) {
	o.pushOwnerJob(&ownerJob{
		Tp:           ownerJobTypeAdminJob,
		AdminJob:     &adminJob,
		ChangefeedID: adminJob.CfID,
		done:         done,
	})
}

// RebalanceTables rebalances the tables of a changefeed.
func (o *Owner) RebalanceTables(cfID model.ChangeFeedID,
	done chan<- error,
) {
	o.pushOwnerJob(&ownerJob{
		Tp:           ownerJobTypeRebalance,
		ChangefeedID: cfID,
		done:         done,
	})
}

// ScheduleTable schedules a table to a capture.
func (o *Owner) ScheduleTable(cfID model.ChangeFeedID,
	toCapture model.CaptureID,
	tableID model.TableID, done chan<- error,
) {
	o.pushOwnerJob(&ownerJob{
		Tp:              ownerJobTypeScheduleTable,
		ChangefeedID:    cfID,
		TargetCaptureID: toCapture,
		TableID:         tableID,
		done:            done,
	})
}

// DrainCapture drains a capture.
func (o *Owner) DrainCapture(query *scheduler.Query,
	done chan<- error,
) {
	o.pushOwnerJob(&ownerJob{
		Tp:            ownerJobTypeDrainCapture,
		scheduleQuery: query,
		done:          done,
	})
}

// WriteDebugInfo writes the debug info to the writer.
func (o *Owner) WriteDebugInfo(w io.Writer,
	done chan<- error,
) {
	o.pushOwnerJob(&ownerJob{
		Tp:              ownerJobTypeDebugInfo,
		debugInfoWriter: w,
		done:            done,
	})
}

// Query queries owner internal information.
func (o *Owner) Query(query *owner.Query, done chan<- error) {
	o.pushOwnerJob(&ownerJob{
		Tp:    ownerJobTypeQuery,
		query: query,
		done:  done,
	})
}

// AsyncStop stops the owner asynchronously.
func (o *Owner) AsyncStop() {
	panic("implement me")
}

// NewOwner creates a new owner.
func NewOwner(
	liveness *model.Liveness,
	upstreamManager *upstream.Manager,
	cfg *config.SchedulerConfig,
	captureObservation *msql.CaptureOb[*gorm.DB],
	querier metadata.Querier,
	storage *sql.DB,
) *Owner {
	return &Owner{
		upstreamManager:    upstreamManager,
		captureObservation: captureObservation,
		cfg:                cfg,
		querier:            querier,
		storage:            storage,
		liveness:           liveness,
	}
}

// Run runs the owner.
func (o *Owner) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			// handleJobs() should be called before clusterVersionConsistent(), because
			// when there are different versions of cdc nodes in the cluster,
			// the admin job may not be processed all the time. And http api relies on
			// admin job, which will cause all http api unavailable.
			o.handleJobs(ctx)
		case cf := <-o.captureObservation.OwnerChanges():
			switch cf.OwnerState {
			case metadata.SchedRemoving:
			case metadata.SchedLaunched:
			}
		}
	}
}

// nolint:unused
type ownerInfoClient struct {
	ownerID  model.CaptureID
	captures []*model.CaptureInfo
}

// nolint:unused
func (o *ownerInfoClient) GetOwnerID(context.Context) (model.CaptureID, error) {
	return o.ownerID, nil
}

// nolint:unused
func (o *ownerInfoClient) GetOwnerRevision(context.Context, model.CaptureID) (int64, error) {
	return 0, nil
}

// nolint:unused
func (o *ownerInfoClient) GetCaptures(context.Context) (int64, []*model.CaptureInfo, error) {
	return 0, o.captures, nil
}

func (o *Owner) handleJobs(_ context.Context) {
	jobs := o.takeOwnerJobs()
	for _, job := range jobs {
		switch job.Tp {
		case ownerJobTypeAdminJob:
		case ownerJobTypeScheduleTable:
		case ownerJobTypeDrainCapture:
			// todo: drain capture
			// o.handleDrainCaptures(ctx, job.scheduleQuery, job.done)
			continue // continue here to prevent close the done channel twice
		case ownerJobTypeRebalance:
			// Scheduler is created lazily, it is nil before initialization.
		case ownerJobTypeQuery:
			job.done <- o.handleQueries(job.query)
		case ownerJobTypeDebugInfo:
			// TODO: implement this function
		}
		close(job.done)
	}
}

// nolint
func (o *Owner) handleQueries(query *owner.Query) error {
	switch query.Tp {
	case owner.QueryChangeFeedStatuses:
	case owner.QueryProcessors:
	case owner.QueryHealth:
		query.Data = o.isHealthy()
	case owner.QueryOwner:
	case owner.QueryChangefeedInfo:
	}
	return nil
}

func (o *Owner) isHealthy() bool {
	return false
}

func (o *Owner) takeOwnerJobs() []*ownerJob {
	o.ownerJobQueue.Lock()
	defer o.ownerJobQueue.Unlock()

	jobs := o.ownerJobQueue.queue
	o.ownerJobQueue.queue = nil
	return jobs
}

func (o *Owner) pushOwnerJob(job *ownerJob) {
	o.ownerJobQueue.Lock()
	defer o.ownerJobQueue.Unlock()
	if atomic.LoadInt32(&o.closed) != 0 {
		log.Info("reject owner job as owner has been closed",
			zap.Int("jobType", int(job.Tp)))
		select {
		case job.done <- cerror.ErrOwnerNotFound.GenWithStackByArgs():
		default:
		}
		close(job.done)
		return
	}
	o.ownerJobQueue.queue = append(o.ownerJobQueue.queue, job)
}

// nolint:unused
func (o *Owner) cleanupOwnerJob() {
	log.Info("cleanup owner jobs as owner has been closed")
	jobs := o.takeOwnerJobs()
	for _, job := range jobs {
		select {
		case job.done <- cerror.ErrOwnerNotFound.GenWithStackByArgs():
		default:
		}
		close(job.done)
	}
}

type ownerJobType int

// All OwnerJob types
const (
	ownerJobTypeRebalance ownerJobType = iota
	ownerJobTypeScheduleTable
	ownerJobTypeDrainCapture
	ownerJobTypeAdminJob
	ownerJobTypeDebugInfo
	ownerJobTypeQuery
)

// Export field names for pretty printing.
type ownerJob struct {
	Tp           ownerJobType
	ChangefeedID model.ChangeFeedID

	// for ScheduleTable only
	TargetCaptureID model.CaptureID
	// for ScheduleTable only
	TableID model.TableID

	// for Admin Job only
	AdminJob *model.AdminJob

	// for debug info only
	debugInfoWriter io.Writer

	// for status provider
	query *owner.Query

	// for scheduler related jobs
	scheduleQuery *scheduler.Query

	done chan<- error
}
