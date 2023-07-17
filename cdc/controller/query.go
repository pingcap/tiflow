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

package controller

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// QueryType is the type of different queries.
type QueryType int32

const (
	// QueryAllChangeFeedStatuses query all changefeed status.
	QueryAllChangeFeedStatuses QueryType = iota
	// QueryAllChangeFeedInfo is the type of query all changefeed info.
	QueryAllChangeFeedInfo
	// QueryCaptures is the type of query captures info.
	QueryCaptures
)

// Query wraps query command and return results.
type Query struct {
	Tp           QueryType
	ChangeFeedID model.ChangeFeedID

	Data interface{}
}

// GetCaptures returns the information about all captures.
func (o *controllerImpl) GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error) {
	query := &Query{
		Tp: QueryCaptures,
	}
	if err := o.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.([]*model.CaptureInfo), nil
}

func (o *controllerImpl) GetAllChangeFeedInfo(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedInfo, error,
) {
	query := &Query{
		Tp: QueryAllChangeFeedInfo,
	}
	if err := o.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.ChangeFeedID]*model.ChangeFeedInfo), nil
}

// Query queries owner internal information.
func (o *controllerImpl) Query(query *Query, done chan<- error) {
	o.pushOwnerJob(&controllerJob{
		Tp:    controllerJobTypeQuery,
		query: query,
		done:  done,
	})
}

func (o *controllerImpl) pushOwnerJob(job *controllerJob) {
	o.controllerJobQueue.Lock()
	defer o.controllerJobQueue.Unlock()
	if atomic.LoadInt32(&o.closed) != 0 {
		log.Info("reject owner job as controller has been closed",
			zap.Int("jobType", int(job.Tp)))
		select {
		case job.done <- cerror.ErrOwnerNotFound.GenWithStackByArgs():
		default:
		}
		close(job.done)
		return
	}
	o.controllerJobQueue.queue = append(o.controllerJobQueue.queue, job)
}

func (o *controllerImpl) sendQueryToOwner(ctx context.Context, query *Query) error {
	doneCh := make(chan error, 1)
	o.Query(query, doneCh)

	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-doneCh:
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (o *controllerImpl) takeControllerJobs() []*controllerJob {
	o.controllerJobQueue.Lock()
	defer o.controllerJobQueue.Unlock()

	jobs := o.controllerJobQueue.queue
	o.controllerJobQueue.queue = nil
	return jobs
}

func (o *controllerImpl) handleJobs(_ context.Context) {
	jobs := o.takeControllerJobs()
	for _, job := range jobs {
		switch job.Tp {
		case controllerJobTypeQuery:
			job.done <- o.handleQueries(job.query)
		}
		close(job.done)
	}
}

func (o *controllerImpl) handleQueries(query *Query) error {
	switch query.Tp {
	case QueryAllChangeFeedInfo:
		ret := map[model.ChangeFeedID]*model.ChangeFeedInfo{}
		for cfID, cfReactor := range o.changefeeds {
			if cfReactor.Info == nil {
				ret[cfID] = &model.ChangeFeedInfo{}
				continue
			}
			var err error
			ret[cfID], err = cfReactor.Info.Clone()
			if err != nil {
				return errors.Trace(err)
			}
		}
		query.Data = ret
	case QueryCaptures:
		var ret []*model.CaptureInfo
		for _, captureInfo := range o.captures {
			ret = append(ret, &model.CaptureInfo{
				ID:            captureInfo.ID,
				AdvertiseAddr: captureInfo.AdvertiseAddr,
				Version:       captureInfo.Version,
			})
		}
		query.Data = ret
	}
	return nil
}
