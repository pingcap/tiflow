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

package jobop

import (
	"github.com/pingcap/log"
	frameworkModel "github.com/pingcap/tiflow/engine/framework/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type operateRouter interface {
	SendCancelJobMessage(ctx context.Context, jobID string) error
}

// JobOperator abstracts a metastore based job operator, it encapsulates logic
// to handle JobOp and a Tick API to ensure job moves towards to expected status.
type JobOperator interface {
	MarkJobCanceling(ctx context.Context, jobID string) error
	MarkJobCanceled(ctx context.Context, jobID string) error
	Tick(ctx context.Context) error
	IsJobCanceling(ctx context.Context, jobID string) bool
}

// JobOperatorImpl implements JobOperator
type JobOperatorImpl struct {
	frameMetaClient pkgOrm.Client
	router          operateRouter
}

// NewJobOperatorImpl creates a new JobOperatorImpl
func NewJobOperatorImpl(cli pkgOrm.Client, router operateRouter) *JobOperatorImpl {
	return &JobOperatorImpl{
		frameMetaClient: cli,
		router:          router,
	}
}

func (oper *JobOperatorImpl) updateJobOperationStatus(
	ctx context.Context, jobID string, op ormModel.JobOpStatus,
) error {
	var ormFn func(ctx context.Context, JobID string) (pkgOrm.Result, error)
	switch op {
	case ormModel.JobOpStatusNoop:
		ormFn = oper.frameMetaClient.SetJobNoop
	case ormModel.JobOpStatusCanceling:
		ormFn = oper.frameMetaClient.SetJobCanceling
	case ormModel.JobOpStatusCanceled:
		ormFn = oper.frameMetaClient.SetJobCanceled
	default:
		log.Panic("unexpected job operate", zap.Any("op", op))
	}
	if result, err := ormFn(ctx, jobID); err != nil {
		return err
	} else if result.RowsAffected() == 0 {
		log.Info("job status is already set", zap.String("job-id", jobID), zap.Any("op", op))
	}
	return nil
}

// MarkJobNoop implements JobOperator.MarkJobNoop
func (oper *JobOperatorImpl) MarkJobNoop(ctx context.Context, jobID string) error {
	return oper.updateJobOperationStatus(ctx, jobID, ormModel.JobOpStatusNoop)
}

// MarkJobCanceling implements JobOperator.MarkJobCanceling
func (oper *JobOperatorImpl) MarkJobCanceling(ctx context.Context, jobID string) error {
	return oper.updateJobOperationStatus(ctx, jobID, ormModel.JobOpStatusCanceling)
}

// MarkJobCanceled implements JobOperator.MarkJobCanceled
func (oper *JobOperatorImpl) MarkJobCanceled(ctx context.Context, jobID string) error {
	return oper.updateJobOperationStatus(ctx, jobID, ormModel.JobOpStatusCanceled)
}

// Tick implements JobOperator.Tick
func (oper *JobOperatorImpl) Tick(ctx context.Context) error {
	ops, err := oper.frameMetaClient.QueryJobOpsByStatus(ctx, ormModel.JobOpStatusCanceling)
	if err != nil {
		return err
	}
	var errs error
	for _, op := range ops {
		isJobTerminated, err := oper.checkJobStatus(ctx, op.JobID)
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}
		if isJobTerminated {
			continue
		}
		if err := oper.router.SendCancelJobMessage(ctx, op.JobID); err != nil {
			log.Warn("send cancel message to job master failed",
				zap.String("job-id", op.JobID), zap.Error(err))
		}
	}
	return errs
}

// IsJobCanceling implements JobOperator
func (oper *JobOperatorImpl) IsJobCanceling(ctx context.Context, jobID string) bool {
	op, err := oper.frameMetaClient.QueryJobOp(ctx, jobID)
	if err != nil {
		if !pkgOrm.IsNotFoundError(err) {
			log.Warn("failed to query job canceling state", zap.Error(err))
		}
		return false
	}
	return op.Op == ormModel.JobOpStatusCanceling
}

// check job status, if job is in terminated, return true, otherwise return false
// and the upper logic needs to send canceling message. Return value
// - whether job is in terminated state
// - error
func (oper *JobOperatorImpl) checkJobStatus(
	ctx context.Context, jobID string,
) (bool, error) {
	isJobTerminated := false
	meta, err := oper.frameMetaClient.GetJobByID(ctx, jobID)
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			log.Warn("found orphan job operation", zap.String("job-id", jobID))
			isJobTerminated = true
			return isJobTerminated, oper.MarkJobNoop(ctx, jobID)
		}
		return isJobTerminated, err
	}
	switch meta.State {
	case frameworkModel.MasterStateFinished,
		frameworkModel.MasterStateStopped, frameworkModel.MasterStateFailed:
		isJobTerminated = true
		return isJobTerminated, oper.MarkJobCanceled(ctx, jobID)
	}
	return isJobTerminated, nil
}
