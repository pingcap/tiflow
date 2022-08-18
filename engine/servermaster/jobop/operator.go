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
	"go.uber.org/zap"
	"golang.org/x/net/context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
)

type messageHandleProvider interface {
	getWorkerHandle(jobID string) (framework.WorkerHandle, bool)
}

// JobOperator abstracts a metastore based job operator, it encapsulates logic
// to handle JobOp and a Tick API to ensure job moves towards to expected status.
type JobOperator interface {
	MarkJobCanceled(ctx context.Context, jobID string) error
	Tick(ctx context.Context) error
}

// JobOperatorImpl implements JobOperator
type JobOperatorImpl struct {
	frameMetaClient   pkgOrm.Client
	msgHandleProvider messageHandleProvider
}

// MarkJobCanceled implements JobOperator.MarkJobCanceled
func (oper *JobOperatorImpl) MarkJobCanceled(ctx context.Context, jobID string) error {
	result, err := oper.frameMetaClient.SetJobCanceled(ctx, jobID)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		log.Info("job is already marked as canceled", zap.String("job-id", jobID))
	}
	return nil
}

// Tick implements JobOperator.Tick
func (oper *JobOperatorImpl) Tick(ctx context.Context) error {
	ops, err := oper.frameMetaClient.QueryJobOpsByStatus(ctx, ormModel.JobOpStatusCanceling)
	if err != nil {
		return err
	}
	for _, op := range ops {
		_, ok := oper.msgHandleProvider.getWorkerHandle(op.JobID)
		if !ok {
			log.Warn("worker handle not found", zap.String("job-id", op.JobID))
			continue
		}
		// TODO: send cancel job message to online job master
	}
	return nil
}
