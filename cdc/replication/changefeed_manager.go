// Copyright 2021 PingCAP, Inc.
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

package replication

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	_ "github.com/pingcap/ticdc/pkg/filter"
	"go.uber.org/zap"
)

type changeFeedManager interface {
	// GetChangeFeedOperations checks for new change-feeds and returns changeFeedOperations to be executed.
	GetChangeFeedOperations(ctx context.Context) ([]*changeFeedOperation, error)
	// GetGCSafePointUpperBound returns a lower bound of GC service safe point. This lower bound has to be used
	// with the checkpointTs of the running changefeeds to produce a correct safe point.
	GetGCSafePointUpperBound() uint64
	// AddAdminJob is called by the HTTP handler to initiate an admin-job.
	AddAdminJob(ctx context.Context, job model.AdminJob) error
}

type changeFeedOperationType = int

const (
	startChangeFeedOperation = changeFeedOperationType(iota)
	stopChangeFeedOperation
)

type changeFeedOperation struct {
	op           changeFeedOperationType
	changeFeedID model.ChangeFeedID
	runner       changeFeedRunner
}

type changeFeedManagerImpl struct {
	changeFeedInfos map[model.ChangeFeedID]*model.ChangeFeedInfo
	ownerState      *ownerReactorState
	bootstrapper    changeFeedBootstrapper

	adminJobsQueue chan *model.AdminJob
}

func newChangeFeedManager(ownerState *ownerReactorState, bootstrapper changeFeedBootstrapper) changeFeedManager {
	return &changeFeedManagerImpl{
		changeFeedInfos: make(map[model.ChangeFeedID]*model.ChangeFeedInfo),
		ownerState:      ownerState,
		bootstrapper:    bootstrapper,
		adminJobsQueue:  make(chan *model.AdminJob, 1024),
	}
}

func (m *changeFeedManagerImpl) GetChangeFeedOperations(ctx context.Context) ([]*changeFeedOperation, error) {
	newChangeFeedInfos := m.ownerState.ChangeFeedInfos
	var changeFeedOperations []*changeFeedOperation

	// handle newly found change-feeds
	for cfID, info := range newChangeFeedInfos {
		_, ok := m.changeFeedInfos[cfID]
		m.changeFeedInfos[cfID] = info
		if !ok {
			if !checkNeedStartChangeFeed(cfID, info) {
				continue
			}

			operation, err := m.startChangeFeed(ctx, cfID)
			if err != nil {
				return nil, err
			}

			if operation == nil {
				// We encountered a retryable error
				continue
			}

			changeFeedOperations = append(changeFeedOperations, operation)
		}
	}

	// handle admin jobs
loop:
	for {
		select {
		case adminJob := <-m.adminJobsQueue:
			operation, err := m.handleAdminJob(ctx, adminJob)
			if err != nil {
				return nil, err
			}

			changeFeedOperations = append(changeFeedOperations, operation)
		default:
			break loop
		}
	}

	return changeFeedOperations, nil
}

func (m *changeFeedManagerImpl) AddAdminJob(ctx context.Context, job model.AdminJob) error {
	// TODO some verification on the job? Since this function will be called by the HTTP handler.
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case m.adminJobsQueue <- &job:
	}

	log.Info("AdminJob accepted and queued", zap.Reflect("job", job))
	return nil
}

func (m *changeFeedManagerImpl) GetGCSafePointUpperBound() uint64 {
	upperBound := uint64(math.MaxUint64)

	for cfID, cfInfo := range m.changeFeedInfos {
		cfStatus, ok := m.ownerState.ChangeFeedStatuses[cfID]
		if !ok {
			if upperBound > cfInfo.StartTs {
				upperBound = cfInfo.StartTs
			}
		} else {
			if upperBound > cfStatus.CheckpointTs {
				upperBound = cfStatus.CheckpointTs
			}
		}
	}

	return upperBound
}

func (m *changeFeedManagerImpl) startChangeFeed(ctx context.Context, cfID model.ChangeFeedID) (*changeFeedOperation, error) {
	startTs := m.changeFeedInfos[cfID].StartTs
	if cfStatus, ok := m.ownerState.ChangeFeedStatuses[cfID]; ok {
		log.Info("found existing changeFeed progress info", zap.Uint64("old-start-ts", startTs),
			zap.Uint64("adjusted-start-ts", cfStatus.CheckpointTs))
		startTs = cfStatus.CheckpointTs
	}

	log.Info("startChangeFeed", zap.String("cfID", cfID), zap.Uint64("startTs", startTs))
	runner, err := m.bootstrapper.bootstrapChangeFeed(ctx, cfID, m.changeFeedInfos[cfID], startTs)
	if err != nil {
		newErr := m.handleOwnerChangeFeedFailure(cfID, err)
		// if newErr != nil, it means that the original error is not ignorable
		if newErr != nil {
			return nil, errors.Trace(newErr)
		}
		return nil, nil
	}

	runner.SetOwnerState(m.ownerState)
	err = runner.InitTables(ctx, startTs)
	if err != nil {
		log.Warn("Could not initialize tables", zap.Error(err))
		return nil, nil
	}

	return &changeFeedOperation{
		op:           startChangeFeedOperation,
		changeFeedID: cfID,
		runner:       runner,
	}, nil
}

func (m *changeFeedManagerImpl) handleOwnerChangeFeedFailure(cfID model.ChangeFeedID, err error) error {
	log.Warn("changeFeed error", zap.String("cfID", cfID), zap.Error(err))
	return err
}

func (m *changeFeedManagerImpl) handleAdminJob(ctx context.Context, adminJob *model.AdminJob) (*changeFeedOperation, error) {
	log.Info("handle admin job", zap.Reflect("admin-job", adminJob))

	cfInfo, ok := m.changeFeedInfos[adminJob.CfID]
	if !ok {
		return nil, cerrors.ErrChangeFeedNotExists.GenWithStackByArgs(adminJob.CfID)
	}

	switch adminJob.Type {
	case model.AdminNone:
		return nil, nil

	case model.AdminStop:
		if cfInfo.State != model.StateNormal {
			log.Info("AdminStop ignored because change-feed is not running",
				zap.Reflect("admin-job", adminJob))
			return nil, nil
		}

		m.ownerState.AlterChangeFeedRuntimeState(
			adminJob.CfID, adminJob.Type, model.StateStopped, adminJob.Error, time.Now().UnixNano()/1e6)

		return &changeFeedOperation{
			op:           stopChangeFeedOperation,
			changeFeedID: adminJob.CfID,
		}, nil

	case model.AdminResume:
		if cfInfo.State == model.StateNormal {
			log.Info("AdminResume ignored because change-feed is already running",
				zap.Reflect("admin-job", adminJob))
			return nil, nil
		}

		if cfInfo.State != model.StateStopped {
			log.Warn("AdminResume failed because change-feed state is not normal",
				zap.Reflect("admin-job", adminJob), zap.String("change-feed-state", string(cfInfo.State)))
			return nil, nil
		}

		m.ownerState.AlterChangeFeedRuntimeState(
			adminJob.CfID, adminJob.Type, model.StateNormal, nil, time.Now().UnixNano()/1e6)

		operation, err := m.startChangeFeed(ctx, adminJob.CfID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return operation, err

	case model.AdminFinish:
		log.Debug("AdminFinish received", zap.Reflect("admin-job", adminJob))

	default:
		log.Warn("Unknown adminJob type", zap.Int("type", int(adminJob.Type)))
		return nil, cerrors.ErrInvalidAdminJobType.GenWithStackByArgs(adminJob.Type)
	}

	panic("unreachable")
}

func checkNeedStartChangeFeed(cfID model.ChangeFeedID, info *model.ChangeFeedInfo) bool {
	if info.Error == nil {
		return true
	}

	if info.State != model.StateNormal && info.State != model.StateStopped {
		return false
	}

	i := sort.Search(len(info.ErrorHis), func(i int) bool {
		ts := info.ErrorHis[i]
		return time.Since(time.Unix(ts/1e3, (ts%1e3)*1e6)) < model.ErrorHistoryCheckInterval
	})
	canInit := len(info.ErrorHis)-i < model.ErrorHistoryThreshold

	if !canInit {
		log.Debug("changeFeedState should not be started due to too many errors", zap.String("changefeed-id", cfID))
	}
	return canInit
}
