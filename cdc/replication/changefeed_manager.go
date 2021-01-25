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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	_ "github.com/pingcap/ticdc/pkg/filter"
	"go.uber.org/zap"
)

type changeFeedManager interface {
	GetChangeFeedOperations(ctx context.Context) ([]*changeFeedOperation, error)
	GetGCSafePointLowerBound() int64
	AddAdminJob(job model.AdminJob)
}

type changeFeedOperationType = int

const (
	startChangeFeedOperation = changeFeedOperationType(iota)
	stopChangeFeedOperation
)

type changeFeedOperation struct {
	op           changeFeedOperationType
	changeFeedID model.ChangeFeedID
	err          error
	sink         sink.Sink
	ddlHandler   ddlHandler
}

type changeFeedManagerImpl struct {
	changeFeedInfos map[model.ChangeFeedID]*model.ChangeFeedInfo
	ownerState      *ownerReactorState
	adminJobsQueue  chan *model.AdminJob
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
			operation, err := m.handleAdminJob(adminJob)
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

func (m *changeFeedManagerImpl) AddAdminJob(job model.AdminJob) {
	panic("implement me")
}

func (m *changeFeedManagerImpl) GetGCSafePointLowerBound() int64 {
	panic("implement me")
}

func (m *changeFeedManagerImpl) startChangeFeed(ctx context.Context, cfID model.ChangeFeedID) (*changeFeedOperation, error) {
	primarySink, ddlHdlr, err := m.bootstrapChangeFeed(ctx, cfID)
	if err != nil {
		newErr := m.handleOwnerChangeFeedFailure(cfID, err)
		if newErr != nil {
			return nil, errors.Trace(newErr)
		}
		return nil, nil
	}


	return &changeFeedOperation{
		op:           startChangeFeedOperation,
		changeFeedID: cfID,
		sink:         primarySink,
		ddlHandler:   ddlHdlr,
	}, nil
}

func (m *changeFeedManagerImpl) bootstrapChangeFeed(ctx context.Context, cfID model.ChangeFeedID) (sink.Sink, ddlHandler, error) {
	panic("implement me")
}

func (m *changeFeedManagerImpl) handleOwnerChangeFeedFailure(cfID model.ChangeFeedID, err error) error {
	panic("implement me")
}

func (m *changeFeedManagerImpl) handleAdminJob(adminJob *model.AdminJob) (*changeFeedOperation, error) {
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


	}
}

func checkNeedStartChangeFeed(cfID model.ChangeFeedID, info *model.ChangeFeedInfo) bool {
	if info.Error == nil {
		return true
	}

	if info.State != model.StateNormal && info.State != model.StateStopped {
		return false
	}

	// TODO better error history checking and GC
	_, canInit := info.CheckErrorHistory()
	if canInit {
		return true
	}

	log.Debug("changeFeed should not be started", zap.String("changefeed-id", cfID))
	return false
}
