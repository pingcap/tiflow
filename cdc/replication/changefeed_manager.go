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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/filter"
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
}

func (m *changeFeedManagerImpl) GetChangeFeedOperations(ctx context.Context) ([]*changeFeedOperation, error) {
	newChangeFeedInfos := m.ownerState.ChangeFeedInfos
	var changeFeedOperations []*changeFeedOperation

	for cfID, info := range newChangeFeedInfos {
		oldInfo, ok := m.changeFeedInfos[cfID]
		if !ok {
			m.changeFeedInfos[cfID] = oldInfo
			primarySink, ddlHdlr, err := m.bootstrapChangeFeed(ctx, cfID)
			if err != nil {
				newErr := m.handleOwnerChangeFeedFailure(cfID, err)
				if newErr != nil {
					return nil, errors.Trace(newErr)
				}
				// goto the next change-feed
				continue
			}

			changeFeedOperations = append(changeFeedOperations, &changeFeedOperation{
				op:           startChangeFeedOperation,
				changeFeedID: cfID,
				sink:         primarySink,
				ddlHandler:   ddlHdlr,
			})
		}
	}
}

func (m *changeFeedManagerImpl) AddAdminJob(job model.AdminJob) {
	panic("implement me")
}

func (m *changeFeedManagerImpl) GetGCSafePointLowerBound() int64 {
	panic("implement me")
}

func (m *changeFeedManagerImpl) bootstrapChangeFeed(ctx context.Context, cfID model.ChangeFeedID) (sink.Sink, ddlHandler, error) {
	panic("implement me")
}

func (m *changeFeedManagerImpl) handleOwnerChangeFeedFailure(cfID model.ChangeFeedID, err error) error {
	panic("implement me")
}

func checkNeedStartChangeFeed(oldInfo *model.ChangeFeedInfo, newInfo *model.ChangeFeedID) bool {
	
}
