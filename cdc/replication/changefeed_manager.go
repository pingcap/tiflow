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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
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

}

func (m *changeFeedManagerImpl) bootstrapChangeFeed(ctx context.Context, cfID model.ChangeFeedID, cfInfo *model.ChangeFeedInfo) (sink.Sink, ddlHandler, error) {
	panic("implement me")
}

func (m *changeFeedManagerImpl) GetGCSafePointLowerBound() int64 {
	panic("implement me")
}

func (m *changeFeedManagerImpl) AddAdminJob(job model.AdminJob) {
	panic("implement me")
}
