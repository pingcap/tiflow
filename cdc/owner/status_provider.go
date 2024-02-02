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

package owner

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// StatusProvider provide some func to get meta-information from owner
// The interface is thread-safe.
type StatusProvider interface {
	// GetAllChangeFeedStatuses returns all changefeeds' runtime status.
	GetAllChangeFeedStatuses(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI, error)

	// GetChangeFeedStatus returns a changefeeds' runtime status.
	GetChangeFeedStatus(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedStatusForAPI, error)

	// GetAllChangeFeedInfo returns all changefeeds' info.
	GetAllChangeFeedInfo(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedInfo, error)

	// GetChangeFeedSyncedStatus returns a changefeeds' synced status.
	GetChangeFeedSyncedStatus(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedSyncedStatusForAPI, error)

	// GetChangeFeedInfo returns a changefeeds' info.
	GetChangeFeedInfo(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedInfo, error)

	// GetAllTaskStatuses returns the task statuses for the specified changefeed.
	GetAllTaskStatuses(ctx context.Context, changefeedID model.ChangeFeedID) (map[model.CaptureID]*model.TaskStatus, error)

	// GetProcessors returns the statuses of all processors
	GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error)

	// GetCaptures returns the information about all captures.
	GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error)

	// IsHealthy return true if the cluster is healthy
	IsHealthy(ctx context.Context) (bool, error)
	// IsChangefeedOwner return true if this capture is the owner of the changefeed
	IsChangefeedOwner(ctx context.Context, id model.ChangeFeedID) (bool, error)
}

// QueryType is the type of different queries.
type QueryType int32

const (
	// QueryAllChangeFeedStatuses query all changefeed status.
	QueryAllChangeFeedStatuses QueryType = iota
	// QueryAllChangeFeedInfo is the type of query all changefeed info.
	QueryAllChangeFeedInfo
	// QueryAllTaskStatuses is the type of query all task statuses.
	QueryAllTaskStatuses
	// QueryProcessors is the type of query processors.
	QueryProcessors
	// QueryCaptures is the type of query captures info.
	QueryCaptures
	// QueryHealth is the type of query cluster health info.
	QueryHealth
	// QueryOwner is the type of query changefeed owner
	QueryOwner
	// QueryChangeFeedSyncedStatus is the type of query changefeed synced status
	QueryChangeFeedSyncedStatus
)

// Query wraps query command and return results.
type Query struct {
	Tp           QueryType
	ChangeFeedID model.ChangeFeedID

	Data interface{}
}

// NewStatusProvider returns a new StatusProvider for the owner.
func NewStatusProvider(owner Owner) StatusProvider {
	return &ownerStatusProvider{owner: owner}
}

type ownerStatusProvider struct {
	owner Owner
}

func (p *ownerStatusProvider) GetAllChangeFeedStatuses(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI, error,
) {
	query := &Query{
		Tp: QueryAllChangeFeedStatuses,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.ChangeFeedID]*model.ChangeFeedStatusForAPI), nil
}

func (p *ownerStatusProvider) GetChangeFeedStatus(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedStatusForAPI, error) {
	statuses, err := p.GetAllChangeFeedStatuses(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	status, exist := statuses[changefeedID]
	if !exist {
		return nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID)
	}
	return status, nil
}

func (p *ownerStatusProvider) GetAllChangeFeedInfo(ctx context.Context) (
	map[model.ChangeFeedID]*model.ChangeFeedInfo, error,
) {
	query := &Query{
		Tp: QueryAllChangeFeedInfo,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.ChangeFeedID]*model.ChangeFeedInfo), nil
}

func (p *ownerStatusProvider) GetChangeFeedSyncedStatus(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedSyncedStatusForAPI, error) {
	query := &Query{
		Tp:           QueryChangeFeedSyncedStatus,
		ChangeFeedID: changefeedID,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	if query.Data == nil {
		return nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID)
	}
	return query.Data.(*model.ChangeFeedSyncedStatusForAPI), nil
}

func (p *ownerStatusProvider) GetChangeFeedInfo(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedInfo, error) {
	infos, err := p.GetAllChangeFeedInfo(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info, exist := infos[changefeedID]
	if !exist {
		return nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID)
	}
	return info, nil
}

func (p *ownerStatusProvider) GetAllTaskStatuses(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (map[model.CaptureID]*model.TaskStatus, error) {
	query := &Query{
		Tp:           QueryAllTaskStatuses,
		ChangeFeedID: changefeedID,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.CaptureID]*model.TaskStatus), nil
}

func (p *ownerStatusProvider) GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error) {
	query := &Query{
		Tp: QueryProcessors,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.([]*model.ProcInfoSnap), nil
}

func (p *ownerStatusProvider) GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error) {
	query := &Query{
		Tp: QueryCaptures,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.([]*model.CaptureInfo), nil
}

func (p *ownerStatusProvider) IsHealthy(ctx context.Context) (bool, error) {
	query := &Query{
		Tp: QueryHealth,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return false, errors.Trace(err)
	}
	return query.Data.(bool), nil
}

func (p *ownerStatusProvider) IsChangefeedOwner(ctx context.Context, id model.ChangeFeedID) (bool, error) {
	query := &Query{
		Tp:           QueryOwner,
		ChangeFeedID: id,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return false, errors.Trace(err)
	}
	return query.Data.(bool), nil
}

func (p *ownerStatusProvider) sendQueryToOwner(ctx context.Context, query *Query) error {
	doneCh := make(chan error, 1)
	p.owner.Query(query, doneCh)

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
