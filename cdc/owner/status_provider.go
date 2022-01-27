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

	cerror "github.com/pingcap/tiflow/pkg/errors"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
)

// StatusProvider provide some func to get meta-information from owner
// The interface is thread-safe.
type StatusProvider interface {
	// GetAllChangeFeedStatuses returns all changefeeds' runtime status.
	GetAllChangeFeedStatuses(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedStatus, error)

	// GetChangeFeedStatus returns a changefeeds' runtime status.
	GetChangeFeedStatus(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedStatus, error)

	// GetAllChangeFeedInfo returns all changefeeds' info.
	GetAllChangeFeedInfo(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedInfo, error)

	// GetChangeFeedInfo returns a changefeeds' info.
	GetChangeFeedInfo(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedInfo, error)

	// GetAllTaskStatuses returns the task statuses for the specified changefeed.
	GetAllTaskStatuses(ctx context.Context, changefeedID model.ChangeFeedID) (map[model.CaptureID]*model.TaskStatus, error)

	// GetTaskPositions returns the task positions for the specified changefeed.
	GetTaskPositions(ctx context.Context, changefeedID model.ChangeFeedID) (map[model.CaptureID]*model.TaskPosition, error)

	// GetProcessors returns the statuses of all processors
	GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error)

	// GetCaptures returns the information about all captures.
	GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error)
}

type OwnerQueryType int32

const (
	OwnerQueryAllChangeFeedStatuses = iota
	OwnerQueryAllChangeFeedInfo
	OwnerQueryAllTaskStatuses
	OwnerQueryTaskPositions
	OwnerQueryProcessors
	OwnerQueryCaptures
)

type OwnerQuery struct {
	Tp           OwnerQueryType
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

func (p *ownerStatusProvider) GetAllChangeFeedStatuses(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedStatus, error) {
	query := &OwnerQuery{
		Tp: OwnerQueryAllChangeFeedStatuses,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.ChangeFeedID]*model.ChangeFeedStatus), nil
}

func (p *ownerStatusProvider) GetChangeFeedStatus(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedStatus, error) {
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

func (p *ownerStatusProvider) GetAllChangeFeedInfo(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedInfo, error) {
	query := &OwnerQuery{
		Tp: OwnerQueryAllChangeFeedInfo,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.ChangeFeedID]*model.ChangeFeedInfo), nil
}

func (p *ownerStatusProvider) GetChangeFeedInfo(ctx context.Context, changefeedID model.ChangeFeedID) (*model.ChangeFeedInfo, error) {
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

func (p *ownerStatusProvider) GetAllTaskStatuses(ctx context.Context, changefeedID model.ChangeFeedID) (map[model.CaptureID]*model.TaskStatus, error) {
	query := &OwnerQuery{
		Tp:           OwnerQueryAllTaskStatuses,
		ChangeFeedID: changefeedID,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.CaptureID]*model.TaskStatus), nil
}

func (p *ownerStatusProvider) GetTaskPositions(ctx context.Context, changefeedID model.ChangeFeedID) (map[model.CaptureID]*model.TaskPosition, error) {
	query := &OwnerQuery{
		Tp:           OwnerQueryTaskPositions,
		ChangeFeedID: changefeedID,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.(map[model.CaptureID]*model.TaskPosition), nil
}

func (p *ownerStatusProvider) GetProcessors(ctx context.Context) ([]*model.ProcInfoSnap, error) {
	query := &OwnerQuery{
		Tp: OwnerQueryProcessors,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.([]*model.ProcInfoSnap), nil
}

func (p *ownerStatusProvider) GetCaptures(ctx context.Context) ([]*model.CaptureInfo, error) {
	query := &OwnerQuery{
		Tp: OwnerQueryCaptures,
	}
	if err := p.sendQueryToOwner(ctx, query); err != nil {
		return nil, errors.Trace(err)
	}
	return query.Data.([]*model.CaptureInfo), nil
}

func (p *ownerStatusProvider) sendQueryToOwner(ctx context.Context, query *OwnerQuery) error {
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
