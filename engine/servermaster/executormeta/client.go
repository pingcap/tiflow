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

package executormeta

import (
	"context"
	"database/sql"

	perrors "github.com/pingcap/errors"
	engineModel "github.com/pingcap/tiflow/engine/model"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/servermaster/executormeta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"gorm.io/gorm"
)

// Client is a client for manage executor meta.
type Client interface {
	CreateExecutor(ctx context.Context, executor *model.Executor) error
	UpdateExecutor(ctx context.Context, executor *model.Executor) error
	DeleteExecutor(ctx context.Context, executorID engineModel.ExecutorID) error
	QueryExecutors(ctx context.Context) ([]*model.Executor, error)
}

// NewClient creates a new executor client.
func NewClient(cc metaModel.ClientConn) (Client, error) {
	if cc == nil {
		return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("input client conn is nil")
	}

	conn, err := cc.GetConn()
	if err != nil {
		return nil, err
	}

	sqlDB, ok := conn.(*sql.DB)
	if !ok {
		return nil, errors.ErrMetaParamsInvalid.GenWithStack("input client conn is not a sql type:%s",
			cc.StoreType())
	}

	db, err := pkgOrm.NewGormDB(sqlDB, cc.StoreType())
	if err != nil {
		return nil, perrors.Trace(err)
	}

	return newClientImpl(db), nil
}

type clientImpl struct {
	db *gorm.DB
}

func newClientImpl(db *gorm.DB) *clientImpl {
	return &clientImpl{db: db}
}

func (c *clientImpl) CreateExecutor(ctx context.Context, executor *model.Executor) error {
	if err := c.db.WithContext(ctx).
		Create(executor).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

func (c *clientImpl) UpdateExecutor(ctx context.Context, executor *model.Executor) error {
	if err := c.db.WithContext(ctx).
		Model(&model.Executor{}).
		Where("id = ?", executor.ID).
		Updates(executor.Map()).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

func (c *clientImpl) DeleteExecutor(ctx context.Context, executorID engineModel.ExecutorID) error {
	if err := c.db.WithContext(ctx).
		Where("id = ?", executorID).
		Delete(&model.Executor{}).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

func (c *clientImpl) QueryExecutors(ctx context.Context) ([]*model.Executor, error) {
	var executors []*model.Executor
	if err := c.db.WithContext(ctx).
		Find(&executors).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}
	return executors, nil
}
