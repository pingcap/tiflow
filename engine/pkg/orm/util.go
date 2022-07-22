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

package orm

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
)

var defaultSlowLogThreshold = 200 * time.Millisecond

// IsNotFoundError checks whether the error is ErrMetaEntryNotFound
// TODO: refine me, need wrap error for api
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "ErrMetaEntryNotFound")
}

// InitAllFrameworkModels will create all related tables in SQL backend
// TODO: What happen if we upgrade the definition of model when rolling update?
// TODO: need test: change column definition/add column/drop column?
func InitAllFrameworkModels(ctx context.Context, cc metaModel.ClientConn) error {
	if cc == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input client conn is nil")
	}

	var err error
	conn, err := cc.GetConn()
	if err != nil {
		return err
	}

	var gormDB *gorm.DB
	// check if a sql.DB
	db, ok := conn.(*sql.DB)
	if ok {
		gormDB, err = NewGormDB(db)
		if err != nil {
			return err
		}
	}

	// check if a gorm.DB
	if gormDB == nil {
		gormDB, ok = conn.(*gorm.DB)
		if !ok {
			return errors.ErrMetaParamsInvalid.GenWithStackByArgs("client conn is not sql DB")
		}
	}

	failpoint.InjectContext(ctx, "initializedDelay", nil)

	if err := gormDB.WithContext(ctx).
		AutoMigrate(globalModels...); err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return ormModel.InitEpochModel(ctx, gormDB)
}

// NewGormDB news a gorm.DB
func NewGormDB(sqlDB *sql.DB) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: false,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger: NewOrmLogger(logutil.WithComponent("gorm"),
			WithSlowThreshold(defaultSlowLogThreshold),
			WithIgnoreTraceRecordNotFoundErr()),
	})
	if err != nil {
		return nil, errors.ErrMetaNewClientFail.Wrap(err)
	}

	return db, nil
}
