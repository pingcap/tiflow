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

	"github.com/glebarez/sqlite"
	"github.com/pingcap/failpoint"
	dmutils "github.com/pingcap/tiflow/dm/pkg/utils" // TODO: move it to pkg
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
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

// InitAllFrameworkModels will create all framework-related tables in SQL backend
// NOT thread-safe.
// TODO: What happen if we upgrade the definition of model when rolling update?
// TODO: need test: change column definition/add column/drop column?
func InitAllFrameworkModels(ctx context.Context, cc metaModel.ClientConn) error {
	gormDB, err := genGormDBFromClientConn(cc)
	if err != nil {
		return err
	}

	failpoint.InjectContext(ctx, "initializedDelay", nil)
	if err := gormDB.WithContext(ctx).
		AutoMigrate(globalModels...); err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// InitEpochModel creates the backend logic epoch table if not exists
// Only use for business meta currently
// NOT thread-safe
func InitEpochModel(ctx context.Context, cc metaModel.ClientConn) error {
	gormDB, err := genGormDBFromClientConn(cc)
	if err != nil {
		return err
	}
	if err := gormDB.WithContext(ctx).
		AutoMigrate(&ormModel.LogicEpoch{}); err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

func genGormDBFromClientConn(cc metaModel.ClientConn) (*gorm.DB, error) {
	if cc == nil {
		return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("input client conn is nil")
	}

	conn, err := cc.GetConn()
	if err != nil {
		return nil, err
	}

	// check if a sql.DB
	db, ok := conn.(*sql.DB)
	if !ok {
		return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("client conn is not sql DB")
	}
	gormDB, err := NewGormDB(db, cc.StoreType())
	if err != nil {
		return nil, err
	}

	return gormDB, nil
}

// NewGormDB news a gorm.DB
func NewGormDB(sqlDB *sql.DB, storeType metaModel.StoreType) (*gorm.DB, error) {
	var dialector gorm.Dialector
	if storeType == metaModel.StoreTypeMySQL {
		dialector = mysql.New(mysql.Config{
			Conn:                      sqlDB,
			SkipInitializeWithVersion: false,
		})
	} else if storeType == metaModel.StoreTypeSQLite {
		dialector = sqlite.Dialector{
			Conn: sqlDB,
		}
	} else {
		return nil, errors.ErrMetaParamsInvalid.GenWithStack("SQL subtype is not supported yet:%s", storeType)
	}

	db, err := gorm.Open(dialector, &gorm.Config{
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

// IsDuplicateEntryError checks whether error contains DuplicateEntry(MySQL) error
// or UNIQUE constraint failed(SQLite) error underlying.
func IsDuplicateEntryError(err error) bool {
	if err == nil {
		return false
	}
	return dmutils.IsErrDuplicateEntry(err) ||
		strings.Contains(err.Error(), "UNIQUE constraint failed")
}
