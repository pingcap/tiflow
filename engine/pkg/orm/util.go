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
	"strings"

	"github.com/pingcap/log"
	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/engine/pkg/sqlutil"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// IsNotFoundError checks whether the error is ErrMetaEntryNotFound
// TODO: refine me, need wrap error for api
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "ErrMetaEntryNotFound")
}

func NewOrmDB(storeConf *metaclient.StoreConfigParams) (*gorm.DB, error) {
	sqlDB, err := sqlutil.NewSQLDB(storeConf)
	if err != nil {
		return nil, err
	}

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: false,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	if err != nil {
		sqlDB.Close()
		log.L().Error("create gorm client fail", zap.Error(err))
		return nil, cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	return db, nil
}
