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
	"database/sql"
	"strings"
	"time"

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

// NewGormDB news a gorm.DB
func NewGormDB(sqlDB *sql.DB) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: false,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		Logger:                 NewOrmLogger(logutil.WithComponent("gorm"), WithSlowThreshold(defaultSlowLogThreshold)),
	})
	if err != nil {
		return nil, errors.ErrMetaNewClientFail.Wrap(err)
	}

	return db, nil
}
