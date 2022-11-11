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

package dbutil

import (
	"database/sql"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// NewSQLDB return sql.DB for specified driver and dsn
func NewSQLDB(driver string, dsn string, dbConf *DBConfig) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Error("open dsn fail", zap.String("dsn", dsn), zap.Any("config", dbConf), zap.Error(err))
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	if dbConf != nil {
		db.SetConnMaxIdleTime(dbConf.ConnMaxIdleTime)
		db.SetConnMaxLifetime(dbConf.ConnMaxLifeTime)
		db.SetMaxIdleConns(dbConf.MaxIdleConns)
		db.SetMaxOpenConns(dbConf.MaxOpenConns)
	}
	return db, nil
}

// GetSQLStrictMode return SQL strict mode for metastore
// Use strict mode to avoid unexpected silent processing of meta data
func GetSQLStrictMode() string {
	needEnable := []string{
		"STRICT_TRANS_TABLES",
		"STRICT_ALL_TABLES",
		"ERROR_FOR_DIVISION_BY_ZERO",
	}

	return strings.Join(needEnable, ",")
}
