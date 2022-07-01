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

package sqlutil

import (
	"database/sql"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiflow/dm/pkg/log"
	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"go.uber.org/zap"
)

// GenerateDSNByParams generates a dsn string.
// dsn format: [username[:password]@][protocol[(address)]]/
func GenerateDSNByParams(storeConf *metaclient.StoreConfigParams) string {
	if storeConf == nil {
		return "invalid dsn"
	}

	dsnCfg := dmysql.NewConfig()
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	if storeConf.Auth != nil {
		dsnCfg.User = storeConf.Auth.User
		dsnCfg.Passwd = storeConf.Auth.Passwd
	}
	dsnCfg.Net = "tcp"
	dsnCfg.Addr = storeConf.Endpoints[0]
	dsnCfg.DBName = storeConf.Schema
	dsnCfg.InterpolateParams = true
	// dsnCfg.MultiStatements = true
	dsnCfg.Params["parseTime"] = "true"
	// TODO: check for timezone
	dsnCfg.Params["loc"] = "Local"
	dsnCfg.Params["readTimeout"] = storeConf.ReadTimeout
	dsnCfg.Params["writeTimeout"] = storeConf.WriteTimeout
	dsnCfg.Params["timeout"] = storeConf.DialTimeout

	return dsnCfg.FormatDSN()
}

// NewSQLDB return sql.DB for specified driver and dsn
func NewSQLDB(storeConf *metaclient.StoreConfigParams) (*sql.DB, error) {
	db, err := sql.Open(driver, GenerateDSNByParams(storeConf))
	if err != nil {
		log.L().Error("open dsn fail", zap.String("dsn", dsn), zap.Any("config", dbConf), zap.Error(err))
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	db.SetConnMaxIdleTime(dbConf.ConnMaxIdleTime)
	db.SetConnMaxLifetime(dbConf.ConnMaxLifeTime)
	db.SetMaxIdleConns(dbConf.MaxIdleConns)
	db.SetMaxOpenConns(dbConf.MaxOpenConns)
	return db, nil
}
