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
	"context"
	"database/sql"
	"fmt"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiflow/dm/pkg/log"
	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"go.uber.org/zap"
)

// CreateDatabaseForProject creates a database in the underlaying DB for project
// TODO: check the projectID
func CreateDatabaseForProject(mc metaclient.StoreConfigParams, projectID tenant.ProjectID, conf DBConfig) error {
	dsn := GenerateDSNByParams(mc, projectID, conf, false)
	log.L().Info("mysql connection", zap.String("dsn", dsn))

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.L().Error("open dsn fail", zap.String("dsn", dsn), zap.Error(err))
		return cerrors.ErrMetaOpFail.Wrap(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	query := fmt.Sprintf("CREATE DATABASE if not exists %s", projectID)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// GenerateDSNByParams will use projectID as DBName to achieve isolation.
// Besides, it will add some default mysql params to the dsn
func GenerateDSNByParams(mc metaclient.StoreConfigParams, projectID tenant.ProjectID,
	conf DBConfig, withDB bool,
) string {
	dsnCfg := dmysql.NewConfig()
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.User = mc.Auth.User
	dsnCfg.Passwd = mc.Auth.Passwd
	dsnCfg.Net = "tcp"
	dsnCfg.Addr = mc.Endpoints[0]
	if withDB {
		dsnCfg.DBName = projectID
	}
	dsnCfg.InterpolateParams = true
	// dsnCfg.MultiStatements = true
	dsnCfg.Params["readTimeout"] = conf.ReadTimeout
	dsnCfg.Params["writeTimeout"] = conf.WriteTimeout
	dsnCfg.Params["timeout"] = conf.DialTimeout
	dsnCfg.Params["parseTime"] = "true"
	// TODO: check for timezone
	dsnCfg.Params["loc"] = "Local"

	// dsn format: [username[:password]@][protocol[(address)]]/
	return dsnCfg.FormatDSN()
}

// NewSQLDB return sql.DB for specified driver and dsn
func NewSQLDB(driver string, dsn string, conf DBConfig) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.L().Error("open dsn fail", zap.String("dsn", dsn), zap.Any("config", conf), zap.Error(err))
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	db.SetConnMaxIdleTime(conf.ConnMaxIdleTime)
	db.SetConnMaxLifetime(conf.ConnMaxLifeTime)
	db.SetMaxIdleConns(conf.MaxIdleConns)
	db.SetMaxOpenConns(conf.MaxOpenConns)
	return db, nil
}
