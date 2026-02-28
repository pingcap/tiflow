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

package common

import (
	"database/sql"
	"encoding/base64"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"go.uber.org/zap"
)

func tryConnectMySQL(cfg *mysql.Config) (*sql.DB, error) {
	failpoint.Inject("MustMySQLPassword", func(val failpoint.Value) {
		pwd := val.(string)
		if cfg.Passwd != pwd {
			failpoint.Return(nil, &mysql.MySQLError{Number: tmysql.ErrAccessDenied, Message: "access denied"})
		}
		failpoint.Return(nil, nil)
	})
	c, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db := sql.OpenDB(c)
	if err = db.Ping(); err != nil {
		_ = db.Close()
		return nil, errors.Trace(err)
	}
	return db, nil
}

func verifyParams(db *sql.DB, sessionCfg *config.SessionConfig) error {
	if sessionCfg == nil {
		return nil
	}
	for param, value := range *sessionCfg {
		res, err := db.Query("show session variables like ?", param)
		if err != nil {
			return err
		}
		//nolint: errcheck
		defer res.Close()
		if res.Next() {
			var paramName, actual string
			if err := res.Scan(&paramName, &actual); err != nil {
				return err
			}
			expected := fmt.Sprint(value)
			if actual != expected {
				log.Warn("The session variable was set, but the database returned a different value",
					zap.String("variable", param),
					zap.String("configured_value", expected),
					zap.String("effective_value", actual),
				)
			}
		} else {
			return fmt.Errorf("parameter %s not found", param)
		}

		if err := res.Err(); err != nil {
			return err
		}
	}
	return nil
}

// ConnectMySQL creates sql.DB used for select data
func ConnectMySQL(sessionCfg *config.SessionConfig, cfg *mysql.Config, num int) (db *sql.DB, err error) {
	defer func() {
		if err == nil && db != nil {
			// SetMaxOpenConns and SetMaxIdleConns for connection to avoid error like
			// `dial tcp 10.26.2.1:3306: connect: cannot assign requested address`
			db.SetMaxOpenConns(num)
			db.SetMaxIdleConns(num)
		}
	}()
	// Try plain password first.
	db, firstErr := tryConnectMySQL(cfg)
	if firstErr == nil && verifyParams(db, sessionCfg) == nil {
		return db, nil
	}
	// If access is denied and password is encoded by base64, try the decoded string as well.
	if mysqlErr, ok := errors.Cause(firstErr).(*mysql.MySQLError); ok && mysqlErr.Number == tmysql.ErrAccessDenied {
		// If password is encoded by base64, try the decoded string as well.
		if password, decodeErr := base64.StdEncoding.DecodeString(cfg.Passwd); decodeErr == nil && string(password) != cfg.Passwd {
			cfg.Passwd = string(password)
			db2, err := tryConnectMySQL(cfg)
			if err == nil && verifyParams(db2, sessionCfg) == nil {
				return db2, nil
			}
		}
	}
	// If we can't connect successfully, return the first error.
	return nil, errors.Trace(firstErr)
}
