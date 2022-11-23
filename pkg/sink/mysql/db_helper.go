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

package mysql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"strconv"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/charset"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	dmutils "github.com/pingcap/tiflow/dm/pkg/utils"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// CreateMySQLDBConn creates a mysql database connection with the given dsn.
func CreateMySQLDBConn(ctx context.Context, dsnStr string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}

	err = db.PingContext(ctx)
	if err != nil {
		// close db to recycle resources
		if closeErr := db.Close(); closeErr != nil {
			log.Warn("close db failed", zap.Error(err))
		}
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}

	return db, nil
}

// GenerateDSN generates the dsn with the given config.
func GenerateDSN(ctx context.Context, sinkURI *url.URL, cfg *Config, dbConnFactory Factory) (dsnStr string, err error) {
	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	username := sinkURI.User.Username()
	if username == "" {
		username = "root"
	}
	password, _ := sinkURI.User.Password()

	hostName := sinkURI.Hostname()
	port := sinkURI.Port()
	if port == "" {
		port = "4000"
	}

	// This will handle the IPv6 address format.
	var dsn *dmysql.Config
	host := net.JoinHostPort(hostName, port)
	dsnStr = fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, cfg.TLS)
	if dsn, err = dmysql.ParseDSN(dsnStr); err != nil {
		return
	}

	// create test db used for parameter detection
	// Refer https://github.com/go-sql-driver/mysql#parameters
	if dsn.Params == nil {
		dsn.Params = make(map[string]string, 1)
	}
	if cfg.Timezone != "" {
		dsn.Params["time_zone"] = cfg.Timezone
	}
	dsn.Params["readTimeout"] = cfg.ReadTimeout
	dsn.Params["writeTimeout"] = cfg.WriteTimeout
	dsn.Params["timeout"] = cfg.DialTimeout

	var testDB *sql.DB
	testDB, err = CheckAndAdjustPassword(ctx, dsn, dbConnFactory)
	if err != nil {
		return
	}
	defer testDB.Close()

	// Adjust sql_mode for compatibility.
	dsn.Params["sql_mode"], err = querySQLMode(ctx, testDB)
	if err != nil {
		return
	}
	dsn.Params["sql_mode"], err = dmutils.AdjustSQLModeCompatible(dsn.Params["sql_mode"])
	if err != nil {
		return
	}
	// NOTE: quote the string is necessary to avoid ambiguities.
	dsn.Params["sql_mode"] = strconv.Quote(dsn.Params["sql_mode"])

	dsnStr, err = generateDSNByConfig(ctx, dsn, cfg, testDB)
	if err != nil {
		return
	}

	// check if GBK charset is supported by downstream
	var gbkSupported bool
	gbkSupported, err = checkCharsetSupport(ctx, testDB, charset.CharsetGBK)
	if err != nil {
		return
	}
	if !gbkSupported {
		log.Warn("GBK charset is not supported by the downstream. "+
			"Some types of DDLs may fail to execute",
			zap.String("hostname", hostName), zap.String("port", port))
	}

	return
}

func generateDSNByConfig(
	ctx context.Context,
	dsnCfg *dmysql.Config,
	cfg *Config,
	testDB *sql.DB,
) (string, error) {
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.DBName = ""
	dsnCfg.InterpolateParams = true
	dsnCfg.MultiStatements = true
	// if timezone is empty string, we don't pass this variable in dsn
	if cfg.Timezone != "" {
		dsnCfg.Params["time_zone"] = cfg.Timezone
	}
	dsnCfg.Params["readTimeout"] = cfg.ReadTimeout
	dsnCfg.Params["writeTimeout"] = cfg.WriteTimeout
	dsnCfg.Params["timeout"] = cfg.DialTimeout

	autoRandom, err := checkTiDBVariable(ctx, testDB, "allow_auto_random_explicit_insert", "1")
	if err != nil {
		return "", err
	}
	if autoRandom != "" {
		dsnCfg.Params["allow_auto_random_explicit_insert"] = autoRandom
	}

	txnMode, err := checkTiDBVariable(ctx, testDB, "tidb_txn_mode", cfg.tidbTxnMode)
	if err != nil {
		return "", err
	}
	if txnMode != "" {
		dsnCfg.Params["tidb_txn_mode"] = txnMode
	}

	// Since we don't need select, just set default isolation level to read-committed
	// transaction_isolation is mysql newly introduced variable and will vary from MySQL5.7/MySQL8.0/Mariadb
	isolation, err := checkTiDBVariable(ctx, testDB, "transaction_isolation", defaultTxnIsolationRC)
	if err != nil {
		return "", err
	}
	if isolation != "" {
		dsnCfg.Params["transaction_isolation"] = fmt.Sprintf(`"%s"`, defaultTxnIsolationRC)
	} else {
		dsnCfg.Params["tx_isolation"] = fmt.Sprintf(`"%s"`, defaultTxnIsolationRC)
	}

	// equals to executing "SET NAMES utf8mb4"
	dsnCfg.Params["charset"] = defaultCharacterSet

	tidbPlacementMode, err := checkTiDBVariable(ctx, testDB, "tidb_placement_mode", "ignore")
	if err != nil {
		return "", err
	}
	if tidbPlacementMode != "" {
		dsnCfg.Params["tidb_placement_mode"] = fmt.Sprintf(`"%s"`, tidbPlacementMode)
	}
	dsnClone := dsnCfg.Clone()
	dsnClone.Passwd = "******"
	log.Info("sink uri is configured", zap.String("dsn", dsnClone.FormatDSN()))

	return dsnCfg.FormatDSN(), nil
}

func querySQLMode(ctx context.Context, db *sql.DB) (sqlMode string, err error) {
	row := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode;")
	err = row.Scan(&sqlMode)
	if err != nil {
		err = cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	return
}

// check whether the target charset is supported
func checkCharsetSupport(ctx context.Context, db *sql.DB, charsetName string) (bool, error) {
	// validate charsetName
	_, err := charset.GetCharsetInfo(charsetName)
	if err != nil {
		return false, errors.Trace(err)
	}

	var characterSetName string
	querySQL := "select character_set_name from information_schema.character_sets " +
		"where character_set_name = '" + charsetName + "';"
	err = db.QueryRowContext(ctx, querySQL).Scan(&characterSetName)
	if err != nil && err != sql.ErrNoRows {
		return false, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	if err != nil {
		return false, nil
	}

	return true, nil
}

func checkTiDBVariable(ctx context.Context, db *sql.DB, variableName, defaultValue string) (string, error) {
	var name string
	var value string
	querySQL := fmt.Sprintf("show session variables like '%s';", variableName)
	err := db.QueryRowContext(ctx, querySQL).Scan(&name, &value)
	if err != nil && err != sql.ErrNoRows {
		errMsg := "fail to query session variable " + variableName
		return "", cerror.ErrMySQLQueryError.Wrap(err).GenWithStack(errMsg)
	}
	// session variable works, use given default value
	if err == nil {
		return defaultValue, nil
	}
	// session variable not exists, return "" to ignore it
	return "", nil
}

func CheckAndAdjustPassword(ctx context.Context, dbConfig *dmysql.Config, dbConnFactory Factory) (*sql.DB, error) {
	password := dbConfig.Passwd
	if dbConnFactory == nil {
		dbConnFactory = CreateMySQLDBConn
	}
	testDB, err := dbConnFactory(ctx, dbConfig.FormatDSN())
	if err != nil {
		// If access is denied and password is encoded by base64, try to decoded password.
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok && mysqlErr.Number == tmysql.ErrAccessDenied {
			if dePassword, decodeErr := base64.StdEncoding.DecodeString(password); decodeErr == nil && string(dePassword) != password {
				dbConfig.Passwd = string(dePassword)
				testDB, err = dbConnFactory(ctx, dbConfig.FormatDSN())
				if err != nil {
					return testDB, err
				}
			}
		} else {
			return nil, err
		}
	}
	return testDB, nil
}
