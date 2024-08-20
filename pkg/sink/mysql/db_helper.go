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
	"net/url"
	"strconv"
	"strings"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/charset"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	dmutils "github.com/pingcap/tiflow/dm/pkg/conn"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// generateDSNs generates the DSNs with the given config.
// generateDSNs uses the provided dbConnFactory to create a temporary connection
// to the downstream database specified by the sinkURI. This temporary connection
// is used to query important information from the downstream database, such as
// version, charset, and other relevant details. After the required information
// is retrieved, the temporary connection is closed. The retrieved data is then
// used to populate additional parameters into the Sink URI, refining
// the connection URL.
func generateDSNs(ctx context.Context, sinkURI *url.URL, cfg *Config, dbConnFactory ConnectionFactory) ([]string, error) {
	dsnCfgs, err := GetDSNCfgs(sinkURI, cfg)
	if err != nil {
		return nil, err
	}

	var oneOfErrs error
	dsnList := make([]string, len(dsnCfgs))
	for i := 0; i < len(dsnCfgs); i++ {
		// dsn format of the driver:
		// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
		dsnList[i], err = checkAndGenerateDSNByConfig(ctx, dsnCfgs[i], cfg, dbConnFactory)
		if err != nil {
			oneOfErrs = err
		}
	}

	return dsnList, oneOfErrs
}

func checkAndGenerateDSNByConfig(ctx context.Context, dsnCfg *dmysql.Config, cfg *Config, dbConnFactory ConnectionFactory) (string, error) {
	testDB, err := GetTestDB(ctx, dsnCfg, dbConnFactory)
	if err != nil {
		return "", err
	}
	defer testDB.Close()

	// check if GBK charset is supported by downstream
	var gbkSupported bool
	gbkSupported, err = checkCharsetSupport(ctx, testDB, charset.CharsetGBK)
	if err != nil {
		return "", err
	}
	if !gbkSupported {
		log.Warn("GBK charset is not supported by the downstream. "+
			"Some types of DDLs may fail to execute",
			zap.String("host", dsnCfg.Addr))
	}

	dsnStr, err := generateDSNByConfig(ctx, dsnCfg, cfg, testDB)
	if err != nil {
		return "", err
	}

	return dsnStr, nil
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
	// auto fetch max_allowed_packet on every new connection
	dsnCfg.Params["maxAllowedPacket"] = "0"

	// we use default sql mode for downstream because all dmls generated and ddls in ticdc
	// are based on default sql mode.
	var err error
	dsnCfg.Params["sql_mode"], err = dmutils.AdjustSQLModeCompatible(tmysql.DefaultSQLMode)
	if err != nil {
		return "", err
	}
	// NOTE: quote the string is necessary to avoid ambiguities.
	dsnCfg.Params["sql_mode"] = strconv.Quote(dsnCfg.Params["sql_mode"])

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

	// disable foreign_key_checks
	dsnCfg.Params["foreign_key_checks"] = "0"

	tidbPlacementMode, err := checkTiDBVariable(ctx, testDB, "tidb_placement_mode", "ignore")
	if err != nil {
		return "", err
	}
	if tidbPlacementMode != "" {
		dsnCfg.Params["tidb_placement_mode"] = fmt.Sprintf(`"%s"`, tidbPlacementMode)
	}
	tidbEnableExternalTSRead, err := checkTiDBVariable(ctx, testDB, "tidb_enable_external_ts_read", "OFF")
	if err != nil {
		return "", err
	}
	if tidbEnableExternalTSRead != "" {
		// set the `tidb_enable_external_ts_read` to `OFF`, so cdc could write to the sink
		dsnCfg.Params["tidb_enable_external_ts_read"] = fmt.Sprintf(`"%s"`, tidbEnableExternalTSRead)
	}
	dsnClone := dsnCfg.Clone()
	dsnClone.Passwd = "******"
	log.Info("sink uri is configured", zap.String("dsn", dsnClone.FormatDSN()))

	return dsnCfg.FormatDSN(), nil
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

// GetTestDB checks and adjusts the password of the given DSN,
// it will return a DB instance opened with the adjusted password.
func GetTestDB(ctx context.Context, dbConfig *dmysql.Config, dbConnFactory ConnectionFactory) (*sql.DB, error) {
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
			}
		}
	}
	return testDB, err
}

// GetDSNCfgs generates DSN configurations for multiple addresses contained in the given sinkURI.
// If the sinkURI contains multiple host addresses, the function will generate a DSN configuration
// for each address. It uses the provided `Config` to set various parameters such as TLS, timeouts,
// and the database timezone. The function supports IPv6 address formats and ensures that appropriate
// query parameters are applied. The DSN configurations are returned as a slice of `*mysql.Config` objects.
func GetDSNCfgs(sinkURI *url.URL, cfg *Config) ([]*dmysql.Config, error) {
	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	username := sinkURI.User.Username()
	if username == "" {
		username = "root"
	}
	password, _ := sinkURI.User.Password()

	hosts := getMultiAddressesInURL(sinkURI.Host)

	// This will handle the IPv6 address format.
	dsnCfgs := make([]*dmysql.Config, len(hosts))
	for i := 0; i < len(dsnCfgs); i++ {
		var err error
		dsnStr := fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hosts[i], cfg.TLS)
		if dsnCfgs[i], err = dmysql.ParseDSN(dsnStr); err != nil {
			return nil, errors.Trace(err)
		}

		// create test db used for parameter detection
		// Refer https://github.com/go-sql-driver/mysql#parameters
		if dsnCfgs[i].Params == nil {
			dsnCfgs[i].Params = make(map[string]string, 1)
		}
		if cfg.Timezone != "" {
			dsnCfgs[i].Params["time_zone"] = cfg.Timezone
		}
		dsnCfgs[i].Params["readTimeout"] = cfg.ReadTimeout
		dsnCfgs[i].Params["writeTimeout"] = cfg.WriteTimeout
		dsnCfgs[i].Params["timeout"] = cfg.DialTimeout
	}
	return dsnCfgs, nil
}

func getMultiAddressesInURL(hostStr string) []string {
	hosts := strings.Split(hostStr, ",")
	for i := 0; i < len(hosts); i++ {
		hostnameAndPort := strings.Split(hosts[i], ":")
		if len(hostnameAndPort) == 1 {
			// If only the hostname is provided, add the default port 4000.
			hostnameAndPort = append(hostnameAndPort, "4000")
		} else if len(hostnameAndPort) == 2 && hostnameAndPort[1] == "" {
			// If both the hostname and port are provided, but the port is empty, then set the port to the default port 4000.
			hostnameAndPort[1] = "4000"
		}
		hosts[i] = hostnameAndPort[0] + ":" + hostnameAndPort[1]
	}
	return hosts
}

// CheckIfBDRModeIsSupported checks if the downstream supports BDR mode.
func CheckIfBDRModeIsSupported(ctx context.Context, db *sql.DB) (bool, error) {
	isTiDB, err := CheckIsTiDB(ctx, db)
	if err != nil || !isTiDB {
		return false, err
	}
	testSourceID := 1
	// downstream is TiDB, set system variables.
	// We should always try to set this variable, and ignore the error if
	// downstream does not support this variable, it is by design.
	query := fmt.Sprintf("SET SESSION %s = %d", "tidb_cdc_write_source", testSourceID)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok &&
			mysqlErr.Number == tmysql.ErrUnknownSystemVariable {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// CheckIsTiDB checks if the downstream is TiDB.
func CheckIsTiDB(ctx context.Context, db *sql.DB) (bool, error) {
	var tidbVer string
	// check if downstream is TiDB
	row := db.QueryRowContext(ctx, "select tidb_version()")
	err := row.Scan(&tidbVer)
	if err != nil {
		log.Error("check tidb version error", zap.Error(err))
		return false, nil
	}
	return true, nil
}

// QueryMaxPreparedStmtCount gets the value of max_prepared_stmt_count
func QueryMaxPreparedStmtCount(ctx context.Context, db *sql.DB) (int, error) {
	row := db.QueryRowContext(ctx, "select @@global.max_prepared_stmt_count;")
	var maxPreparedStmtCount sql.NullInt32
	err := row.Scan(&maxPreparedStmtCount)
	if err != nil {
		err = cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	return int(maxPreparedStmtCount.Int32), err
}

// QueryMaxAllowedPacket gets the value of max_allowed_packet
func QueryMaxAllowedPacket(ctx context.Context, db *sql.DB) (int64, error) {
	row := db.QueryRowContext(ctx, "select @@global.max_allowed_packet;")
	var maxAllowedPacket sql.NullInt64
	if err := row.Scan(&maxAllowedPacket); err != nil {
		return 0, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	return maxAllowedPacket.Int64, nil
}

// SetWriteSource sets write source for the transaction.
func SetWriteSource(ctx context.Context, cfg *Config, txn *sql.Tx) error {
	// we only set write source when donwstream is TiDB and write source is existed.
	if !cfg.IsWriteSourceExisted {
		return nil
	}
	// downstream is TiDB, set system variables.
	// We should always try to set this variable, and ignore the error if
	// downstream does not support this variable, it is by design.
	query := fmt.Sprintf("SET SESSION %s = %d", "tidb_cdc_write_source", cfg.SourceID)
	_, err := txn.ExecContext(ctx, query)
	if err != nil {
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok &&
			mysqlErr.Number == tmysql.ErrUnknownSystemVariable {
			return nil
		}
		return err
	}
	return nil
}
