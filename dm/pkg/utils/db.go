// Copyright 2019 PingCAP, Inc.
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

package utils

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/check"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/gtid"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/terror"
)

const (
	// DefaultDBTimeout represents a DB operation timeout for common usages.
	DefaultDBTimeout = 30 * time.Second

	// for MariaDB, UUID set as `gtid_domain_id` + domainServerIDSeparator + `server_id`.
	domainServerIDSeparator = "-"

	// the default base(min) server id generated by random.
	defaultBaseServerID = math.MaxUint32 / 10
)

// GetFlavor gets flavor from DB.
func GetFlavor(ctx context.Context, db *sql.DB) (string, error) {
	value, err := dbutil.ShowVersion(ctx, db)
	if err != nil {
		return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	if check.IsMariaDB(value) {
		return gmysql.MariaDBFlavor, nil
	}
	return gmysql.MySQLFlavor, nil
}

// GetAllServerID gets all slave server id and master server id.
func GetAllServerID(ctx context.Context, db *sql.DB) (map[uint32]struct{}, error) {
	serverIDs, err := GetSlaveServerID(ctx, db)
	if err != nil {
		return nil, err
	}

	masterServerID, err := GetServerID(ctx, db)
	if err != nil {
		return nil, err
	}

	serverIDs[masterServerID] = struct{}{}
	return serverIDs, nil
}

// GetRandomServerID gets a random server ID which is not used.
func GetRandomServerID(ctx context.Context, db *sql.DB) (uint32, error) {
	rand.Seed(time.Now().UnixNano())

	serverIDs, err := GetAllServerID(ctx, db)
	if err != nil {
		return 0, err
	}

	for i := 0; i < 99999; i++ {
		randomValue := uint32(rand.Intn(100000))
		randomServerID := uint32(defaultBaseServerID) + randomValue
		if _, ok := serverIDs[randomServerID]; ok {
			continue
		}

		return randomServerID, nil
	}

	// should never happened unless the master has too many slave.
	return 0, terror.ErrInvalidServerID.Generatef("can't find a random available server ID")
}

// GetSlaveServerID gets all slave server id.
func GetSlaveServerID(ctx context.Context, db *sql.DB) (map[uint32]struct{}, error) {
	rows, err := db.QueryContext(ctx, `SHOW SLAVE HOSTS`)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	/*
		in MySQL:
		mysql> SHOW SLAVE HOSTS;
		+------------+-----------+------+-----------+--------------------------------------+
		| Server_id  | Host      | Port | Master_id | Slave_UUID                           |
		+------------+-----------+------+-----------+--------------------------------------+
		|  192168010 | iconnect2 | 3306 | 192168011 | 14cb6624-7f93-11e0-b2c0-c80aa9429562 |
		| 1921680101 | athena    | 3306 | 192168011 | 07af4990-f41f-11df-a566-7ac56fdaf645 |
		+------------+-----------+------+-----------+--------------------------------------+

		in MariaDB:
		mysql> SHOW SLAVE HOSTS;
		+------------+-----------+------+-----------+
		| Server_id  | Host      | Port | Master_id |
		+------------+-----------+------+-----------+
		|  192168010 | iconnect2 | 3306 | 192168011 |
		| 1921680101 | athena    | 3306 | 192168011 |
		+------------+-----------+------+-----------+
	*/

	var (
		serverID  sql.NullInt64
		host      sql.NullString
		port      sql.NullInt64
		masterID  sql.NullInt64
		slaveUUID sql.NullString
	)
	serverIDs := make(map[uint32]struct{})
	for rows.Next() {
		if len(rowColumns) == 5 {
			err = rows.Scan(&serverID, &host, &port, &masterID, &slaveUUID)
		} else {
			err = rows.Scan(&serverID, &host, &port, &masterID)
		}
		if err != nil {
			return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}

		if serverID.Valid {
			serverIDs[uint32(serverID.Int64)] = struct{}{}
		} else {
			// should never happened
			log.L().Warn("get invalid server_id when execute `SHOW SLAVE HOSTS;`")
			continue
		}
	}

	if rows.Err() != nil {
		return nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}

	return serverIDs, nil
}

// GetMasterStatus gets status from master.
// When the returned error is nil, the gtid.Set must be not nil.
func GetMasterStatus(ctx context.Context, db *sql.DB, flavor string) (gmysql.Position, gtid.Set, error) {
	var (
		binlogPos gmysql.Position
		gs        gtid.Set
	)

	rows, err := db.QueryContext(ctx, `SHOW MASTER STATUS`)
	if err != nil {
		return binlogPos, gs, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return binlogPos, gs, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	// Show an example.
	/*
		MySQL [test]> SHOW MASTER STATUS;
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| File      | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                          |
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| ON.000001 |     4822 |              |                  | 85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-46
		+-----------+----------+--------------+------------------+--------------------------------------------+
	*/
	/*
		For MariaDB,SHOW MASTER STATUS:
		+--------------------+----------+--------------+------------------+
		| File               | Position | Binlog_Do_DB | Binlog_Ignore_DB |
		+--------------------+----------+--------------+------------------+
		| mariadb-bin.000016 |      475 |              |                  |
		+--------------------+----------+--------------+------------------+
		SELECT @@global.gtid_binlog_pos;
		+--------------------------+
		| @@global.gtid_binlog_pos |
		+--------------------------+
		| 0-1-2                    |
		+--------------------------+
	*/
	var (
		gtidStr    string
		binlogName string
		pos        uint32
		nullPtr    interface{}
	)
	if !rows.Next() {
		return binlogPos, gs, terror.ErrNoMasterStatus.Generate()
	}

	if len(rowColumns) == 5 {
		err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr, &gtidStr)
	} else {
		err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr)
	}
	if err != nil {
		return binlogPos, gs, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	binlogPos = gmysql.Position{
		Name: binlogName,
		Pos:  pos,
	}

	gs, err = gtid.ParserGTID(flavor, gtidStr)
	if err != nil {
		return binlogPos, gs, err
	}
	if rows.Next() {
		log.L().Warn("SHOW MASTER STATUS returns more than one row, will only use first row")
	}

	if rows.Close() != nil {
		return binlogPos, gs, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	if rows.Err() != nil {
		return binlogPos, gs, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}

	if flavor != gmysql.MySQLFlavor && (gs == nil || gs.String() == "") {
		gs, err = GetMariaDBGTID(ctx, db)
		if err != nil {
			return binlogPos, gs, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
		}
	}

	return binlogPos, gs, nil
}

// GetMariaDBGTID gets MariaDB's `gtid_binlog_pos`
// it can not get by `SHOW MASTER STATUS`.
func GetMariaDBGTID(ctx context.Context, db *sql.DB) (gtid.Set, error) {
	gtidStr, err := GetGlobalVariable(ctx, db, "gtid_binlog_pos")
	if err != nil {
		return nil, err
	}
	gs, err := gtid.ParserGTID(gmysql.MariaDBFlavor, gtidStr)
	if err != nil {
		return nil, err
	}
	return gs, nil
}

// GetGlobalVariable gets server's global variable.
func GetGlobalVariable(ctx context.Context, db *sql.DB, variable string) (value string, err error) {
	failpoint.Inject("GetGlobalVariableFailed", func(val failpoint.Value) {
		items := strings.Split(val.(string), ",")
		if len(items) != 2 {
			log.L().Fatal("failpoint GetGlobalVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		variableName := items[0]
		errCode, err1 := strconv.ParseUint(items[1], 10, 16)
		if err1 != nil {
			log.L().Fatal("failpoint GetGlobalVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		if variable == variableName {
			err = tmysql.NewErr(uint16(errCode))
			log.L().Warn("GetGlobalVariable failed", zap.String("variable", variable), zap.String("failpoint", "GetGlobalVariableFailed"), zap.Error(err))
			failpoint.Return("", terror.DBErrorAdapt(err, terror.ErrDBDriverError))
		}
	})

	conn, err := db.Conn(ctx)
	if err != nil {
		return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer conn.Close()
	return getVariable(ctx, conn, variable, true)
}

// GetSessionVariable gets connection's session variable.
func GetSessionVariable(ctx context.Context, conn *sql.Conn, variable string) (value string, err error) {
	failpoint.Inject("GetSessionVariableFailed", func(val failpoint.Value) {
		items := strings.Split(val.(string), ",")
		if len(items) != 2 {
			log.L().Fatal("failpoint GetSessionVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		variableName := items[0]
		errCode, err1 := strconv.ParseUint(items[1], 10, 16)
		if err1 != nil {
			log.L().Fatal("failpoint GetSessionVariableFailed's value is invalid", zap.String("val", val.(string)))
		}
		if variable == variableName {
			err = tmysql.NewErr(uint16(errCode))
			log.L().Warn("GetSessionVariable failed", zap.String("variable", variable), zap.String("failpoint", "GetSessionVariableFailed"), zap.Error(err))
			failpoint.Return("", terror.DBErrorAdapt(err, terror.ErrDBDriverError))
		}
	})
	return getVariable(ctx, conn, variable, false)
}

func getVariable(ctx context.Context, conn *sql.Conn, variable string, isGlobal bool) (value string, err error) {
	var template string
	if isGlobal {
		template = "SHOW GLOBAL VARIABLES LIKE '%s'"
	} else {
		template = "SHOW VARIABLES LIKE '%s'"
	}
	query := fmt.Sprintf(template, variable)
	row := conn.QueryRowContext(ctx, query)

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/

	err = row.Scan(&variable, &value)
	if err != nil {
		return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	return value, nil
}

// GetServerID gets server's `server_id`.
func GetServerID(ctx context.Context, db *sql.DB) (uint32, error) {
	serverIDStr, err := GetGlobalVariable(ctx, db, "server_id")
	if err != nil {
		return 0, err
	}

	serverID, err := strconv.ParseUint(serverIDStr, 10, 32)
	return uint32(serverID), terror.ErrInvalidServerID.Delegate(err, serverIDStr)
}

// GetMariaDBGtidDomainID gets MariaDB server's `gtid_domain_id`.
func GetMariaDBGtidDomainID(ctx context.Context, db *sql.DB) (uint32, error) {
	domainIDStr, err := GetGlobalVariable(ctx, db, "gtid_domain_id")
	if err != nil {
		return 0, err
	}

	domainID, err := strconv.ParseUint(domainIDStr, 10, 32)
	return uint32(domainID), terror.ErrMariaDBDomainID.Delegate(err, domainIDStr)
}

// GetServerUUID gets server's `server_uuid`.
func GetServerUUID(ctx context.Context, db *sql.DB, flavor string) (string, error) {
	if flavor == gmysql.MariaDBFlavor {
		return GetMariaDBUUID(ctx, db)
	}
	serverUUID, err := GetGlobalVariable(ctx, db, "server_uuid")
	return serverUUID, err
}

// GetServerUnixTS gets server's `UNIX_TIMESTAMP()`.
func GetServerUnixTS(ctx context.Context, db *sql.DB) (int64, error) {
	var ts int64
	row := db.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP()")
	err := row.Scan(&ts)
	if err != nil {
		log.L().Error("can't SELECT UNIX_TIMESTAMP()", zap.Error(err))
		return ts, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	return ts, err
}

// GetCharsetAndDefaultCollation gets charset and default collation map.
func GetCharsetAndDefaultCollation(ctx context.Context, db *sql.DB) (map[string]string, error) {
	charsetAndDefaultCollation := make(map[string]string)
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	defer conn.Close()

	// Show an example.
	/*
		mysql> SHOW CHARACTER SET;
		+----------+---------------------------------+---------------------+--------+
		| Charset  | Description                     | Default collation   | Maxlen |
		+----------+---------------------------------+---------------------+--------+
		| armscii8 | ARMSCII-8 Armenian              | armscii8_general_ci |      1 |
		| ascii    | US ASCII                        | ascii_general_ci    |      1 |
		| big5     | Big5 Traditional Chinese        | big5_chinese_ci     |      2 |
		| binary   | Binary pseudo charset           | binary              |      1 |
		| cp1250   | Windows Central European        | cp1250_general_ci   |      1 |
		| cp1251   | Windows Cyrillic                | cp1251_general_ci   |      1 |
		+----------+---------------------------------+---------------------+--------+
	*/

	rows, err := conn.QueryContext(ctx, "SHOW CHARACTER SET")
	if err != nil {
		return nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	defer rows.Close()
	for rows.Next() {
		var charset, description, collation string
		var maxlen int
		if scanErr := rows.Scan(&charset, &description, &collation, &maxlen); scanErr != nil {
			return nil, terror.DBErrorAdapt(scanErr, terror.ErrDBDriverError)
		}
		charsetAndDefaultCollation[strings.ToLower(charset)] = collation
	}

	if rows.Close() != nil {
		return nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	return charsetAndDefaultCollation, err
}

// GetSchemaList gets db schema list with `SHOW DATABASES`.
func GetSchemaList(ctx context.Context, db *sql.DB) ([]string, error) {
	schemaList := []string{}
	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return schemaList, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	if rows.Err() != nil {
		return nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	defer rows.Close()
	for rows.Next() {
		var schema string
		if scanErr := rows.Scan(&schema); scanErr != nil {
			return nil, terror.DBErrorAdapt(scanErr, terror.ErrDBDriverError)
		}
		schemaList = append(schemaList, schema)
	}
	return schemaList, err
}

// GetTableList gets db schema list with `SHOW TABLES FROM`.
func GetTableList(ctx context.Context, db *sql.DB, schemaName string) ([]string, error) {
	tableList := []string{}
	sql := "SHOW TABLES FROM " + schemaName
	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return tableList, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	if rows.Err() != nil {
		return nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		if scanErr := rows.Scan(&table); scanErr != nil {
			return nil, terror.DBErrorAdapt(scanErr, terror.ErrDBDriverError)
		}
		tableList = append(tableList, table)
	}
	return tableList, err
}

// GetMariaDBUUID gets equivalent `server_uuid` for MariaDB
// `gtid_domain_id` joined `server_id` with domainServerIDSeparator.
func GetMariaDBUUID(ctx context.Context, db *sql.DB) (string, error) {
	domainID, err := GetMariaDBGtidDomainID(ctx, db)
	if err != nil {
		return "", err
	}
	serverID, err := GetServerID(ctx, db)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d%s%d", domainID, domainServerIDSeparator, serverID), nil
}

// GetParser gets a parser for sql.DB which is suitable for session variable sql_mode.
func GetParser(ctx context.Context, db *sql.DB) (*parser.Parser, error) {
	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return GetParserForConn(ctx, c)
}

// GetParserForConn gets a parser for sql.Conn which is suitable for session variable sql_mode.
func GetParserForConn(ctx context.Context, conn *sql.Conn) (*parser.Parser, error) {
	sqlMode, err := GetSessionVariable(ctx, conn, "sql_mode")
	if err != nil {
		return nil, err
	}
	return GetParserFromSQLModeStr(sqlMode)
}

// GetParserFromSQLModeStr gets a parser and applies given sqlMode.
func GetParserFromSQLModeStr(sqlMode string) (*parser.Parser, error) {
	mode, err := tmysql.GetSQLMode(sqlMode)
	if err != nil {
		return nil, err
	}

	parser2 := parser.New()
	parser2.SetSQLMode(mode)
	return parser2, nil
}

// KillConn kills the DB connection (thread in mysqld).
func KillConn(ctx context.Context, db *sql.DB, connID uint32) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("KILL %d", connID))
	return terror.DBErrorAdapt(err, terror.ErrDBDriverError)
}

// IsMySQLError checks whether err is MySQLError error.
func IsMySQLError(err error, code uint16) bool {
	err = errors.Cause(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

// IsErrBinlogPurged checks whether err is BinlogPurged error.
func IsErrBinlogPurged(err error) bool {
	return IsMySQLError(err, tmysql.ErrMasterFatalErrorReadingBinlog)
}

// IsNoSuchThreadError checks whether err is NoSuchThreadError.
func IsNoSuchThreadError(err error) bool {
	return IsMySQLError(err, tmysql.ErrNoSuchThread)
}

// GetGTIDMode return GTID_MODE.
func GetGTIDMode(ctx context.Context, db *sql.DB) (string, error) {
	val, err := GetGlobalVariable(ctx, db, "GTID_MODE")
	return val, err
}

// ExtractTiDBVersion extract tidb's version
// version format: "5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty"
//                               ^~~~~~~~~^
func ExtractTiDBVersion(version string) (*semver.Version, error) {
	versions := strings.Split(strings.TrimSuffix(version, "-dirty"), "-")
	end := len(versions)
	switch end {
	case 3, 4:
	case 5, 6:
		end -= 2
	default:
		return nil, errors.Errorf("not a valid TiDB version: %s", version)
	}
	rawVersion := strings.Join(versions[2:end], "-")
	rawVersion = strings.TrimPrefix(rawVersion, "v")
	return semver.NewVersion(rawVersion)
}

// AddGSetWithPurged is used to handle this case: https://github.com/pingcap/dm/issues/1418
// we might get a gtid set from Previous_gtids event in binlog, but that gtid set can't be used to start a gtid sync
// because it doesn't cover all gtid_purged. The error of using it will be
// ERROR 1236 (HY000): The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires.
// so we add gtid_purged to it.
func AddGSetWithPurged(ctx context.Context, gset gtid.Set, conn *sql.Conn) (gtid.Set, error) {
	if _, ok := gset.(*gtid.MariadbGTIDSet); ok {
		return gset, nil
	}

	var (
		gtidStr string
		row     *sql.Row
		err     error
	)

	failpoint.Inject("GetGTIDPurged", func(val failpoint.Value) {
		str := val.(string)
		gtidStr = str
		failpoint.Goto("bypass")
	})
	row = conn.QueryRowContext(ctx, "select @@GLOBAL.gtid_purged")
	err = row.Scan(&gtidStr)
	if err != nil {
		log.L().Error("can't get @@GLOBAL.gtid_purged when try to add it to gtid set", zap.Error(err))
		return gset, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	failpoint.Label("bypass")
	if gtidStr == "" {
		return gset, nil
	}

	newGset := gset.Origin()
	err = newGset.Update(gtidStr)
	if err != nil {
		return nil, err
	}
	ret := &gtid.MySQLGTIDSet{}
	_ = ret.Set(newGset)
	return ret, nil
}

// AdjustSQLModeCompatible adjust downstream sql mode to compatible.
// TODO: When upstream's datatime is 2020-00-00, 2020-00-01, 2020-06-00
// and so on, downstream will be 2019-11-30, 2019-12-01, 2020-05-31,
// as if set the 'NO_ZERO_IN_DATE', 'NO_ZERO_DATE'.
// This is because the implementation of go-mysql, that you can see
// https://github.com/go-mysql-org/go-mysql/blob/master/replication/row_event.go#L1063-L1087
func AdjustSQLModeCompatible(sqlModes string) (string, error) {
	needDisable := []string{
		"NO_ZERO_IN_DATE",
		"NO_ZERO_DATE",
		"ERROR_FOR_DIVISION_BY_ZERO",
		"NO_AUTO_CREATE_USER",
		"STRICT_TRANS_TABLES",
		"STRICT_ALL_TABLES",
	}
	needEnable := []string{
		"IGNORE_SPACE",
		"NO_AUTO_VALUE_ON_ZERO",
		"ALLOW_INVALID_DATES",
	}
	disable := strings.Join(needDisable, ",")
	enable := strings.Join(needEnable, ",")

	mode, err := tmysql.GetSQLMode(sqlModes)
	if err != nil {
		return sqlModes, err
	}
	disableMode, err2 := tmysql.GetSQLMode(disable)
	if err2 != nil {
		return sqlModes, err2
	}
	enableMode, err3 := tmysql.GetSQLMode(enable)
	if err3 != nil {
		return sqlModes, err3
	}
	// About this bit manipulation, details can be seen
	// https://github.com/pingcap/dm/pull/1869#discussion_r669771966
	mode = (mode &^ disableMode) | enableMode

	return GetSQLModeStrBySQLMode(mode), nil
}

// GetSQLModeStrBySQLMode get string represent of sql_mode by sql_mode.
func GetSQLModeStrBySQLMode(sqlMode tmysql.SQLMode) string {
	var sqlModeStr []string
	for str, SQLMode := range tmysql.Str2SQLMode {
		if sqlMode&SQLMode != 0 {
			sqlModeStr = append(sqlModeStr, str)
		}
	}
	return strings.Join(sqlModeStr, ",")
}

// GetTableCreateSQL gets table create sql by 'show create table schema.table'.
func GetTableCreateSQL(ctx context.Context, conn *sql.Conn, tableID string) (sql string, err error) {
	querySQL := fmt.Sprintf("SHOW CREATE TABLE %s", tableID)
	var table, createStr string
	row := conn.QueryRowContext(ctx, querySQL)
	err = row.Scan(&table, &createStr)
	if err != nil {
		return "", terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}
	return createStr, nil
}
