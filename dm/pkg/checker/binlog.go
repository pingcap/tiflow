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

package checker

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/errors"
)

// MySQLBinlogEnableChecker checks whether `log_bin` variable is enabled in MySQL.
type MySQLBinlogEnableChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogEnableChecker returns a RealChecker.
func NewMySQLBinlogEnableChecker(db *sql.DB, dbinfo *dbutil.DBConfig) RealChecker {
	return &MySQLBinlogEnableChecker{db: db, dbinfo: dbinfo}
}

// Check implements the RealChecker interface.
func (pc *MySQLBinlogEnableChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql binlog is enabled",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	value, err := dbutil.ShowLogBin(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if strings.ToUpper(value) != "ON" {
		result.Errors = append(result.Errors, NewError("log_bin is %s, and should be ON", value))
		result.Instruction = "MySQL as source: please refer to the document to enable the binlog https://dev.mysql.com/doc/refman/5.7/en/replication-howto-masterbaseconfig.html"
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the RealChecker interface.
func (pc *MySQLBinlogEnableChecker) Name() string {
	return "mysql_binlog_enable"
}

/*****************************************************/

// MySQLBinlogFormatChecker checks mysql binlog_format.
type MySQLBinlogFormatChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogFormatChecker returns a RealChecker.
func NewMySQLBinlogFormatChecker(db *sql.DB, dbinfo *dbutil.DBConfig) RealChecker {
	return &MySQLBinlogFormatChecker{db: db, dbinfo: dbinfo}
}

// Check implements the RealChecker interface.
func (pc *MySQLBinlogFormatChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql binlog_format is ROW",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	value, err := dbutil.ShowBinlogFormat(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if strings.ToUpper(value) != "ROW" {
		result.Errors = append(result.Errors, NewError("binlog_format is %s, and should be ROW", value))
		result.Instruction = "MySQL as source: please execute 'set global binlog_format=ROW;'; AWS Aurora (MySQL)/RDS MySQL as source: please refer to the document to create a new DB parameter group and set the binlog_format=row: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithDBInstanceParamGroups.html. Then modify the instance to use the new DB parameter group and restart the instance to take effect."
		return result
	}
	result.State = StateSuccess

	return result
}

// Name implements the RealChecker interface.
func (pc *MySQLBinlogFormatChecker) Name() string {
	return "mysql_binlog_format"
}

/*****************************************************/

var (
	mysqlBinlogRowImageRequired   MySQLVersion = [3]uint{5, 6, 2}
	mariaDBBinlogRowImageRequired MySQLVersion = [3]uint{10, 1, 6}
)

// MySQLBinlogRowImageChecker checks mysql binlog_row_image.
type MySQLBinlogRowImageChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLBinlogRowImageChecker returns a RealChecker.
func NewMySQLBinlogRowImageChecker(db *sql.DB, dbinfo *dbutil.DBConfig) RealChecker {
	return &MySQLBinlogRowImageChecker{db: db, dbinfo: dbinfo}
}

// Check implements the RealChecker interface.
// 'binlog_row_image' is introduced since mysql 5.6.2, and mariadb 10.1.6.
// > In MySQL 5.5 and earlier, full row images are always used for both before images and after images.
// So we need check 'binlog_row_image' after mysql 5.6.2 version and mariadb 10.1.6.
// ref:
// - https://dev.mysql.com/doc/refman/5.6/en/replication-options-binary-log.html#sysvar_binlog_row_image
// - https://mariadb.com/kb/en/library/replication-and-binary-log-server-system-variables/#binlog_row_image
func (pc *MySQLBinlogRowImageChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql binlog_row_image is FULL",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	// check version firstly
	value, err := dbutil.ShowVersion(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	version, err := toMySQLVersion(value)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	// for mysql.version < 5.6.2,  we don't need to check binlog_row_image.
	if !version.Ge(mysqlBinlogRowImageRequired) {
		result.State = StateSuccess
		return result
	}

	// for mariadb.version < 10.1.6.,  we don't need to check binlog_row_image.
	if conn.IsMariaDB(value) && !version.Ge(mariaDBBinlogRowImageRequired) {
		result.State = StateSuccess
		return result
	}

	value, err = dbutil.ShowBinlogRowImage(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if strings.ToUpper(value) != "FULL" {
		result.Errors = append(result.Errors, NewError("binlog_row_image is %s, and should be FULL", value))
		result.Instruction = "MySQL as source: please execute 'set global binlog_row_image = FULL;'; AWS Aurora (MySQL)/RDS MySQL as source: please refer to the document to create a new DB parameter group and set the binlog_row_image = FULL: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithDBInstanceParamGroups.html Then modify the instance to use the new DB parameter group and restart the instance to take effect."
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the RealChecker interface.
func (pc *MySQLBinlogRowImageChecker) Name() string {
	return "mysql_binlog_row_image"
}

// BinlogDBChecker checks if migrated dbs are in binlog_do_db or binlog_ignore_db.
type BinlogDBChecker struct {
	db            *conn.BaseDB
	dbinfo        *dbutil.DBConfig
	schemas       map[string]struct{}
	caseSensitive bool
}

// NewBinlogDBChecker returns a RealChecker.
func NewBinlogDBChecker(db *conn.BaseDB, dbinfo *dbutil.DBConfig, schemas map[string]struct{}, caseSensitive bool) RealChecker {
	newSchemas := make(map[string]struct{}, len(schemas))
	for schema := range schemas {
		newSchemas[schema] = struct{}{}
	}
	return &BinlogDBChecker{db: db, dbinfo: dbinfo, schemas: newSchemas, caseSensitive: caseSensitive}
}

// Check implements the RealChecker interface.
func (c *BinlogDBChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check whether migrated dbs are in binlog_do_db/binlog_ignore_db",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", c.dbinfo.Host, c.dbinfo.Port),
	}

	flavor, err := conn.GetFlavor(ctx, c.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	tctx := tcontext.NewContext(ctx, log.L())
	binlogDoDB, binlogIgnoreDB, err := conn.GetBinlogDB(tctx, c.db, flavor)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if !c.caseSensitive {
		binlogDoDB = strings.ToLower(binlogDoDB)
		binlogIgnoreDB = strings.ToLower(binlogIgnoreDB)
	}
	binlogDoDBs := strings.Split(binlogDoDB, ",")
	binlogIgnoreDBs := strings.Split(binlogIgnoreDB, ",")
	// MySQL will check –binlog-do-db first, if there are any options,
	// it will apply this one and ignore –binlog-ignore-db. If the
	// –binlog-do-db is NOT set, then mysql will check –binlog-ignore-db.
	// If both of them are empty, it will log changes for all DBs.
	if len(binlogDoDB) != 0 {
		for _, doDB := range binlogDoDBs {
			delete(c.schemas, doDB)
		}
		if len(c.schemas) > 0 {
			dbs := utils.SetToSlice(c.schemas)
			result.Errors = append(result.Errors, NewWarn("these dbs [%s] are not in binlog_do_db[%s]", strings.Join(dbs, ","), binlogDoDB))
			result.Instruction = "Ensure that the do_dbs contains the dbs you want to migrate"
			return result
		}
	} else {
		ignoreDBs := []string{}
		for _, ignoreDB := range binlogIgnoreDBs {
			if _, ok := c.schemas[ignoreDB]; ok {
				ignoreDBs = append(ignoreDBs, ignoreDB)
			}
		}
		if len(ignoreDBs) > 0 {
			result.Errors = append(result.Errors, NewWarn("these dbs [%s] are in binlog_ignore_db[%s]", strings.Join(ignoreDBs, ","), binlogIgnoreDB))
			result.Instruction = "Ensure that the ignore_dbs does not contain the dbs you want to migrate"
			return result
		}
	}
	result.State = StateSuccess
	return result
}

// Name implements the RealChecker interface.
func (c *BinlogDBChecker) Name() string {
	return "binlog_do_db/binlog_ignore_db check"
}

// MetaPositionChecker checks if meta position for given source database is valid.
type MetaPositionChecker struct {
	db         *conn.BaseDB
	sourceCfg  dbconfig.DBConfig
	enableGTID bool
	meta       *config.Meta
}

// NewBinlogDBChecker returns a RealChecker.
func NewMetaPositionChecker(db *conn.BaseDB, sourceCfg dbconfig.DBConfig, enableGTID bool, meta *config.Meta) RealChecker {
	return &MetaPositionChecker{db: db, sourceCfg: sourceCfg, enableGTID: enableGTID, meta: meta}
}

// Check implements the RealChecker interface.
func (c *MetaPositionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check whether meta position is valid for db",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", c.sourceCfg.Host, c.sourceCfg.Port),
	}

	var tlsConfig *tls.Config
	var err error
	if c.sourceCfg.Security != nil {
		if loadErr := c.sourceCfg.Security.LoadTLSContent(); loadErr != nil {
			markCheckError(result, loadErr)
			return result
		}
		tlsConfig, err = util.NewTLSConfig(
			util.WithCAContent(c.sourceCfg.Security.SSLCABytes),
			util.WithCertAndKeyContent(c.sourceCfg.Security.SSLCertBytes, c.sourceCfg.Security.SSLKeyBytes),
			util.WithVerifyCommonName(c.sourceCfg.Security.CertAllowedCN),
		)
		if err != nil {
			markCheckError(result, err)
			return result
		}
	}

	flavor, err := conn.GetFlavor(ctx, c.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	// always use a new random serverID
	randomServerID, err := conn.GetRandomServerID(tcontext.NewContext(ctx, log.L()), c.db)
	if err != nil {
		// should never happened unless the master has too many slave
		markCheckError(result, terror.Annotate(err, "fail to get random server id for relay reader"))
		return result
	}

	h, _ := os.Hostname()
	h = "dm-checker-" + h
	// https://github.com/mysql/mysql-server/blob/1bfe02bdad6604d54913c62614bde57a055c8332/include/my_hostname.h#L33-L42
	if len(h) > 60 {
		h = h[:60]
	}

	syncCfg := replication.BinlogSyncerConfig{
		ServerID:  randomServerID,
		Flavor:    flavor,
		Host:      c.sourceCfg.Host,
		Port:      uint16(c.sourceCfg.Port),
		User:      c.sourceCfg.User,
		Password:  c.sourceCfg.Password,
		TLSConfig: tlsConfig,
		Localhost: h,
	}

	syncer := replication.NewBinlogSyncer(syncCfg)
	defer syncer.Close()
	var streamer *replication.BinlogStreamer
	if c.enableGTID {
		gtidSet, err2 := gtid.ParserGTID(flavor, c.meta.BinLogGTID)
		if err2 != nil {
			markCheckError(result, err2)
			return result
		}
		streamer, err = syncer.StartSyncGTID(gtidSet)
	} else {
		streamer, err = syncer.StartSync(mysql.Position{Name: c.meta.BinLogName, Pos: c.meta.BinLogPos})
	}
	if err != nil {
		markCheckError(result, err)
		return result
	}
	// if we don't get a new event after 15s, it means there is no new event in the binlog
	ctx2, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = streamer.GetEvent(ctx2)
	if err != nil && errors.Cause(err) != context.DeadlineExceeded {
		markCheckError(result, err)
		return result
	}

	result.State = StateSuccess
	return result
}

// Name implements the RealChecker interface.
func (c *MetaPositionChecker) Name() string {
	return "meta position check"
}
