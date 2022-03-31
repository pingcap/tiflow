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

package dbconn

import (
	"context"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/filter"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// UpStreamConn connect to upstream DB
// Normally, we need to get some upstream information through some helper functions
// these helper functions are all easy query functions, so we use a pool of connections here
// maybe change to one connection some day.
type UpStreamConn struct {
	BaseDB *conn.BaseDB
}

// NewUpStreamConn creates an UpStreamConn from config.
func NewUpStreamConn(dbCfg *config.DBConfig) (*UpStreamConn, error) {
	baseDB, err := CreateBaseDB(dbCfg)
	if err != nil {
		return nil, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeUpstream)
	}
	return &UpStreamConn{BaseDB: baseDB}, nil
}

// GetMasterStatus returns binlog location that extracted from SHOW MASTER STATUS.
func (conn *UpStreamConn) GetMasterStatus(ctx context.Context, flavor string) (mysql.Position, gtid.Set, error) {
	pos, gtidSet, err := utils.GetPosAndGs(ctx, conn.BaseDB.DB, flavor)

	failpoint.Inject("GetMasterStatusFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("GetMasterStatus failed", zap.String("failpoint", "GetMasterStatusFailed"), zap.Error(err))
	})

	return pos, gtidSet, err
}

// GetServerUUID returns upstream server UUID.
func (conn *UpStreamConn) GetServerUUID(ctx context.Context, flavor string) (string, error) {
	return utils.GetServerUUID(ctx, conn.BaseDB.DB, flavor)
}

// GetServerUnixTS returns the result of current timestamp in upstream.
func (conn *UpStreamConn) GetServerUnixTS(ctx context.Context) (int64, error) {
	return utils.GetServerUnixTS(ctx, conn.BaseDB.DB)
}

// GetCharsetAndDefaultCollation returns charset and collation info.
func GetCharsetAndCollationInfo(tctx *tcontext.Context, conn *DBConn) (map[string]string, map[int]string, error) {
	charsetAndDefaultCollation := make(map[string]string)
	idAndCollationMap := make(map[int]string)

	// Show an example.
	/*
		mysql> SELECT COLLATION_NAME,CHARACTER_SET_NAME,ID,IS_DEFAULT from INFORMATION_SCHEMA.COLLATIONS;
		+----------------------------+--------------------+-----+------------+
		| COLLATION_NAME             | CHARACTER_SET_NAME | ID  | IS_DEFAULT |
		+----------------------------+--------------------+-----+------------+
		| armscii8_general_ci        | armscii8           |  32 | Yes        |
		| armscii8_bin               | armscii8           |  64 |            |
		| ascii_general_ci           | ascii              |  11 | Yes        |
		| ascii_bin                  | ascii              |  65 |            |
		| big5_chinese_ci            | big5               |   1 | Yes        |
		| big5_bin                   | big5               |  84 |            |
		| binary                     | binary             |  63 | Yes        |
		+----------------------------+--------------------+-----+------------+
	*/

	rows, err := conn.QuerySQL(tctx, "SELECT COLLATION_NAME,CHARACTER_SET_NAME,ID,IS_DEFAULT from INFORMATION_SCHEMA.COLLATIONS")
	if err != nil {
		return nil, nil, terror.DBErrorAdapt(err, terror.ErrDBDriverError)
	}

	defer rows.Close()
	for rows.Next() {
		var collation, charset, isDefault string
		var id int
		if scanErr := rows.Scan(&collation, &charset, &id, &isDefault); scanErr != nil {
			return nil, nil, terror.DBErrorAdapt(scanErr, terror.ErrDBDriverError)
		}
		idAndCollationMap[id] = strings.ToLower(collation)
		if strings.ToLower(isDefault) == "yes" {
			charsetAndDefaultCollation[strings.ToLower(charset)] = collation
		}
	}

	if err = rows.Close(); err != nil {
		return nil, nil, terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError)
	}
	return charsetAndDefaultCollation, idAndCollationMap, err
}

// GetParser returns the parser with correct flag for upstream.
func (conn *UpStreamConn) GetParser(ctx context.Context) (*parser.Parser, error) {
	return utils.GetParser(ctx, conn.BaseDB.DB)
}

// KillConn kills a connection in upstream.
func (conn *UpStreamConn) KillConn(ctx context.Context, connID uint32) error {
	return utils.KillConn(ctx, conn.BaseDB.DB, connID)
}

// FetchAllDoTables returns tables matches allow-list.
func (conn *UpStreamConn) FetchAllDoTables(ctx context.Context, bw *filter.Filter) (map[string][]string, error) {
	return utils.FetchAllDoTables(ctx, conn.BaseDB.DB, bw)
}

// CloseUpstreamConn closes the UpStreamConn.
func CloseUpstreamConn(tctx *tcontext.Context, conn *UpStreamConn) {
	if conn != nil {
		CloseBaseDB(tctx, conn.BaseDB)
	}
}
