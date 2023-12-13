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
	"github.com/pingcap/tidb/pkg/parser"
	tmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

// UpStreamConn connect to upstream DB
// Normally, we need to get some upstream information through some helper functions
// these helper functions are all easy query functions, so we use a pool of connections here
// maybe change to one connection some day.
type UpStreamConn struct {
	BaseDB *conn.BaseDB
}

// GetMasterStatus returns binlog location that extracted from SHOW MASTER STATUS.
func (c *UpStreamConn) GetMasterStatus(ctx *tcontext.Context, flavor string) (mysql.Position, mysql.GTIDSet, error) {
	pos, gtidSet, err := conn.GetPosAndGs(ctx, c.BaseDB, flavor)

	failpoint.Inject("GetMasterStatusFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("GetMasterStatus failed", zap.String("failpoint", "GetMasterStatusFailed"), zap.Error(err))
	})

	return pos, gtidSet, err
}

// GetServerUUID returns upstream server UUID.
func (c *UpStreamConn) GetServerUUID(ctx context.Context, flavor string) (string, error) {
	return conn.GetServerUUID(tcontext.NewContext(ctx, log.L()), c.BaseDB, flavor)
}

// GetServerUnixTS returns the result of current timestamp in upstream.
func (c *UpStreamConn) GetServerUnixTS(ctx context.Context) (int64, error) {
	return conn.GetServerUnixTS(ctx, c.BaseDB)
}

// GetCharsetAndCollationInfo returns charset and collation info.
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

	rows, err := conn.QuerySQL(tctx, nil, "SELECT COLLATION_NAME,CHARACTER_SET_NAME,ID,IS_DEFAULT from INFORMATION_SCHEMA.COLLATIONS")
	if err != nil {
		return nil, nil, terror.DBErrorAdapt(err, conn.Scope(), terror.ErrDBDriverError)
	}

	defer rows.Close()
	for rows.Next() {
		var collation, charset, isDefault string
		var id int
		if scanErr := rows.Scan(&collation, &charset, &id, &isDefault); scanErr != nil {
			return nil, nil, terror.DBErrorAdapt(scanErr, conn.Scope(), terror.ErrDBDriverError)
		}
		idAndCollationMap[id] = strings.ToLower(collation)
		if strings.ToLower(isDefault) == "yes" {
			charsetAndDefaultCollation[strings.ToLower(charset)] = collation
		}
	}

	if err = rows.Close(); err != nil {
		return nil, nil, terror.DBErrorAdapt(rows.Err(), conn.Scope(), terror.ErrDBDriverError)
	}
	return charsetAndDefaultCollation, idAndCollationMap, err
}

// GetParser returns the parser with correct flag for upstream.
func (c *UpStreamConn) GetParser(ctx context.Context) (*parser.Parser, error) {
	return conn.GetParser(tcontext.NewContext(ctx, log.L()), c.BaseDB)
}

// KillConn kills a connection in upstream.
func (c *UpStreamConn) KillConn(ctx context.Context, connID uint32) error {
	return conn.KillConn(tcontext.NewContext(ctx, log.L()), c.BaseDB, connID)
}

// FetchAllDoTables returns tables matches allow-list.
func (c *UpStreamConn) FetchAllDoTables(ctx context.Context, bw *filter.Filter) (map[string][]string, error) {
	return conn.FetchAllDoTables(ctx, c.BaseDB, bw)
}

// CloseUpstreamConn closes the UpStreamConn.
func CloseUpstreamConn(tctx *tcontext.Context, conn *UpStreamConn) {
	if conn != nil {
		CloseBaseDB(tctx, conn.BaseDB)
	}
}
