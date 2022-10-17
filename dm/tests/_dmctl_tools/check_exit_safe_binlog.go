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

package main

import (
	"database/sql"
	"fmt"
	"os"

	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/tests/utils"
)

var compareMap = map[string]map[int]struct{}{
	">":  {1: {}},
	">=": {1: {}, 0: {}},
	"=":  {0: {}},
	"<=": {-1: {}, 0: {}},
	"<":  {-1: {}},
}

func parseBinlogLocation(binlogName string, binlogPos uint32, binlogGTID string) (binlog.Location, error) {
	gset, err := gtid.ParserGTID("mysql", binlogGTID) // default to "".
	if err != nil {
		return binlog.Location{}, err
	}
	return binlog.NewLocation(
		mysql.Position{
			Name: binlogName,
			Pos:  binlogPos,
		},
		gset,
	), nil
}

// use show-ddl-locks request to test DM-master is online
func main() {
	password := os.Args[1]
	sourceID := os.Args[2]
	compareGTIDStr := os.Args[3]
	compare := os.Args[4]

	const (
		host = "127.0.0.1"
		user = "test"
		port = 4000
	)

	err := log.InitLogger(&log.Config{Level: "info"})
	if err != nil {
		utils.ExitWithError(err)
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true&maxAllowedPacket=0",
		user, password, host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		utils.ExitWithError(err)
	}

	var (
		binlogName, exitSafeBinlogName string
		binlogPos, exitSafeBinlogPos   uint32
		binlogGTID, exitSafeBinlogGTID sql.NullString
	)
	err = db.QueryRow("SELECT binlog_name,binlog_pos,binlog_gtid,exit_safe_binlog_name,exit_safe_binlog_pos,exit_safe_binlog_gtid FROM dm_meta.test_syncer_checkpoint WHERE is_global=1 AND id=?", sourceID).
		Scan(&binlogName, &binlogPos, &binlogGTID, &exitSafeBinlogName, &exitSafeBinlogPos, &exitSafeBinlogGTID)
	if err != nil {
		utils.ExitWithError(err)
	}

	globalLoc, err := parseBinlogLocation(binlogName, binlogPos, binlogGTID.String)
	if err != nil {
		utils.ExitWithError(err)
	}
	exitSafeModeLoc, err := parseBinlogLocation(exitSafeBinlogName, exitSafeBinlogPos, exitSafeBinlogGTID.String)
	if err != nil {
		utils.ExitWithError(err)
	}
	compareGTID := false
	if compareGTIDStr == "true" {
		compareGTID = true
	}
	t := binlog.CompareLocation(globalLoc, exitSafeModeLoc, compareGTID)
	if mp, ok := compareMap[compare]; ok {
		if _, ok = mp[t]; ok {
			fmt.Println("success")
		} else {
			utils.ExitWithError(errors.Errorf("unmatched compare %s and compare result %d", compare, t))
		}
	} else {
		utils.ExitWithError(errors.Errorf("unknown compare %s", compare))
	}
}
