// Copyright 2020 PingCAP, Inc.
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
	"flag"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/tests/integration_tests/util"
)

func main() {
	cfg := util.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.S().Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	sourceDB, err := util.CreateDB(cfg.SourceDBCfg[0])
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(sourceDB); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()
	runPKorUKcases(sourceDB)
	util.MustExec(sourceDB, "create table finish_mark(a int primary key);")
}

// create a table with one column id with different type
// test the case whether it is primary key too, this can
// also help test when the column is handle or not.
func runPKorUKcases(db *sql.DB) {
	cases := []struct {
		Tp     string
		Value  interface{}
		Update interface{}
	}{
		{
			Tp:     "BIGINT UNSIGNED",
			Value:  uint64(math.MaxUint64),
			Update: uint64(math.MaxUint64) - 1,
		},
		{
			Tp:     "BIGINT SIGNED",
			Value:  int64(math.MaxInt64),
			Update: int64(math.MinInt64),
		},
		{
			Tp:     "INT UNSIGNED",
			Value:  uint32(math.MaxUint32),
			Update: uint32(math.MaxUint32) - 1,
		},
		{
			Tp:     "INT SIGNED",
			Value:  int32(math.MaxInt32),
			Update: int32(math.MinInt32),
		},
		{
			Tp:     "SMALLINT UNSIGNED",
			Value:  uint16(math.MaxUint16),
			Update: uint16(math.MaxUint16) - 1,
		},
		{
			Tp:     "SMALLINT SIGNED",
			Value:  int16(math.MaxInt16),
			Update: int16(math.MinInt16),
		},
		{
			Tp:     "TINYINT UNSIGNED",
			Value:  uint8(math.MaxUint8),
			Update: uint8(math.MaxUint8) - 1,
		},
		{
			Tp:     "TINYINT SIGNED",
			Value:  int8(math.MaxInt8),
			Update: int8(math.MaxInt8) - 1,
		},
	}

	var g sync.WaitGroup

	for i, c := range cases {
		for j, pkOrUK := range []string{"UNIQUE NOT NULL", "PRIMARY KEY"} {
			g.Add(1)
			tableName := fmt.Sprintf("pk_or_uk_%d_%d", i, j)
			pkOrUK := pkOrUK
			c := c
			go func() {
				sql := fmt.Sprintf("CREATE TABLE %s(id %s %s)", tableName, c.Tp, pkOrUK)
				util.MustExec(db, sql)
				sql = fmt.Sprintf("INSERT INTO %s(id) values( ? )", tableName)
				util.MustExec(db, sql, c.Value)
				sql = fmt.Sprintf("UPDATE %s set id = ? where id = ?", tableName)
				util.MustExec(db, sql, c.Update, c.Value)
				sql = fmt.Sprintf("INSERT INTO %s(id) values( ? )", tableName)
				util.MustExec(db, sql, c.Value)
				sql = fmt.Sprintf("DELETE from %s where id = ?", tableName)
				util.MustExec(db, sql, c.Update)
				g.Done()
			}()
		}
	}
	g.Wait()
}
