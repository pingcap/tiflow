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
	"math/rand"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	numTables          = 10
	numQueriesPerTable = 200
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

	errg := new(errgroup.Group)
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("test_table_%d", i)
		errg.Go(func() error {
			err := createTable(sourceDB, tableName)
			if err != nil {
				return errors.Trace(err)
			}
			err = doDMLs(sourceDB, tableName, numQueriesPerTable)
			return err
		})
	}

	err = errg.Wait()
	if err != nil {
		log.Fatal("process_panic data insertion failed", zap.Error(err))
	}

	err = createTable(sourceDB, "end_mark_table")
	if err != nil {
		log.Fatal("process_panic could not create end_mark_table")
	}
}

func createTable(db *sql.DB, tableName string) error {
	_, err := db.Exec("CREATE table " + quotes.QuoteName(tableName) + " (id int primary key, v1 int, v2 int)")
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func doDMLs(db *sql.DB, tableName string, numQueries int) error {
	log.Info("Start inserting data", zap.String("tableName", tableName), zap.Int("numQueries", numQueries))
	stmt, err := db.Prepare("insert into " + quotes.QuoteName(tableName) + " (id, v1, v2) values (?, ?, ?)")
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i <= numQueries; i++ {
		_, err := stmt.Exec(i, rand.Int()%65535, rand.Int()%65536)
		if err != nil {
			return errors.Trace(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("Finished inserting data", zap.String("tableName", tableName))
	return nil
}
