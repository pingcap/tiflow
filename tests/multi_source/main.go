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
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/tests/util"
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

	sourceDB0, err := util.CreateDB(cfg.SourceDBCfg[0])
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(sourceDB0); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()
	sourceDB1, err := util.CreateDB(cfg.SourceDBCfg[1])
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := util.CloseDB(sourceDB1); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()
	util.MustExec(sourceDB0, "create database mark;")
	runDDLTest([]*sql.DB{sourceDB0, sourceDB1})
	util.MustExec(sourceDB0, "create table mark.finish_mark(a int primary key);")
}

// for every DDL, run the DDL continuously, and one goroutine for one TiDB instance to do some DML op
func runDDLTest(srcs []*sql.DB) {
	runTime := time.Second * 5
	start := time.Now()
	defer func() {
		log.S().Infof("runDDLTest take %v", time.Since(start))
	}()

	for i, ddlFunc := range []func(context.Context, *sql.DB){createDropSchemaDDL, truncateDDL, addDropColumnDDL,
		modifyColumnDDL, addDropIndexDDL} {
		testName := getFunctionName(ddlFunc)
		log.S().Info("running ddl test: ", i, " ", testName)

		var wg sync.WaitGroup
		ctx, cancel := context.WithTimeout(context.Background(), runTime)

		for idx, src := range srcs {
			wg.Add(1)
			go func(i int, s *sql.DB) {
				dml(ctx, s, testName, i)
				wg.Done()
			}(idx, src)
		}

		time.Sleep(time.Millisecond)

		wg.Add(1)
		go func() {
			ddlFunc(ctx, srcs[0])
			wg.Done()
		}()

		wg.Wait()
		time.Sleep(5 * time.Second)
		cancel()

		util.MustExec(srcs[0], fmt.Sprintf("create table mark.finish_mark_%d(a int primary key);", i))
	}
}

func getFunctionName(i interface{}) string {
	strs := strings.Split(runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name(), ".")
	return strs[len(strs)-1]
}

func createDropSchemaDDL(ctx context.Context, db *sql.DB) {
	testName := getFunctionName(createDropSchemaDDL)
	/*
	   mysql> use test;
	   Database changed
	   mysql> create table test1(id int);
	   Query OK, 0 rows affected (0.05 sec)

	   mysql> drop database test;
	   Query OK, 3 rows affected (0.02 sec)

	   mysql> create database test;
	   Query OK, 1 row affected (0.02 sec)

	   mysql> create table test1(id int);
	   ERROR 1046 (3D000): No database selected
	*/
	// drop the database used will make the session become No database selected
	// this make later code use *sql.DB* fail as expected
	// so we setback the used db before close the conn
	conn, err := db.Conn(ctx)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		conn.Close()
	}()

	for {
		mustCreateTableWithConn(ctx, conn, testName)
		select {
		case <-ctx.Done():
			return
		default:
		}
		time.Sleep(100 * time.Millisecond)
		util.MustExecWithConn(ctx, conn, "drop database test")
	}
}

func truncateDDL(ctx context.Context, db *sql.DB) {
	testName := getFunctionName(truncateDDL)
	mustCreateTable(db, testName)

	sql := fmt.Sprintf("truncate table test.`%s`", testName)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		util.MustExec(db, sql)
		time.Sleep(100 * time.Millisecond)
	}
}

func ignoreableError(err error) bool {
	knownErrorList := []string{
		"Error 1146:", // table doesn't exist
		"Error 1049",  // database doesn't exist
	}
	for _, e := range knownErrorList {
		if strings.HasPrefix(err.Error(), e) {
			return true
		}
	}
	return false
}

func dml(ctx context.Context, db *sql.DB, table string, id int) {
	var err error
	var i int
	var insertSuccess int
	var deleteSuccess int
	insertSQL := fmt.Sprintf("insert into test.`%s`(id1, id2) values(?,?)", table)
	deleteSQL := fmt.Sprintf("delete from test.`%s` where id1 = ? or id2 = ?", table)
	for i = 0; ; i++ {
		_, err = db.Exec(insertSQL, i+id*100000000, i+id*100000000+1)
		if err == nil {
			insertSuccess++
			if insertSuccess%100 == 0 {
				log.S().Info(id, " insert success: ", insertSuccess)
			}
		}
		if err != nil && !ignoreableError(err) {
			log.Fatal("unexpected error when executing sql", zap.Error(err))
		}

		if i%2 == 0 {
			result, err := db.Exec(deleteSQL, i+id*100000000, i+id*100000000+1)
			if err == nil {
				rows, _ := result.RowsAffected()
				if rows != 0 {
					deleteSuccess++
					if deleteSuccess%100 == 0 {
						log.S().Info(id, " delete success: ", deleteSuccess)
					}
				}
			}
			if err != nil && !ignoreableError(err) {
				log.Fatal("unexpected error when executing sql", zap.Error(err))
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func addDropColumnDDL(ctx context.Context, db *sql.DB) {
	testName := getFunctionName(addDropColumnDDL)
	mustCreateTable(db, testName)

	for value := 1; ; value++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		sql := fmt.Sprintf("alter table test.`%s` drop column v1", testName)
		util.MustExec(db, sql)
		time.Sleep(100 * time.Millisecond)

		var notNULL string
		var defaultValue interface{}

		if value%5 == 0 {
			// use default <value> not null
			notNULL = "not null"
			defaultValue = value
		} else if value%5 == 1 {
			// use default null
			defaultValue = nil
		} else {
			// use default <value>
			defaultValue = value
		}
		sql = fmt.Sprintf("alter table test.`%s` add column v1 int default ? %s", testName, notNULL)
		util.MustExec(db, sql, defaultValue)
		time.Sleep(100 * time.Millisecond)
	}
}

func modifyColumnDDL(ctx context.Context, db *sql.DB) {
	testName := getFunctionName(modifyColumnDDL)
	mustCreateTable(db, testName)
	sql := fmt.Sprintf("alter table test.`%s` modify column v1 int default ?", testName)
	for value := 1; ; value++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var defaultValue interface{}
		// use default null per five modify
		if value%5 == 0 {
			defaultValue = nil
		} else {
			defaultValue = value
		}
		util.MustExec(db, sql, defaultValue)
		time.Sleep(100 * time.Millisecond)
	}
}

func addDropIndexDDL(ctx context.Context, db *sql.DB) {
	testName := getFunctionName(addDropIndexDDL)
	mustCreateTable(db, testName)

	for value := 1; ; value++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		sql := fmt.Sprintf("drop index id1 on test.`%s`;", testName)
		util.MustExec(db, sql)
		time.Sleep(100 * time.Millisecond)

		sql = fmt.Sprintf("create unique index `id1` on test.`%s` (id1);", testName)
		util.MustExec(db, sql)
		time.Sleep(100 * time.Millisecond)

		sql = fmt.Sprintf("drop index id2 on test.`%s`;", testName)
		util.MustExec(db, sql)
		time.Sleep(100 * time.Millisecond)

		sql = fmt.Sprintf("create unique index `id2` on test.`%s` (id2);", testName)
		util.MustExec(db, sql)
		time.Sleep(100 * time.Millisecond)
	}
}

const createDatabaseSQL = "create database if not exists test"
const createTableSQL = `
create table if not exists test.%s
(
    id1 int unique key not null,
    id2 int unique key not null,
    v1  int default null
)
`

func mustCreateTable(db *sql.DB, tableName string) {
	util.MustExec(db, createDatabaseSQL)
	sql := fmt.Sprintf(createTableSQL, tableName)
	util.MustExec(db, sql)
}

func mustCreateTableWithConn(ctx context.Context, conn *sql.Conn, tableName string) {
	util.MustExecWithConn(ctx, conn, createDatabaseSQL)
	sql := fmt.Sprintf(createTableSQL, tableName)
	util.MustExecWithConn(ctx, conn, sql)
}
