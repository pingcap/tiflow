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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"go.uber.org/zap"
)

var finishIdx int

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
	testGetZeroValue([]*sql.DB{sourceDB0, sourceDB1})
	testGetDefaultValue([]*sql.DB{sourceDB0, sourceDB1})
	util.MustExec(sourceDB0, "create table mark.finish_mark(a int primary key);")
}

// for every DDL, run the DDL continuously, and one goroutine for one TiDB instance to do some DML op
func testGetDefaultValue(srcs []*sql.DB) {
	runTime := time.Second * 60
	start := time.Now()
	defer func() {
		log.S().Infof("testGetDefaultValue take %v", time.Since(start))
	}()

	var total int
	var cf context.CancelFunc
	for i, ddlFunc := range []func(context.Context, *sql.DB){
		addDropColumnDDL, modifyColumnDefaultValueDDL1, modifyColumnDefaultValueDDL2,
	} {
		testName := getFunctionName(ddlFunc)
		log.S().Info("running ddl test: ", i, " ", testName)

		var wg sync.WaitGroup
		ctx, cancel := context.WithTimeout(context.Background(), runTime)
		cf = cancel

		for idx, src := range srcs {
			wg.Add(1)
			go func(i int, s *sql.DB) {
				dml(ctx, s, testName, i, nil)
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

		util.MustExec(srcs[0], fmt.Sprintf("create table mark.finish_mark_%d(a int primary key);", i+finishIdx))
		total++
	}

	finishIdx += total
	cf() // ignore go lint error
}

func getFunctionName(i interface{}) string {
	strs := strings.Split(runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name(), ".")
	return strs[len(strs)-1]
}

func ignoreableError(err error) bool {
	knownErrorList := []string{
		"Error 1146", // table doesn't exist
		"Error 1049", // database doesn't exist
		"Error 1054", // unknown column
	}
	for _, e := range knownErrorList {
		if strings.HasPrefix(err.Error(), e) {
			return true
		}
	}
	return false
}

func dml(ctx context.Context, db *sql.DB, table string, id int, defaultValue interface{}) {
	var err error
	var i int
	var insertSuccess int
	var deleteSuccess int
	var insertSQL string
	var updateSQL string

	if defaultValue != nil {
		insertSQL = fmt.Sprintf("insert into test.`%s`(id1, id2, v1) values(?,?,?)", table)
	} else {
		insertSQL = fmt.Sprintf("insert into test.`%s`(id1, id2) values(?,?)", table)
	}
	deleteSQL := fmt.Sprintf("delete from test.`%s` where id1 = ? or id2 = ?", table)

	// When meet `not null+no default` and `update`, it may trigger strict sql mode error
	if defaultValue == nil {
		updateSQL = fmt.Sprintf("update test.`%s` set v0=13 where id1 = ? or id2 = ?", table)
	}

	for i = 0; ; i++ {
		if defaultValue != nil {
			_, err = db.Exec(insertSQL, i+id*10000000, i+id*10000000+1, defaultValue)
		} else {
			_, err = db.Exec(insertSQL, i+id*10000000, i+id*10000000+1)
		}
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
			if defaultValue == nil {
				_, err := db.Exec(updateSQL, i+id*100000000, i+id*100000000+1)
				if err != nil && !ignoreableError(err) {
					log.Fatal("unexpected error when executing sql", zap.Error(err))
				}
			}

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

// Test `add column default null` + `modify column default xxx`/`alter column set default xxx`
// DefaultValue will be changed by modify column/alter column, but OriginalDefaultValue
// is only assigned by `add column default xxx`
func modifyColumnDefaultValueDDL1(ctx context.Context, db *sql.DB) {
	testName := getFunctionName(modifyColumnDefaultValueDDL1)
	mustCreateTable(db, testName)

	modifyColumnFmts := []string{
		"alter table test.`%s` modify column v1 int default ?",
		"alter table test.`%s` alter column v1 set default ?",
	}

	for _, modifyFmt := range modifyColumnFmts {
		for value := 1; value < 10; value++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			var defaultValue interface{}

			if value%5 != 0 {
				defaultValue = value
			} else {
				// use default null
				defaultValue = nil
			}

			sql := fmt.Sprintf(modifyFmt, testName)
			util.MustExec(db, sql, defaultValue)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Test `add column default xxx` + `modify column default xxx`/`alter column set default xxx`
// DefaultValue will be changed by modify column/alter column, but OriginalDefaultValue
// is only assigned by `add column default xxx`
func modifyColumnDefaultValueDDL2(ctx context.Context, db *sql.DB) {
	testName := getFunctionName(modifyColumnDefaultValueDDL2)
	mustCreateTable(db, testName)
	sql := fmt.Sprintf("alter table test.`%s` drop column v1", testName)
	util.MustExec(db, sql)
	time.Sleep(100 * time.Millisecond)
	sql = fmt.Sprintf("alter table test.`%s` add column v1 int default 11", testName)
	util.MustExec(db, sql)
	time.Sleep(100 * time.Millisecond)

	modifyColumnFmts := []string{
		"alter table test.`%s` modify column v1 int default ?",
		"alter table test.`%s` alter column v1 set default ? ",
	}

	for _, modifyFmt := range modifyColumnFmts {
		for value := 1; value < 10; value++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			var defaultValue interface{}

			if value%5 != 0 {
				defaultValue = value
			} else {
				// use default null
				defaultValue = nil
			}

			sql := fmt.Sprintf(modifyFmt, testName)
			util.MustExec(db, sql, defaultValue)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// [TODO] add some dynamic type here, CURRENT_TIMESTAMP
// Ref: https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html

// Test add column with different column type
// All OriginalDefaultValue is string type
func addDropColumnDDL(ctx context.Context, db *sql.DB) {
	testName := getFunctionName(addDropColumnDDL)
	mustCreateTable(db, testName)

	type addColumnUnit struct {
		AddFmt       string
		DefaultValue interface{}
	}

	addColumnUnits := []addColumnUnit{
		// date and time data type
		{
			AddFmt:       "alter table test.`%s` add column v1 date default ? %s",
			DefaultValue: "2020-10-10",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 datetime default ? %s",
			DefaultValue: "2020-10-10 10:10:10",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 timestamp default ? %s",
			DefaultValue: "2020-10-10 10:10:10",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 time default ? %s",
			DefaultValue: "10:10:10",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 year default ? %s",
			DefaultValue: "2020",
		},
		{
			// For int year default
			AddFmt:       "alter table test.`%s` add column v1 year default ? %s",
			DefaultValue: 2020,
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 datetime(5) default ? %s",
			DefaultValue: "2020-10-10 10:10:10.9999",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 timestamp(5) default ? %s",
			DefaultValue: "2020-10-10 10:10:10.9999",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 time(5) default ? %s",
			DefaultValue: "10:10:10.9999",
		},

		// numeric data type
		{
			// default bit[1]
			AddFmt:       "alter table test.`%s` add column v1 bit default ? %s",
			DefaultValue: []byte{0x01},
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 tinyint default ? %s",
			DefaultValue: -13,
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 mediumint default ? %s",
			DefaultValue: -13,
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 int default ? %s",
			DefaultValue: -13,
		},
		{
			"alter table test.`%s` add column v1 bigint default ? %s",
			-13,
		},
		{
			"alter table test.`%s` add column v1 decimal(5) default ? %s",
			-13,
		},
		{
			"alter table test.`%s` add column v1 float default ? %s",
			-13.13,
		},
		{
			"alter table test.`%s` add column v1 double default ? %s",
			-13.13,
		},
		{
			"alter table test.`%s` add column v1 bit(4) default ? %s",
			[]byte{0x03},
		},
		{
			"alter table test.`%s` add column v1 tinyint(4) unsigned default ? %s",
			13,
		},
		{
			"alter table test.`%s` add column v1 mediumint(4) unsigned default ? %s",
			13,
		},
		{
			"alter table test.`%s` add column v1 int(4) unsigned default ? %s",
			13,
		},
		{
			"alter table test.`%s` add column v1 bigint(4) unsigned default ? %s",
			13,
		},
		{
			"alter table test.`%s` add column v1 decimal(5,2) unsigned default ? %s",
			13.13,
		},
		{
			"alter table test.`%s` add column v1 float(5,2) unsigned default ? %s",
			13.13,
		},
		{
			"alter table test.`%s` add column v1 double(5,2) unsigned default ? %s",
			13.13,
		},

		// string data type
		{
			"alter table test.`%s` add column v1 char(10) default ? %s",
			"char",
		},
		{
			"alter table test.`%s` add column v1 varchar(10) default ? %s",
			"varchar",
		},
		{
			"alter table test.`%s` add column v1 binary(10) default ? %s",
			"binary",
		},
		{
			"alter table test.`%s` add column v1 varbinary(10) default ? %s",
			"varbinary",
		},
		/*
			// The BLOB, TEXT, GEOMETRY, and JSON data types cannot be assigned a default value.
			{
				"alter table test.`%s` add column v1 blob default ? %s",
				"blob",
			},
			// The BLOB, TEXT, GEOMETRY, and JSON data types cannot be assigned a default value.
			{
				"alter table test.`%s` add column v1 text default ? %s",
				"text",
			},
		*/
		{
			"alter table test.`%s` add column v1 enum('e0', 'e1') default ? %s",
			"e1",
		},
		{
			"alter table test.`%s` add column v1 set('e0', 'e1') default ? %s",
			"e0,e1",
		},

		/*
			// json, https://dev.mysql.com/doc/refman/5.7/en/data-type-defaults.html
			// The BLOB, TEXT, GEOMETRY, and JSON data types cannot be assigned a default value.
			{
				"alter table test.`%s` add column v1 json default ? %s",
				"[99, {\"id\": \"HK500\", \"cost\": 75.99}, [\"hot\", \"cold\"]]",
			},
		*/
	}

	for _, unit := range addColumnUnits {
		for value := 1; value < 10; value++ {
			select {
			case <-ctx.Done():
				return
			default:
			}
			sql := fmt.Sprintf("alter table test.`%s` drop column v1", testName)
			util.MustExec(db, sql)
			time.Sleep(100 * time.Millisecond)

			var notNULL string

			if value%5 == 0 {
				// use default <value> not null
				notNULL = "not null"
			}
			sql = fmt.Sprintf(unit.AddFmt, testName, notNULL)
			util.MustExec(db, sql, unit.DefaultValue)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Test `column not null` + drop column(online DDL/delete-only state), which will trigger GetZeroValue
// NOTICE: when meet `add column xxx not null`, TiDB will add OriginalDefaultValue automatically
// Not null + no default value
func testGetZeroValue(srcs []*sql.DB) {
	type addColumnUnit struct {
		AddFmt       string
		DefaultValue interface{}
	}

	addColumnUnits := []addColumnUnit{
		// date and time data type
		{
			AddFmt:       "alter table test.`%s` add column v1 date not null",
			DefaultValue: "2020-10-10",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 datetime not null",
			DefaultValue: "2020-10-10 10:10:10",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 timestamp not null",
			DefaultValue: "2020-10-10 10:10:10",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 time not null",
			DefaultValue: "10:10:10",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 year not null",
			DefaultValue: "2020",
		},
		{
			// For int year default
			AddFmt:       "alter table test.`%s` add column v1 year not null",
			DefaultValue: 2020,
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 datetime(5) not null",
			DefaultValue: "2020-10-10 10:10:10.9999",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 timestamp(5) not null",
			DefaultValue: "2020-10-10 10:10:10.9999",
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 time(5) not null",
			DefaultValue: "10:10:10.9999",
		},

		// numeric data type
		{
			// default bit[1]
			AddFmt:       "alter table test.`%s` add column v1 bit not null",
			DefaultValue: []byte{0x01},
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 tinyint not null",
			DefaultValue: -13,
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 mediumint not null",
			DefaultValue: -13,
		},
		{
			AddFmt:       "alter table test.`%s` add column v1 int not null",
			DefaultValue: -13,
		},
		{
			"alter table test.`%s` add column v1 bigint not null",
			-13,
		},
		{
			"alter table test.`%s` add column v1 decimal(5) not null",
			-13,
		},
		{
			"alter table test.`%s` add column v1 float not null",
			-13.13,
		},
		{
			"alter table test.`%s` add column v1 double not null",
			-13.13,
		},
		{
			"alter table test.`%s` add column v1 bit(4) not null",
			[]byte{0x03},
		},
		{
			"alter table test.`%s` add column v1 tinyint(4) unsigned not null",
			13,
		},
		{
			"alter table test.`%s` add column v1 mediumint(4) unsigned not null",
			13,
		},
		{
			"alter table test.`%s` add column v1 int(4) unsigned not null",
			13,
		},
		{
			"alter table test.`%s` add column v1 bigint(4) unsigned not null",
			13,
		},
		{
			"alter table test.`%s` add column v1 decimal(5,2) unsigned not null",
			13.13,
		},
		{
			"alter table test.`%s` add column v1 float(5,2) unsigned not null",
			13.13,
		},
		{
			"alter table test.`%s` add column v1 double(5,2) unsigned not null",
			13.13,
		},

		// string data type
		{
			"alter table test.`%s` add column v1 char(10) not null",
			"char",
		},
		{
			"alter table test.`%s` add column v1 varchar(10) not null",
			"varchar",
		},
		{
			"alter table test.`%s` add column v1 binary(10) not null",
			"binary",
		},
		{
			"alter table test.`%s` add column v1 varbinary(10) not null",
			"varbinary",
		},
		{
			"alter table test.`%s` add column v1 blob not null",
			"blob",
		},
		{
			"alter table test.`%s` add column v1 text not null",
			"text",
		},
		{
			"alter table test.`%s` add column v1 enum('e0', 'e1') not null",
			"e1",
		},
		{
			"alter table test.`%s` add column v1 set('e0', 'e1') not null",
			"e0,e1",
		},

		// json data type
		{
			"alter table test.`%s` add column v1 json not null",
			"[99, {\"id\": \"HK500\", \"cost\": 75.99}, [\"hot\", \"cold\"]]",
		},
	}

	testName := getFunctionName(testGetZeroValue)
	mustCreateTable(srcs[0], testName)

	runTime := time.Second * 2
	start := time.Now()
	defer func() {
		log.S().Infof("testGetZeroValue take %v", time.Since(start))
	}()

	log.S().Info("running ddl test: ", testName)
	// drop column at first
	format := fmt.Sprintf("alter table test.`%s` drop column v1", testName)
	util.MustExec(srcs[0], format)
	time.Sleep(100 * time.Millisecond)

	var cf context.CancelFunc
	for i, unit := range addColumnUnits {
		var wg sync.WaitGroup
		ctx, cancel := context.WithTimeout(context.Background(), runTime)
		cf = cancel

		for idx, src := range srcs {
			wg.Add(1)
			go func(i int, s *sql.DB) {
				dml(ctx, s, testName, i, unit.DefaultValue)
				wg.Done()
			}(idx+i*2, src)
		}

		time.Sleep(time.Millisecond)

		ddlExecFunc := func(ctx context.Context, db *sql.DB, format string) {
			for value := 1; value < 10; value++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				// add column
				sql := fmt.Sprintf(format, testName)
				util.MustExec(db, sql)
				time.Sleep(100 * time.Millisecond)
				// drop column
				sql = fmt.Sprintf("alter table test.`%s` drop column v1", testName)
				util.MustExec(db, sql)
				time.Sleep(100 * time.Millisecond)
			}
		}

		wg.Add(1)
		go func() {
			ddlExecFunc(ctx, srcs[0], unit.AddFmt)
			wg.Done()
		}()

		wg.Wait()
	}
	util.MustExec(srcs[0], fmt.Sprintf("create table mark.finish_mark_%d(a int primary key);", finishIdx))
	finishIdx++
	cf()
}

const (
	createDatabaseSQL = "create database if not exists test"
	createTableSQL    = `
create table if not exists test.%s
(
    id1 int unique key not null,
    id2 int unique key not null,
    v0 int default 11,
    v1  int default null
)
`
)

func mustCreateTable(db *sql.DB, tableName string) {
	util.MustExec(db, createDatabaseSQL)
	sql := fmt.Sprintf(createTableSQL, tableName)
	util.MustExec(db, sql)
}
