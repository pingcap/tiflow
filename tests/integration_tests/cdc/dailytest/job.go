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

package dailytest

import (
	"database/sql"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

func addJobs(jobCount int, jobChan chan struct{}) {
	for i := 0; i < jobCount; i++ {
		jobChan <- struct{}{}
	}

	close(jobChan)
}

func doSqls(table *table, db *sql.DB, count int) {
	var sqls []string
	var args [][]interface{}
	var err error

	sql, arg, err := genDeleteSqls(table, db, count/10)
	if err != nil {
		log.S().Error(errors.ErrorStack(err))
	} else {
		sqls = append(sqls, sql...)
		args = append(args, arg...)
	}

	sql, arg, err = genInsertSqls(table, count)
	if err != nil {
		log.S().Error(errors.ErrorStack(err))
	} else {
		sqls = append(sqls, sql...)
		args = append(args, arg...)
	}

	sql, arg, err = genUpdateSqls(table, db, count/10)
	if err != nil {
		log.S().Error(errors.ErrorStack(err))
	} else {
		sqls = append(sqls, sql...)
		args = append(args, arg...)
	}

	execSqls(db, sqls, args)
}

func execSqls(db *sql.DB, sqls []string, args [][]interface{}) {
	txn, err := db.Begin()
	if err != nil {
		log.S().Fatalf(errors.ErrorStack(err))
	}

	for i := range sqls {
		_, err = txn.Exec(sqls[i], args[i]...)
		if err != nil {
			log.S().Error(errors.ErrorStack(err))
		}
	}

	err = txn.Commit()
	if err != nil {
		log.S().Warn(errors.ErrorStack(err))
	}
}

func doJob(table *table, db *sql.DB, batch int, jobChan chan struct{}, doneChan chan struct{}) {
	count := 0
	for range jobChan {
		count++
		if count == batch {
			doSqls(table, db, count)
			count = 0
		}
	}

	if count > 0 {
		doSqls(table, db, count)
	}

	doneChan <- struct{}{}
}

func doWait(doneChan chan struct{}, workerCount int) {
	for i := 0; i < workerCount; i++ {
		<-doneChan
	}

	close(doneChan)
}

func doDMLProcess(table *table, db *sql.DB, jobCount int, workerCount int, batch int) {
	jobChan := make(chan struct{}, 16*workerCount)
	doneChan := make(chan struct{}, workerCount)

	go addJobs(jobCount, jobChan)

	for i := 0; i < workerCount; i++ {
		go doJob(table, db, batch, jobChan, doneChan)
	}

	doWait(doneChan, workerCount)
}

func doDDLProcess(table *table, db *sql.DB) {
	// do drop column ddl
	index := randInt(2, len(table.columns)-1)
	col := table.columns[index]

	_, ok1 := table.indices[col.name]
	_, ok2 := table.uniqIndices[col.name]
	if !ok1 && !ok2 {
		newCols := make([]*column, 0, len(table.columns)-1)
		newCols = append(newCols, table.columns[:index]...)
		newCols = append(newCols, table.columns[index+1:]...)
		table.columns = newCols
		sql := fmt.Sprintf("alter table %s drop column %s", table.name, col.name)
		execSqls(db, []string{sql}, [][]interface{}{{}})
	}

	// do add  column ddl
	index = randInt(2, len(table.columns)-1)
	colName := randString(5)
	tp := types.NewFieldType(mysql.TypeVarchar)
	tp.SetFlen(45)
	col = &column{name: colName, tp: tp}

	newCols := make([]*column, 0, len(table.columns)+1)
	newCols = append(newCols, table.columns[:index]...)
	newCols = append(newCols, col)
	newCols = append(newCols, table.columns[index:]...)

	table.columns = newCols
	sql := fmt.Sprintf("alter table %s add column `%s` varchar(45) after %s", table.name, col.name, table.columns[index-1].name)
	execSqls(db, []string{sql}, [][]interface{}{{}})
}

func doProcess(table *table, db *sql.DB, jobCount int, workerCount int, batch int) {
	if len(table.columns) <= 2 {
		log.S().Fatal("column count must > 2, and the first and second column are for primary key")
	}

	doDMLProcess(table, db, jobCount/2, workerCount, batch)
	doDDLProcess(table, db)
	doDMLProcess(table, db, jobCount/2, workerCount, batch)
}
