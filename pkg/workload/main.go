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
	"database/sql"
	"flag"
	"fmt"
	"golang.org/x/time/rate"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/workload/schema"
)

var (
	tableCount      int
	tableStartIndex int
	tps             int
	rowCount        int64
	thread          int

	dbHost     string
	dbPort     int
	dbUser     string
	dbPassword string
	dbName     string

	total      uint64
	totalError uint64

	workloadType  string
	errorLogLimit = rate.NewLimiter(rate.Every(5*time.Second), 1)
)

const (
	bank     = "bank"
	sysbench = "sysbench"
	express  = "express"
)

func init() {
	flag.StringVar(&workloadType, "workload-type", "sysbench", "sysbench,bank,express")
	flag.IntVar(&tableCount, "table-count", 1, "table count of the workload")
	flag.IntVar(&tableStartIndex, "table-start-index", 0, "table start index, sbtest<index>")
	flag.IntVar(&tps, "tps", 1000, "tps of the workload")
	flag.IntVar(&thread, "thread", 0, "thread of the workload")
	flag.Int64Var(&rowCount, "row-count", 1000000, "row count of the workload")

	flag.StringVar(&dbHost, "database-host", "127.0.0.1", "database host")
	flag.StringVar(&dbUser, "database-user", "root", "database user")
	flag.StringVar(&dbPassword, "database-password", "", "database password")
	flag.StringVar(&dbName, "database-db-name", "test", "database db name")
	flag.IntVar(&dbPort, "database-port", 4000, "database port")
	flag.Parse()
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	threadCount := getThreadCount()
	fmt.Printf("use thread %d\n", threadCount)
	wg := &sync.WaitGroup{}
	wg.Add(threadCount)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", dbUser, dbPassword, dbHost, dbPort, dbName))
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(threadCount)
	db.SetMaxIdleConns(threadCount)

	if err = initTables(db); err != nil {
		panic(err)
	}

	rowPerThread := rowCount / int64(threadCount)
	tpsPerThread := int(math.Round(float64(tps) / float64(threadCount)))
	for i := 0; i < threadCount; i++ {
		go func() {
			defer wg.Done()
			worker(db, rowPerThread, tpsPerThread)
		}()
	}

	go printTPS()
	wg.Wait()
}

func getThreadCount() int {
	cpuNum := runtime.NumCPU()
	if thread > 0 {
		cpuNum = thread
	}
	if tps < cpuNum {
		cpuNum = tps
	}
	return cpuNum
}

// initTables create tables if not exists
func initTables(db *sql.DB) error {
	var createTable func(int) string
	switch workloadType {
	case bank:
		createTable = schema.GetBankCreateTableStatement
	case express:
		createTable = schema.GetExpressCreateTableStatement
	case sysbench:
		createTable = schema.GetSysbenchCreateTableStatement
	default:
		panic(fmt.Sprintf("unknown workload type %s", workloadType))
	}

	for i := 0; i < tableCount; i++ {
		tableN := i + tableStartIndex
		fmt.Printf("try to create table %d\n", tableN)
		ddl := createTable(tableN)
		if _, err := db.Exec(ddl); err != nil {
			return errors.Annotate(err,
				fmt.Sprintf("create table %d failed, ddl: %s", tableN, ddl))
		}
	}
	return nil
}

func worker(db *sql.DB, totalRows int64, tps int) {
	tick := time.Tick(time.Second)
	var prepareDML func(int, int) string
	switch workloadType {
	case bank:
		prepareDML = schema.BuildBankInsertSql
	case express:
		prepareDML = schema.BuildExpressReplaceSql
	case sysbench:
		prepareDML = schema.BuildSysbenchInsertSql
	default:
		panic(fmt.Sprintf("unknown workload type %s", workloadType))
	}

	for {
		select {
		case <-tick:
			tablesMap := make(map[int]int) // tableIndex -> rowsCount
			rows := int(math.Round(float64(tps)/float64(tableCount))) + 1
			// Random select and count rows for each table.
			for i := 0; i < tps; i += rows {
				n := rand.Int63()
				tableIndex := int(n)%tableCount + tableStartIndex
				tablesMap[tableIndex] += rows
			}

			for tableIndex, rowsCount := range tablesMap {
				dml := prepareDML(tableIndex, rowsCount)
				_, err := db.Exec(dml)
				if err != nil {
					if errorLogLimit.Allow() {
						fmt.Println(err)
					}
					atomic.AddUint64(&totalError, 1)
				}
				atomic.AddUint64(&total, uint64(rowsCount))
				totalRows -= int64(rowsCount)
				if totalRows <= 0 {
					return
				}
			}
		}
	}
}

func printTPS() {
	t := time.Tick(time.Second * 5)
	old := uint64(0)
	oldErr := uint64(0)
	for {
		select {
		case <-t:
			temp := atomic.LoadUint64(&total)
			qps := (float64(temp) - float64(old)) / 5.0
			old = temp
			temp = atomic.LoadUint64(&totalError)
			fmt.Printf("total %d, total err %d, qps is %f, err qps %f\n", total, totalError, qps, (float64(temp)-float64(oldErr))/5.0)
			oldErr = temp
		}
	}
}
