// Copyright 2025 PingCAP, Inc.
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
	"log"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	upHost := GetEnvDefault("UP_TIDB_HOST", "127.0.0.1")
	upPort := GetEnvDefault("UP_TIDB_PORT", "4000")
	downHost := GetEnvDefault("UP_TIDB_HOST", "127.0.0.1")
	downPort := GetEnvDefault("UP_TIDB_PORT", "3306")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		run(upHost, upPort)
	}()
	go func() {
		defer wg.Done()
		run(downHost, downPort)
	}()
	wg.Wait()
}

func run(host string, port string) {
	dsn := fmt.Sprintf("root@tcp(%s:%s)/", host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("open db failed:", dsn, ", err: ", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal("ping db failed:", dsn, ", err: ", err)
	}
	log.Println("connect to tidb success, dsn: ", dsn)

	createTable := `create table if not exists test.t (
		id int primary key,
		col decimal(65,30)
	);`
	insertDML := `insert into test.t values (?, ?)`

	concurrency := 200
	maxRowCnt := 1000000
	num := maxRowCnt / concurrency
	db.SetMaxOpenConns(concurrency)

	_, err = db.Exec(createTable)
	if err != nil {
		log.Fatal("create table failed:, err: ", err)
	}

	var wg sync.WaitGroup
	for k := 0; k < concurrency; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for i := 0; i < num; i++ {
				val := k*num + i
				_, err = db.Exec(insertDML, val, val)
				if err != nil {
					log.Fatal("insert value failed:, err: ", err)
				}
			}
		}(k)
	}
	wg.Wait()
}

func GetEnvDefault(key, defaultV string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		return defaultV
	}
	return val
}
