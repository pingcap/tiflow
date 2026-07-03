// Copyright 2024 PingCAP, Inc.
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
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	upHost := GetEnvDefault("UP_TIDB_HOST", "127.0.0.1")
	upPort := GetEnvDefault("UP_TIDB_PORT", "4000")
	dsn := fmt.Sprintf("root@tcp(%s:%s)/", upHost, upPort)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("open db failed:", dsn, ", err: ", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal("ping db failed:", dsn, ", err: ", err)
	}
	log.Println("connect to tidb success, dsn: ", dsn)

	createTable := `create table if not exists ddl_with_exists.t%d (
		id int primary key auto_increment,
		name varchar(255)
	);`
	addColumn := "alter table ddl_with_exists.t%d add column if not exists age int;"
	dropColumn := "alter table ddl_with_exists.t%d drop column if exists age;"
	addIndex := "alter table ddl_with_exists.t%d add index if not exists idx1(id);"
	dropIndex := "alter table ddl_with_exists.t%d drop index if exists idx1;"

	concurrency := 16
	maxTableCnt := 20
	db.SetMaxOpenConns(concurrency)

	start := time.Now()
	for i := 0; i < maxTableCnt; i++ {
		_, err := db.Exec(fmt.Sprintf(createTable, i))
		if err != nil {
			log.Fatal("create table failed:", i, ", err: ", err)
		}
	}
	log.Println("create table cost:", time.Since(start).Seconds(), "s")

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Println("worker start:", i)
			for j := 0; j < 20; j++ {
				idx := rand.Intn(maxTableCnt)
				ddl := fmt.Sprintf(createTable, idx)
				switch rand.Intn(5) {
				case 0:
					ddl = fmt.Sprintf(addColumn, idx)
				case 1:
					ddl = fmt.Sprintf(dropColumn, idx)
				case 2:
					ddl = fmt.Sprintf(addIndex, idx)
				case 3:
					ddl = fmt.Sprintf(dropIndex, idx)
				default:
				}
				_, err := db.Exec(ddl)
				if err != nil {
					log.Println(err)
				}
			}
			log.Println("worker exit:", i)
		}()
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
