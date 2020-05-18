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
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/log"
)

func mustCreateTable(db *sql.DB) {
	conn, err := db.Conn(context.Background())
	if err != nil {
		log.S().Fatal(err)
	}
	mustCreateTableWithConn(conn)
}

func mustCreateTableWithConn(conn *sql.Conn) {
	var err error
	_, err = conn.ExecContext(context.Background(), "create database if not exists test")
	if err != nil {
		log.S().Fatal(err)
	}
	_, err = conn.ExecContext(context.Background(), "create table if not exists test.test1(id int primary key, v1 int default null)")
	if err != nil {
		log.S().Fatal(err)
	}
}

func createDropSchemaDDL(ctx context.Context, db *sql.DB) {
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
		_, err := conn.ExecContext(context.Background(), "use test")
		if err != nil {
			log.S().Fatal(err)
		}
		conn.Close()
	}()

	for {
		mustCreateTableWithConn(conn)
		select {
		case <-ctx.Done():
			return
		default:
		}

		time.Sleep(time.Millisecond)

		_, err = conn.ExecContext(context.Background(), "drop database test")
		if err != nil {
			log.S().Fatal(err)
		}

	}
}

func truncateDDL(ctx context.Context, db *sql.DB) {
	var err error
	mustCreateTable(db)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, err = db.Exec("truncate table test.test1")
		if err != nil {
			log.S().Fatal(err)
		}

		time.Sleep(time.Millisecond)
	}
}

func dml(ctx context.Context, db *sql.DB, id int) {
	var err error
	var i int
	var success int

	for i = 0; ; i++ {
		_, err = db.Exec("insert into test.test1(id) values(?)", i+id*100000000)
		if err == nil {
			success++
			if success%100 == 0 {
				log.S().Info(id, " success: ", success)
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
	var err error
	mustCreateTable(db)

	for value := 1; ; value++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, err = db.Exec("alter table test.test1 drop column v1")
		if err != nil {
			log.S().Fatal(err)
		}
		time.Sleep(time.Millisecond)

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

		_, err = db.Exec(fmt.Sprintf("alter table test.test1 add column v1 int default ? %s", notNULL), defaultValue)
		if err != nil {
			log.S().Fatal(err)
		}
		time.Sleep(time.Millisecond)

	}
}

func modifyColumnDDL(ctx context.Context, db *sql.DB) {
	var err error

	mustCreateTable(db)

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

		_, err = db.Exec("alter table test.test1 modify column v1 int default ?", defaultValue)
		if err != nil {
			log.S().Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

// for every DDL, run the DDL continuously, and one goroutine for one TiDB instance to do some DML op
func runDDLTest(srcs []*sql.DB, targetDB *sql.DB, schema string) {
	runTime := time.Second * 2
	start := time.Now()
	defer func() {
		log.S().Infof("runDDLTest take %v", time.Since(start))
	}()

	for _, ddlFunc := range []func(context.Context, *sql.DB){createDropSchemaDDL, truncateDDL, addDropColumnDDL, modifyColumnDDL} {
		RunTest(srcs[0], targetDB, schema, func(_ *sql.DB) {
			log.S().Info("running ddl test: ", getFunctionName(ddlFunc))

			var wg sync.WaitGroup
			ctx, cancel := context.WithTimeout(context.Background(), runTime)
			defer cancel()

			for idx, src := range srcs {
				wg.Add(1)
				go func(i int, s *sql.DB) {
					dml(ctx, s, i)
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
		})

		// just cleanup
		RunTest(srcs[0], targetDB, schema, func(db *sql.DB) {
			_, err := db.Exec("drop table if exists test1")
			if err != nil {
				log.S().Fatal(err)
			}
		})
	}
}
