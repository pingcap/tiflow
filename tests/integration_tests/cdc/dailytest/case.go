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
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

var casePKAddDuplicateUK = []string{
	`
CREATE TABLE binlog_pk_add_duplicate_uk(id INT PRIMARY KEY, a1 INT);
`,
	`
INSERT INTO binlog_pk_add_duplicate_uk(id, a1) VALUES(1,1),(2,1);
`,
	`
ALTER TABLE binlog_pk_add_duplicate_uk ADD UNIQUE INDEX aidx(a1);
`,
}

var casePKAddDuplicateUKClean = []string{
	`DROP TABLE binlog_pk_add_duplicate_uk`,
}

var (
	caseAlterDatabase = []string{
		`CREATE DATABASE to_be_altered CHARACTER SET utf8;`,
		`ALTER DATABASE to_be_altered CHARACTER SET utf8mb4;`,
	}
	caseAlterDatabaseClean = []string{
		`DROP DATABASE to_be_altered;`,
	}
)

type testRunner struct {
	src    *sql.DB
	dst    *sql.DB
	schema string
}

func (tr *testRunner) run(test func(*sql.DB)) {
	RunTest(tr.src, tr.dst, tr.schema, test)
}

func (tr *testRunner) execSQLs(sqls []string) {
	RunTest(tr.src, tr.dst, tr.schema, func(src *sql.DB) {
		err := execSQLs(tr.src, sqls)
		if err != nil {
			log.S().Fatal(err)
		}
	})
}

// RunCase run some simple test case
func RunCase(src *sql.DB, dst *sql.DB, schema string) {
	tr := &testRunner{src: src, dst: dst, schema: schema}
	ineligibleTable(tr, src, dst)

	tr.run(caseUpdateWhileAddingCol)
	tr.execSQLs([]string{"DROP TABLE growing_cols;"})

	tr.execSQLs(caseAlterDatabase)
	tr.execSQLs(caseAlterDatabaseClean)

	// run casePKAddDuplicateUK
	tr.run(func(src *sql.DB) {
		err := execSQLs(src, casePKAddDuplicateUK)
		// the add unique index will failed by duplicate entry
		if err != nil && !strings.Contains(err.Error(), "Duplicate") {
			log.S().Fatal(err)
		}
	})
	tr.execSQLs(casePKAddDuplicateUKClean)

	tr.run(caseUpdateWhileDroppingCol)
	tr.execSQLs([]string{"DROP TABLE many_cols;"})

	tr.run(caseTblWithGeneratedCol)
	tr.execSQLs([]string{"DROP TABLE gen_contacts;"})
	tr.run(caseCreateView)
	tr.execSQLs([]string{"DROP TABLE base_for_view;"})
	tr.execSQLs([]string{"DROP VIEW view_user_sum;"})

	// random op on have both pk and uk table
	var start time.Time
	tr.run(func(src *sql.DB) {
		start = time.Now()

		err := updatePKUK(src, 1000)
		if err != nil {
			log.S().Fatal(errors.ErrorStack(err))
		}
	})

	tr.execSQLs([]string{"DROP TABLE pkuk"})
	log.S().Info("sync updatePKUK take: ", time.Since(start))

	// swap unique index value
	tr.run(func(src *sql.DB) {
		mustExec(src, "create table uindex(id int primary key, a1 int unique)")

		mustExec(src, "insert into uindex(id, a1) values(1, 10), (2, 20)")

		tx, err := src.Begin()
		if err != nil {
			log.S().Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 30 where id = 1")
		if err != nil {
			log.S().Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 10 where id = 2")
		if err != nil {
			log.S().Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 20 where id = 1")
		if err != nil {
			log.S().Fatal(err)
		}

		err = tx.Commit()
		if err != nil {
			log.S().Fatal(err)
		}
	})
	tr.run(func(src *sql.DB) {
		mustExec(src, "drop table uindex")
	})

	// test big cdc msg
	tr.run(func(src *sql.DB) {
		mustExec(src, "create table binlog_big(id int primary key, data longtext);")

		tx, err := src.Begin()
		if err != nil {
			log.S().Fatal(err)
		}
		// insert 5 * 1M
		// note limitation of TiDB: https://github.com/pingcap/docs/blob/733a5b0284e70c5b4d22b93a818210a3f6fbb5a0/FAQ.md#the-error-message-transaction-too-large-is-displayed
		data := make([]byte, 1<<20)
		for i := 0; i < 5; i++ {
			_, err = tx.Query("INSERT INTO binlog_big(id, data) VALUES(?, ?);", i, data)
			if err != nil {
				log.S().Fatal(err)
			}
		}
		err = tx.Commit()
		if err != nil {
			log.S().Fatal(err)
		}
	})
	tr.execSQLs([]string{"DROP TABLE binlog_big;"})
}

func ineligibleTable(tr *testRunner, src *sql.DB, dst *sql.DB) {
	sqls := []string{
		"CREATE TABLE ineligible_table1 (uk int UNIQUE null, ncol int);",
		"CREATE TABLE ineligible_table2 (ncol1 int, ncol2 int);",

		"insert into ineligible_table1 (uk, ncol) values (1,1);",
		"insert into ineligible_table2 (ncol1, ncol2) values (2,2);",
		"ALTER TABLE ineligible_table1 ADD COLUMN c1 INT NOT NULL;",
		"ALTER TABLE ineligible_table2 ADD COLUMN c1 INT NOT NULL;",
		"insert into ineligible_table1 (uk, ncol, c1) values (null,2,3);",
		"insert into ineligible_table2 (ncol1, ncol2, c1) values (1,1,3);",

		"CREATE TABLE eligible_table (uk int UNIQUE not null, ncol int);",
		"insert into eligible_table (uk, ncol) values (1,1);",
		"insert into eligible_table (uk, ncol) values (2,2);",
		"ALTER TABLE eligible_table ADD COLUMN c1 INT NOT NULL;",
		"insert into eligible_table (uk, ncol, c1) values (3,4,5);",
	}
	// execute SQL but don't check
	for _, sql := range sqls {
		mustExec(src, sql)
	}

	synced := false
TestLoop:
	for {
		rows, err := dst.Query("show tables")
		if err != nil {
			log.S().Fatalf("exec failed, sql: 'show tables', err: %+v", err)
		}
		for rows.Next() {
			var tableName string
			err := rows.Scan(&tableName)
			if err != nil {
				log.S().Fatalf("scan result set failed, err: %+v", err)
			}
			if tableName == "ineligible_table1" || tableName == "ineligible_table2" {
				log.S().Fatalf("found unexpected table %s", tableName)
			}
			if synced {
				break TestLoop
			}
			if tableName == "eligible_table" {
				synced = true
			}
		}
	}

	// clean up
	sqls = []string{
		"DROP TABLE ineligible_table1;",
		"DROP TABLE ineligible_table2;",
		"DROP TABLE eligible_table;",
	}
	tr.execSQLs(sqls)
}

func caseUpdateWhileAddingCol(db *sql.DB) {
	mustExec(db, `
CREATE TABLE growing_cols (
	id INT AUTO_INCREMENT PRIMARY KEY,
	val INT DEFAULT 0
);`)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		insertSQL := `INSERT INTO growing_cols(id, val) VALUES (?, ?);`
		mustExec(db, insertSQL, 1, 0)

		// Keep updating to generate DMLs while the other goroutine's adding columns
		updateSQL := `UPDATE growing_cols SET val = ? WHERE id = ?;`
		for i := 0; i < 256; i++ {
			mustExec(db, updateSQL, i, 1)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 32; i++ {
			updateSQL := fmt.Sprintf(`ALTER TABLE growing_cols ADD COLUMN col%d VARCHAR(50);`, i)
			mustExec(db, updateSQL)
		}
	}()

	wg.Wait()
}

func caseUpdateWhileDroppingCol(db *sql.DB) {
	const nCols = 10
	var builder strings.Builder
	for i := 0; i < nCols; i++ {
		if i != 0 {
			builder.WriteRune(',')
		}
		builder.WriteString(fmt.Sprintf("col%d VARCHAR(50) NOT NULL", i))
	}
	createSQL := fmt.Sprintf(`
CREATE TABLE many_cols (
	id INT AUTO_INCREMENT PRIMARY KEY,
	val INT DEFAULT 0,
	%s
);`, builder.String())
	mustExec(db, createSQL)

	builder.Reset()
	for i := 0; i < nCols; i++ {
		if i != 0 {
			builder.WriteRune(',')
		}
		builder.WriteString(fmt.Sprintf("col%d", i))
	}
	cols := builder.String()

	builder.Reset()
	for i := 0; i < nCols; i++ {
		if i != 0 {
			builder.WriteRune(',')
		}
		builder.WriteString(`""`)
	}
	placeholders := builder.String()

	// Insert a row with all columns set to empty string
	insertSQL := fmt.Sprintf(`INSERT INTO many_cols(id, %s) VALUES (?, %s);`, cols, placeholders)
	mustExec(db, insertSQL, 1)

	closeCh := make(chan struct{})
	go func() {
		// Keep updating to generate DMLs while the other goroutine's dropping columns
		updateSQL := `UPDATE many_cols SET val = ? WHERE id = ?;`
		for i := 0; ; i++ {
			mustExec(db, updateSQL, i, 1)
			select {
			case <-closeCh:
				return
			default:
			}
		}
	}()

	for i := 0; i < nCols; i++ {
		mustExec(db, fmt.Sprintf("ALTER TABLE many_cols DROP COLUMN col%d;", i))
	}
	close(closeCh)
}

// caseTblWithGeneratedCol creates a table with generated column,
// and insert values into the table
func caseTblWithGeneratedCol(db *sql.DB) {
	mustExec(db, `
CREATE TABLE gen_contacts (
	id INT AUTO_INCREMENT PRIMARY KEY,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
	other VARCHAR(101),
	fullname VARCHAR(101) GENERATED ALWAYS AS (CONCAT(first_name,' ',last_name)),
	initial VARCHAR(101) GENERATED ALWAYS AS (CONCAT(LEFT(first_name, 1),' ',LEFT(last_name,1))) STORED
);`)

	insertSQL := "INSERT INTO gen_contacts(first_name, last_name) VALUES(?, ?);"
	updateSQL := "UPDATE gen_contacts SET other = fullname WHERE first_name = ?"
	for i := 0; i < 64; i++ {
		mustExec(db, insertSQL, fmt.Sprintf("John%d", i), fmt.Sprintf("Dow%d", i))

		idxToUpdate := rand.Intn(i + 1)
		mustExec(db, updateSQL, fmt.Sprintf("John%d", idxToUpdate))
	}
	delSQL := "DELETE FROM gen_contacts WHERE fullname = ?"
	for i := 0; i < 10; i++ {
		mustExec(db, delSQL, fmt.Sprintf("John%d Dow%d", i, i))
	}
}

func caseCreateView(db *sql.DB) {
	mustExec(db, `
CREATE TABLE base_for_view (
	id INT AUTO_INCREMENT PRIMARY KEY,
	user_id INT NOT NULL,
	amount INT NOT NULL
);`)

	mustExec(db, `
CREATE VIEW view_user_sum (user_id, total)
AS SELECT user_id, SUM(amount) FROM base_for_view GROUP BY user_id;`)

	insertSQL := "INSERT INTO base_for_view(user_id, amount) VALUES(?, ?);"
	updateSQL := "UPDATE base_for_view SET amount = ? WHERE user_id = ?;"
	deleteSQL := "DELETE FROM base_for_view WHERE user_id = ? AND amount = ?;"
	for i := 0; i < 42; i++ {
		for j := 0; j < 3; j++ {
			mustExec(db, insertSQL, i, j*10+i)
			if i%2 == 0 && j == 1 {
				mustExec(db, updateSQL, 1111, i)
			}
		}
	}
	for i := 0; i < 10; i++ {
		mustExec(db, deleteSQL, i, 1111)
	}
}

// updatePKUK create a table with primary key and unique key
// then do opNum randomly DML
func updatePKUK(db *sql.DB, opNum int) error {
	maxKey := 20
	mustExec(db, "create table pkuk(pk int primary key, uk int, v int, unique key uk(uk));")

	pks := make(map[int]struct{})
	freePks := rand.Perm(maxKey)

	nextPk := func() int {
		rand.Shuffle(len(freePks), func(i, j int) {
			freePks[i], freePks[j] = freePks[j], freePks[i]
		})
		return freePks[0]
	}
	addPK := func(pk int) {
		pks[pk] = struct{}{}
		var i, v int
		for i, v = range freePks {
			if v == pk {
				break
			}
		}
		freePks = append(freePks[:i], freePks[i+1:]...)
	}
	removePK := func(pk int) {
		delete(pks, pk)
		freePks = append(freePks, pk)
	}
	genOldPk := func() int {
		n := rand.Intn(len(pks))
		var i, pk int
		for pk = range pks {
			if i == n {
				break
			}
			i++
		}
		return pk
	}

	for i := 0; i < opNum; {
		var (
			sql       string
			pk, oldPK int
		)

		// try randomly insert&update&delete
		op := rand.Intn(3)
		switch op {
		case 0:
			if len(pks) == maxKey {
				continue
			}
			pk = nextPk()
			uk := rand.Intn(maxKey)
			v := rand.Intn(10000)
			sql = fmt.Sprintf("insert into pkuk(pk, uk, v) values(%d,%d,%d)", pk, uk, v)
		case 1:
			if len(pks) == 0 || len(pks) == maxKey {
				continue
			}
			pk = nextPk()
			oldPK = genOldPk()
			uk := rand.Intn(maxKey)
			v := rand.Intn(10000)
			sql = fmt.Sprintf("update pkuk set pk = %d, uk = %d, v = %d where pk = %d", pk, uk, v, oldPK)
		case 2:
			if len(pks) == 0 {
				continue
			}
			oldPK = genOldPk()
			sql = fmt.Sprintf("delete from pkuk where pk = %d", oldPK)
		}

		_, err := db.Exec(sql)
		if err != nil {
			// for insert and update, we didn't check for uk's duplicate
			if strings.Contains(err.Error(), "Duplicate entry") {
				continue
			}
			return errors.Trace(err)
		}

		switch op {
		case 0:
			addPK(pk)
		case 1:
			removePK(oldPK)
			addPK(pk)
		case 2:
			removePK(oldPK)
		}
		i++
	}
	return nil
}

func mustExec(db *sql.DB, sql string, args ...interface{}) {
	_, err := db.Exec(sql, args...)
	if err != nil {
		log.S().Fatalf("exec failed, sql: %s args: %v, err: %+v", sql, args, err)
	}
}
