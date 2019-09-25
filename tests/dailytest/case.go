// Copyright 2019 PingCAP, Inc.
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
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// test different data type of mysql
// mysql will change boolean to tinybit(1)
var caseMultiDataType = []string{`
CREATE TABLE binlog_multi_data_type (
	id INT AUTO_INCREMENT,
	t_boolean BOOLEAN,
	t_bigint BIGINT,
	t_double DOUBLE,
	t_decimal DECIMAL(38,19),
	t_bit BIT(64),
	t_date DATE,
	t_datetime DATETIME,
	t_timestamp TIMESTAMP NULL,
	t_time TIME,
	t_year YEAR,
	t_char CHAR,
	t_varchar VARCHAR(10),
	t_blob BLOB,
	t_text TEXT,
	t_enum ENUM('enum1', 'enum2', 'enum3'),
	t_set SET('a', 'b', 'c'),
	t_json JSON,
	PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
`,
	`
INSERT INTO binlog_multi_data_type(t_boolean, t_bigint, t_double, t_decimal, t_bit
	,t_date, t_datetime, t_timestamp, t_time, t_year
	,t_char, t_varchar, t_blob, t_text, t_enum
	,t_set, t_json) VALUES
	(true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
	,'1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
	,'测', '测试', 'blob', '测试text', 'enum2'
	,'a,b', NULL);
`,
	`
INSERT INTO binlog_multi_data_type(t_boolean) VALUES(TRUE);
`,
	`
INSERT INTO binlog_multi_data_type(t_boolean) VALUES(FALSE);
`,
	// minmum value of bigint
	`
INSERT INTO binlog_multi_data_type(t_bigint) VALUES(-9223372036854775808);
`,
	// maximum value of bigint
	`
INSERT INTO binlog_multi_data_type(t_bigint) VALUES(9223372036854775807);
`,
	`
INSERT INTO binlog_multi_data_type(t_json) VALUES('{"key1": "value1", "key2": "value2"}');
`,
}

var caseMultiDataTypeClean = []string{`
	DROP TABLE binlog_multi_data_type`,
}

// https://internal.pingcap.net/jira/browse/TOOL-714
var caseUKWithNoPK = []string{`
CREATE TABLE binlog_uk_with_no_pk (id INT, a1 INT, a3 INT, UNIQUE KEY dex1(a1, a3));
`,
	`
INSERT INTO binlog_uk_with_no_pk(id, a1, a3) VALUES(1, 1, NULL);
`,
	`
INSERT INTO binlog_uk_with_no_pk(id, a1, a3) VALUES(2, 1, NULL);
`,
	`
UPDATE binlog_uk_with_no_pk SET id = 10 WHERE id = 1;
`,
	`
UPDATE binlog_uk_with_no_pk SET id = 100 WHERE id = 10;
`,
}

var caseUKWithNoPKClean = []string{`
	DROP TABLE binlog_uk_with_no_pk`,
}

var casePKAddDuplicateUK = []string{`
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

// Test issue: TOOL-1346
var caseInsertBit = []string{`
CREATE TABLE binlog_insert_bit(a BIT(1) NOT NULL);
`,
	`
INSERT INTO binlog_insert_bit VALUES (0x01);
`,
	`
UPDATE binlog_insert_bit SET a = 0x00;
`,
}

var caseInsertBitClean = []string{`
	DROP TABLE binlog_insert_bit;
`,
}

// Test issue: TOOL-1407
var caseRecoverAndInsert = []string{`
CREATE TABLE binlog_recover_and_insert(id INT, a INT);
`,
	`
INSERT INTO binlog_recover_and_insert(id, a) VALUES(1, -1);
`,
	`
UPDATE binlog_recover_and_insert SET a = -5 WHERE id = 1;
`,
	`
DROP TABLE binlog_recover_and_insert;
`,
	`
RECOVER TABLE binlog_recover_and_insert;
`,
	// make sure we can insert data after recovery
	`
INSERT INTO binlog_recover_and_insert(id, a) VALUES(2, -3);
`,
}

var caseRecoverAndInsertClean = []string{`
	DROP TABLE binlog_recover_and_insert;
`,
}

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

	runPKcases(tr)

	tr.execSQLs(caseMultiDataType)
	tr.execSQLs(caseMultiDataTypeClean)

	tr.execSQLs(caseUKWithNoPK)
	tr.execSQLs(caseUKWithNoPKClean)

	// run casePKAddDuplicateUK
	tr.run(func(src *sql.DB) {
		err := execSQLs(src, casePKAddDuplicateUK)
		if err != nil && !strings.Contains(err.Error(), "Duplicate for key") {
			log.S().Fatal(err)
		}
	})
	tr.execSQLs(casePKAddDuplicateUKClean)

	// run caseInsertBit
	tr.execSQLs(caseInsertBit)
	tr.execSQLs(caseInsertBitClean)

	// run caseRecoverAndInsert
	tr.execSQLs(caseRecoverAndInsert)
	tr.execSQLs(caseRecoverAndInsertClean)

	tr.run(caseTblWithGeneratedCol)
	tr.execSQLs([]string{"DROP TABLE gen_contacts;"})

	tr.run(caseCreateView)
	tr.execSQLs([]string{"DROP TABLE base_for_view;"})
	tr.execSQLs([]string{"DROP VIEW view_user_sum;"})

	// random op on have both pk and uk table
	tr.run(func(src *sql.DB) {
		start := time.Now()

		err := updatePKUK(src, 1000)
		if err != nil {
			log.S().Fatal(errors.ErrorStack(err))
		}

		log.S().Info(" updatePKUK take: ", time.Since(start))
	})

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
		var data = make([]byte, 1<<20)
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

	_, err := db.Exec("DROP TABLE pkuk")
	return errors.Trace(err)
}

// create a table with one column id with different type
// test the case whether it is primary key too, this can
// also help test when the column is handle or not.
func runPKcases(tr *testRunner) {
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
			Update: int64(math.MaxInt64) - 1,
		},
		{
			Tp:     "INT UNSIGNED",
			Value:  uint32(math.MaxUint32),
			Update: uint32(math.MaxUint32) - 1,
		},
		{
			Tp:     "INT SIGNED",
			Value:  int32(math.MaxInt32),
			Update: int32(math.MaxInt32) - 1,
		},
		{
			Tp:     "SMALLINT UNSIGNED",
			Value:  uint16(math.MaxUint16),
			Update: uint16(math.MaxUint16) - 1,
		},
		{
			Tp:     "SMALLINT SIGNED",
			Value:  int16(math.MaxInt16),
			Update: int16(math.MaxInt16) - 1,
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

	for _, c := range cases {
		for _, ispk := range []string{"", "PRIMARY KEY"} {

			tr.run(func(src *sql.DB) {
				sql := fmt.Sprintf("CREATE TABLE pk(id %s %s)", c.Tp, ispk)
				mustExec(src, sql)

				sql = "INSERT INTO pk(id) values( ? )"
				mustExec(src, sql, c.Value)

				if len(ispk) == 0 {
					// insert a null value
					mustExec(src, sql, nil)
				}

				sql = "UPDATE pk set id = ? where id = ?"
				mustExec(src, sql, c.Update, c.Value)

				sql = "DELETE from pk where id = ?"
				mustExec(src, sql, c.Update)
			})

			tr.execSQLs([]string{"DROP TABLE pk"})
		}
	}
}

func mustExec(db *sql.DB, sql string, args ...interface{}) {
	_, err := db.Exec(sql, args...)
	if err != nil {
		log.S().Fatalf("exec failed, sql: %s args: %v, err: %+v", sql, args, err)
	}
}
