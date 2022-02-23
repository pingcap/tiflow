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
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// -- Create table
// CREATE TABLE IF NOT EXISTS accounts%d (
// 	id BIGINT PRIMARY KEY,
// 	balance BIGINT NOT NULL,
// 	startts BIGINT NOT NULL
// )
// CREATE TABLE IF NOT EXISTS accounts_seq%d (
// 	id BIGINT PRIMARY KEY,
// 	counter BIGINT NOT NULL,
// 	sequence BIGINT NOT NULL,
// 	startts BIGINT NOT NULL
// )
//
// BEGIN
// -- Add sequential update rows.
// SELECT counter, sequence FROM accounts_seq%d WHERE id = %d FOR UPDATE
// UPDATE accounts_seq%d SET
//   counter = %d,
//   sequence = %d,
//   startts = @@tidb_current_ts
// WHERE id IN (%d, %d)
//
// -- Transaction between accounts.
// SELECT id, balance FROM accounts%d WHERE id IN (%d, %d) FOR UPDATE
// UPDATE accounts%d SET
//   balance = CASE id WHEN %d THEN %d WHEN %d THEN %d END,
//   sequence = %d,
//   startts = @@tidb_current_ts
// WHERE id IN (%d, %d)
// COMMIT
//
// -- Verify sum of balance always be the same.
// SELECT SUM(balance) as total FROM accounts%d
// -- Verify no missing transaction
// SELECT sequence FROM accounts_seq%d ORDER BY sequence

// testcase ...
// testcase.cleanup
// testcase.prepare
// go { loop { testcase.workload } }
// go { loop { testcase.verify } }

const (
	initBalance = 1000
)

type testcase interface {
	prepare(ctx context.Context, db *sql.DB, accounts int, tableID int, concurrency int) error
	workload(ctx context.Context, tx *sql.Tx, accounts int, tableID int) error
	verify(ctx context.Context, db *sql.DB, accounts, tableID int, tag string, endTs string) error
	cleanup(ctx context.Context, db *sql.DB, accounts, tableID int, force bool) bool
}

type sequenceTest struct{}

var _ testcase = &sequenceTest{}

func (*sequenceTest) workload(ctx context.Context, tx *sql.Tx, accounts int, tableID int) error {
	const sequenceRowID = 0

	getCounterSeq := fmt.Sprintf("SELECT counter, sequence FROM accounts_seq%d WHERE id = %d FOR UPDATE", tableID, sequenceRowID)

	var counter, maxSeq int
	row := tx.QueryRowContext(ctx, getCounterSeq)
	if err := row.Scan(&counter, &maxSeq); err != nil {
		return errors.Trace(err)
	}

	next := counter % accounts
	if next == sequenceRowID {
		next++
		counter++
	}
	counter++

	addSeqCounter := fmt.Sprintf(`
	UPDATE accounts_seq%d SET
  		counter = %d,
  		sequence = %d,
  		startts = @@tidb_current_ts
	WHERE id IN (%d, %d)`, tableID, counter, maxSeq+1, sequenceRowID, next)

	if _, err := tx.ExecContext(ctx, addSeqCounter); err != nil {
		log.Error("sequenceTest workload exec failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (s *sequenceTest) prepare(ctx context.Context, db *sql.DB, accounts, tableID, concurrency int) error {
	createTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS accounts_seq%d (
		id BIGINT PRIMARY KEY,
		counter BIGINT NOT NULL,
		sequence BIGINT NOT NULL,
		startts BIGINT NOT NULL
	)`, tableID)
	batchInsertSQLF := func(batchSize, offset int) string {
		args := make([]string, batchSize)
		for j := 0; j < batchSize; j++ {
			args[j] = fmt.Sprintf("(%d, 0, 0, 0)", offset+j)
		}
		return fmt.Sprintf("INSERT IGNORE INTO accounts_seq%d (id, counter, sequence, startts) VALUES %s", tableID, strings.Join(args, ","))
	}

	prepareImpl(ctx, s, createTable, batchInsertSQLF, db, accounts, tableID, concurrency)
	return nil
}

func (*sequenceTest) verify(ctx context.Context, db *sql.DB, accounts, tableID int, tag string, endTs string) error {
	return retry.Do(ctx, func() error {
		query := fmt.Sprintf("set @@tidb_snapshot='%s'", endTs)
		if _, err := db.ExecContext(ctx, query); err != nil {
			log.Error("sequenceTest set tidb_snapshot failed", zap.String("query", query), zap.Error(err))
			return errors.Trace(err)
		}

		query = fmt.Sprintf("SELECT sequence FROM accounts_seq%d ORDER BY sequence", tableID)
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			log.Warn("select sequence err", zap.String("query", query), zap.Error(err), zap.String("tag", tag))
			return nil
		}
		defer rows.Close()

		var curr, previous int
		for rows.Next() {
			if err = rows.Scan(&curr); err != nil {
				log.Warn("select sequence err", zap.String("query", query), zap.Error(err), zap.String("tag", tag))
				return nil
			}

			if previous != 0 && previous != curr && previous+1 != curr {
				return errors.Errorf("missing changes sequence account_seq%d, current sequence=%d, previous sequence=%d", tableID, curr, previous)
			}
			previous = curr
		}

		log.Info("sequence verify pass", zap.String("tag", tag))

		if _, err := db.ExecContext(ctx, "set @@tidb_snapshot=''"); err != nil {
			log.Warn("sequenceTest reset tidb_snapshot failed")
		}

		return nil
	}, retry.WithBackoffMaxDelay(500), retry.WithBackoffMaxDelay(120*1000), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))
}

// tryDropDB will drop table if data incorrect and panic error likes bad connect.
func (s *sequenceTest) cleanup(ctx context.Context, db *sql.DB, accounts, tableID int, force bool) bool {
	return cleanupImpl(ctx, s, fmt.Sprintf("accounts_seq%d", tableID), db, accounts, tableID, force)
}

type bankTest struct{}

var _ testcase = &bankTest{}

func (*bankTest) workload(ctx context.Context, tx *sql.Tx, accounts int, tableID int) error {
	var (
		from, fromBalance int
		to, toBalance     int
	)

	for {
		from, to = rand.Intn(accounts), rand.Intn(accounts)
		if from != to {
			break
		}
	}

	sqlFormat := fmt.Sprintf("SELECT balance FROM accounts%d WHERE id = ? FOR UPDATE", tableID)
	row := tx.QueryRowContext(ctx, sqlFormat, from)
	if err := row.Scan(&fromBalance); err != nil {
		return errors.Trace(err)
	}
	row = tx.QueryRowContext(ctx, sqlFormat, to)
	if err := row.Scan(&toBalance); err != nil {
		return errors.Trace(err)
	}

	amount := rand.Intn(fromBalance/2 + 1)
	fromBalance -= amount
	toBalance += amount

	sqlFormat = fmt.Sprintf("UPDATE accounts%d SET balance = ? WHERE id = ?", tableID)
	if _, err := tx.ExecContext(ctx, sqlFormat, fromBalance, from); err != nil {
		return errors.Trace(err)
	}
	if _, err := tx.ExecContext(ctx, sqlFormat, toBalance, to); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *bankTest) prepare(ctx context.Context, db *sql.DB, accounts, tableID, concurrency int) error {
	createTable := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS accounts%d (
		id BIGINT PRIMARY KEY,
		balance BIGINT NOT NULL,
		startts BIGINT NOT NULL
	)`, tableID)
	batchInsertSQLF := func(batchSize, offset int) string {
		args := make([]string, batchSize)
		for j := 0; j < batchSize; j++ {
			args[j] = fmt.Sprintf("(%d, %d, 0)", offset+j, initBalance)
		}
		return fmt.Sprintf("INSERT IGNORE INTO accounts%d (id, balance, startts) VALUES %s", tableID, strings.Join(args, ","))
	}

	prepareImpl(ctx, s, createTable, batchInsertSQLF, db, accounts, tableID, concurrency)
	return nil
}

func (*bankTest) verify(ctx context.Context, db *sql.DB, accounts, tableID int, tag string, endTs string) error {
	return retry.Do(ctx,
		func() error {
			if _, err := db.ExecContext(ctx, fmt.Sprintf("set @@tidb_snapshot='%s'", endTs)); err != nil {
				log.Error("bank set tidb_snapshot failed", zap.String("endTs", endTs))
				return errors.Trace(err)
			}

			var obtained, expect int

			query := fmt.Sprintf("SELECT SUM(balance) as total FROM accounts%d", tableID)
			if err := db.QueryRowContext(ctx, query).Scan(&obtained); err != nil {
				log.Warn("query failed", zap.String("query", query), zap.Error(err), zap.String("tag", tag))
				return errors.Trace(err)
			}

			expect = accounts * initBalance
			if obtained != expect {
				return errors.Errorf("verify balance failed, accounts%d expect %d, but got %d", tableID, expect, obtained)
			}

			query = fmt.Sprintf("SELECT COUNT(*) as count FROM accounts%d", tableID)
			if err := db.QueryRowContext(ctx, query).Scan(&obtained); err != nil {
				log.Warn("query failed", zap.String("query", query), zap.Error(err), zap.String("tag", tag))
				return errors.Trace(err)
			}
			if obtained != accounts {
				return errors.Errorf("verify count failed, accounts%d expected=%d, obtained=%d", tableID, accounts, obtained)
			}

			log.Info("bank verify pass", zap.String("tag", tag))

			if _, err := db.ExecContext(ctx, "set @@tidb_snapshot=''"); err != nil {
				log.Warn("bank reset tidb_snapshot failed")
			}

			return nil
		}, retry.WithBackoffMaxDelay(500), retry.WithBackoffMaxDelay(120*1000), retry.WithMaxTries(10), retry.WithIsRetryableErr(cerror.IsRetryableError))
}

// tryDropDB will drop table if data incorrect and panic error likes bad connect.
func (s *bankTest) cleanup(ctx context.Context, db *sql.DB, accounts, tableID int, force bool) bool {
	return cleanupImpl(ctx, s, fmt.Sprintf("accounts%d", tableID), db, accounts, tableID, force)
}

func prepareImpl(
	ctx context.Context,
	test testcase, createTable string, batchInsertSQLF func(batchSize, offset int) string,
	db *sql.DB, accounts, tableID, concurrency int,
) {
	isDropped := test.cleanup(ctx, db, accounts, tableID, false)
	if !isDropped {
		return
	}

	mustExec(ctx, db, createTable)

	batchSize := 100
	jobCount := accounts / batchSize
	if accounts%batchSize != 0 {
		jobCount++
	}

	insertF := func(query string) error {
		_, err := db.ExecContext(ctx, query)
		return err
	}

	g := new(errgroup.Group)
	ch := make(chan int, jobCount)
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for {
				startIndex, ok := <-ch
				if !ok {
					return nil
				}

				size := batchSize
				remained := accounts - startIndex + 1
				if remained < size {
					size = remained
				}

				batchInsertSQL := batchInsertSQLF(size, startIndex)
				start := time.Now()
				err := retry.Do(context.Background(), func() error {
					return insertF(batchInsertSQL)
				}, retry.WithBackoffBaseDelay(100), retry.WithBackoffMaxDelay(60*100), retry.WithMaxTries(5))
				if err != nil {
					log.Panic("exec batch insert failed", zap.String("query", batchInsertSQL), zap.Error(err))
				}
				log.Info(fmt.Sprintf("insert %d takes %s", batchSize, time.Since(start)), zap.String("query", batchInsertSQL))
			}
		})
	}

	for i := 0; i < jobCount; i++ {
		ch <- i * batchSize
	}
	close(ch)
	_ = g.Wait()
}

func dropDB(ctx context.Context, db *sql.DB) {
	log.Info("drop database")
	mustExec(ctx, db, "DROP DATABASES IF EXISTS bank")
}

func dropTable(ctx context.Context, db *sql.DB, table string) {
	log.Info("drop tables", zap.String("table", table))
	mustExec(ctx, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
}

func cleanupImpl(ctx context.Context, test testcase, tableName string, db *sql.DB, accounts, tableID int, force bool) bool {
	if force {
		dropTable(ctx, db, tableName)
		return true
	}

	if !isTableExist(ctx, db, tableName) {
		dropTable(ctx, db, tableName)
		return true
	}

	if err := test.verify(ctx, db, accounts, tableID, "tryDropDB", ""); err != nil {
		dropTable(ctx, db, tableName)
		return true
	}

	return false
}

func mustExec(ctx context.Context, db *sql.DB, query string) {
	execF := func() error {
		_, err := db.ExecContext(ctx, query)
		return err
	}

	err := retry.Do(context.Background(), execF, retry.WithBackoffBaseDelay(100), retry.WithBackoffMaxDelay(60*100), retry.WithMaxTries(5))
	if err != nil {
		log.Panic("exec failed", zap.String("query", query), zap.Error(err))
	}
}

func isTableExist(ctx context.Context, db *sql.DB, table string) bool {
	// if table is not exist, return true directly
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", table)
	var t string
	err := db.QueryRowContext(ctx, query).Scan(&t)
	switch {
	case err == sql.ErrNoRows:
		return false
	case err != nil:
		log.Panic("query failed", zap.String("query", query), zap.Error(err))
	}
	return true
}

func openDB(ctx context.Context, dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Panic("open db failed", zap.String("dsn", dsn), zap.Error(err))
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(10 * time.Minute)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		log.Panic("ping db failed", zap.String("dsn", dsn), zap.Error(err))
	}
	log.Info("open db success", zap.String("dsn", dsn))
	return db
}

func run(
	ctx context.Context, upstream, downstream string, accounts, tables, concurrency int,
	interval, testRound int64, cleanupOnly bool,
) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	upstreamDB := openDB(ctx, upstream)
	defer upstreamDB.Close()

	downstreamDB := openDB(ctx, downstream)
	defer downstreamDB.Close()

	tests := []testcase{&sequenceTest{}, &bankTest{}}

	if cleanupOnly {
		for tableID := 0; tableID < tables; tableID++ {
			for i := range tests {
				tests[i].cleanup(ctx, upstreamDB, accounts, tableID, true)
				tests[i].cleanup(ctx, downstreamDB, accounts, tableID, true)
			}
		}

		// a lot of ddl executed at upstream, just drop the db
		dropDB(ctx, upstreamDB)
		dropDB(ctx, downstreamDB)
		log.Info("cleanup done")
		return
	}

	// prepare data for upstream db.
	for _, test := range tests {
		for tableID := 0; tableID < tables; tableID++ {
			if err := test.prepare(ctx, upstreamDB, accounts, tableID, concurrency); err != nil {
				log.Panic("prepare failed", zap.Error(err))
			}
		}
	}

	// DDL is a strong sync point in TiCDC. Once finishmark table is replicated to downstream
	// all previous DDL and DML are replicated too.
	mustExec(ctx, upstreamDB, `CREATE TABLE IF NOT EXISTS finishmark (foo BIGINT PRIMARY KEY)`)
	waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Minute)
	endTs, err := getDownStreamSyncedEndTs(waitCtx, downstreamDB, "finishmark")
	waitCancel()
	if err != nil {
		log.Panic("wait for table finishmark failed", zap.Error(err))
	}
	log.Info("all tables synced", zap.String("endTs", endTs))

	var (
		counts       int64 = 0
		g                  = new(errgroup.Group)
		tblChan            = make(chan string, tables)
		doneCh             = make(chan struct{}, 1)
		valid, tried int64 = 0, 0
	)

	for id := 0; id < tables; id++ {
		tableID := id
		// Workload
		g.Go(func() error {
			workload := func() error {
				tx, err := upstreamDB.Begin()
				if err != nil {
					log.Error("upstream begin tx failed", zap.Error(err))
					return errors.Trace(err)
				}

				for _, test := range tests {
					if err := test.workload(context.Background(), tx, accounts, tableID); err != nil {
						_ = tx.Rollback()
						return errors.Trace(err)
					}
				}

				if err := tx.Commit(); err != nil {
					_ = tx.Rollback()
					log.Error("upstream tx commit failed", zap.Error(err))
					return errors.Trace(err)
				}

				curr := atomic.AddInt64(&counts, 1)
				if curr%interval == 0 {
					tblName := fmt.Sprintf("finishmark%d", curr)
					ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (foo BIGINT PRIMARY KEY)", tblName)
					mustExec(ctx, upstreamDB, ddl)
					tblChan <- tblName

					log.Info("upstream create table executed", zap.String("tblName", tblName))
				}
				return nil
			}

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					err := workload()
					if err != nil && errors.Cause(err) != context.Canceled {
						log.Warn("workload failed", zap.Error(err))
					}

					curr := atomic.LoadInt64(&counts)
					if curr >= testRound {
						log.Info("one upstream workload finished", zap.Int64("round", curr))
						doneCh <- struct{}{}
						return nil
					}
				}
			}
		})

		// Verify
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case tblName := <-tblChan:
					log.Info("downstream start wait for table", zap.String("tblName", tblName))
					waitCtx, waitCancel := context.WithTimeout(ctx, 15*time.Minute)
					endTs, err := getDownStreamSyncedEndTs(waitCtx, downstreamDB, tblName)
					waitCancel()
					log.Info("ddl synced", zap.String("table", tblName))
					if err != nil {
						log.Fatal("[cdc-bank] get ddl end ts error", zap.Error(err))
					}

					atomic.AddInt64(&tried, 1)
					log.Info("downstream sync success", zap.String("endTs", endTs))

					if endTs == "" {
						continue
					}
					atomic.AddInt64(&valid, 1)

					for _, test := range tests {
						verifyCtx, verifyCancel := context.WithTimeout(ctx, 2*time.Minute)
						if err := test.verify(verifyCtx, upstreamDB, accounts, tableID, upstream, ""); err != nil {
							log.Panic("upstream verify failed", zap.Error(err))
						}
						verifyCancel()

						verifyCtx, verifyCancel = context.WithTimeout(ctx, 2*time.Minute)
						if err := test.verify(verifyCtx, downstreamDB, accounts, tableID, downstream, endTs); err != nil {
							log.Panic("downstream verify failed", zap.Error(err))
						}
						verifyCancel()
					}
				case <-doneCh:
					log.Info("one downstream exit due to receive done")
					return nil
				}
			}
		})
	}

	_ = g.Wait()

	if tried == 0 {
		log.Warn("bank test finished, but tries is 0")
	} else {
		log.Info("bank test finished", zap.Int64("valid", valid), zap.Int64("tries", tried), zap.Float64("ratio", float64(valid)/float64(tried)))
	}
}

func getDownStreamSyncedEndTs(ctx context.Context, db *sql.DB, tableName string) (result string, err error) {
	for {
		select {
		case <-ctx.Done():
			log.Error("get downstream sync end ts failed due to timeout", zap.String("table", tableName), zap.Error(ctx.Err()))
			return "", ctx.Err()
		case <-time.After(2 * time.Second):
			result, ok := tryGetEndTs(db, tableName)
			if ok {
				return result, nil
			}
		}
	}
}

func tryGetEndTs(db *sql.DB, tableName string) (result string, ok bool) {
	query := "select END_TIME from information_schema.ddl_jobs where table_name = ?"
	log.Info("try get end ts", zap.String("query", query))
	var endTime string
	row := db.QueryRow(query, tableName)
	if err := row.Scan(&endTime); err != nil {
		if err != sql.ErrNoRows {
			log.Info("rows scan failed", zap.Error(err))
		}
		return "", false
	}

	return endTime, true
}
