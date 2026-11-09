// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const hotUpdates = 2100

// main parses connection options and reports transaction order verification.
func main() {
	upstream := flag.String("upstream", "root@tcp(127.0.0.1:4000)/mysql_txn_order", "upstream DSN")
	downstream := flag.String("downstream", "root@tcp(127.0.0.1:3306)/mysql_txn_order", "downstream DSN")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := run(ctx, *upstream, *downstream); err != nil {
		panic(err)
	}
	fmt.Printf("verified %d transactions in upstream commit order\n", hotUpdates+1)
}

// run checks downstream commit order while concurrent table transactions accumulate.
func run(ctx context.Context, upDSN, downDSN string) error {
	up, err := sql.Open("mysql", upDSN)
	if err != nil {
		return err
	}
	defer up.Close()
	down, err := sql.Open("mysql", downDSN)
	if err != nil {
		return err
	}
	defer down.Close()

	// Wait for the table to start replicating before generating updates. This
	// also keeps the workload past the initial replay/safe-mode interval.
	if _, err := up.ExecContext(ctx, "INSERT INTO t VALUES (3,0)"); err != nil {
		return err
	}
	for _, want := range []int{1, 0} {
		for {
			var count int
			if err := down.QueryRowContext(ctx, "SELECT COUNT(*) FROM t WHERE id=3").Scan(&count); err != nil {
				return err
			}
			if count == want {
				break
			}
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if want == 1 {
			if _, err := up.ExecContext(ctx, "DELETE FROM t WHERE id=3"); err != nil {
				return err
			}
		}
	}

	var tidbVersion string
	isTiDB := down.QueryRowContext(ctx, "SELECT tidb_version()").Scan(&tidbVersion) == nil
	if !isTiDB {
		for _, query := range []string{
			"CREATE TABLE audit (seq BIGINT AUTO_INCREMENT PRIMARY KEY, row_id INT, v INT)",
			"CREATE TRIGGER audit_update AFTER UPDATE ON t FOR EACH ROW INSERT INTO audit(row_id,v) VALUES (NEW.id,NEW.v)",
		} {
			if _, err := down.ExecContext(ctx, query); err != nil {
				return err
			}
		}
	}

	// Hold the first hot row while enough separate autocommit transactions arrive
	// to saturate the conflict detector's 1024-entry worker cache.
	lock, err := down.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer lock.Rollback()
	var id int
	if err := lock.QueryRowContext(ctx, "SELECT id FROM t WHERE id=1 FOR UPDATE").Scan(&id); err != nil {
		return err
	}
	for value := 1; value <= hotUpdates; value++ {
		if _, err := up.ExecContext(ctx, "UPDATE t SET v=? WHERE id=1", value); err != nil {
			return err
		}
	}
	if _, err := up.ExecContext(ctx, "UPDATE t SET v=1 WHERE id=2"); err != nil {
		return err
	}
	// Allow the final independent transaction to reach the blocked sink.
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := lock.Commit(); err != nil {
		return err
	}

	if isTiDB {
		// TiDB has no triggers. Check that the independent update never becomes
		// visible before all earlier hot-row updates.
		for {
			var hot, cold int
			err := down.QueryRowContext(ctx, "SELECT MAX(IF(id=1,v,0)),MAX(IF(id=2,v,0)) FROM t").Scan(&hot, &cold)
			if err != nil {
				return err
			}
			if cold == 1 {
				if hot != hotUpdates {
					return fmt.Errorf("independent transaction overtook hot row at v=%d", hot)
				}
				return nil
			}
			select {
			case <-time.After(time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	for {
		var count int
		if err := down.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil {
			return err
		}
		if count > hotUpdates+1 {
			return fmt.Errorf("duplicate updates: %d", count)
		}
		if count == hotUpdates+1 {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	rows, err := down.QueryContext(ctx, "SELECT row_id,v FROM audit ORDER BY seq")
	if err != nil {
		return err
	}
	defer rows.Close()
	seq := 1
	for rows.Next() {
		var id, value int
		if err := rows.Scan(&id, &value); err != nil {
			return err
		}
		wantID, wantValue := 1, seq
		if seq == hotUpdates+1 {
			wantID, wantValue = 2, 1
		}
		if id != wantID || value != wantValue {
			return fmt.Errorf("transaction %d: got (%d,%d), want (%d,%d)", seq, id, value, wantID, wantValue)
		}
		seq++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seq != hotUpdates+2 {
		return fmt.Errorf("got %d audited transactions, want %d", seq-1, hotUpdates+1)
	}
	return nil
}
