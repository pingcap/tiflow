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
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/model/codec"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/reader"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/cdc/redo/writer/file"
	"github.com/pingcap/tiflow/pkg/redo"
)

const (
	hotUpdates   = 2100
	coldUpdates  = 8
	bulkRows     = 1000
	firstBulkID  = coldUpdates + 2
	bulkCommitTs = 5600
	checkpointTs = 1000
	resolvedTs   = 6000
)

// main verifies ordered and atomic transactions through the redo apply command.
func main() {
	downstream := flag.String("downstream", "root@tcp(127.0.0.1:3306)/mysql_txn_order_redo", "downstream DSN")
	sinkURI := flag.String("sink-uri", "mysql://root@127.0.0.1:3306/", "redo apply sink URI")
	workDir := flag.String("work-dir", "", "directory for redo logs and replay output")
	configPath := flag.String("config", "", "redo sink configuration")
	cdcBinary := flag.String("cdc-binary", "cdc.test", "TiCDC binary")
	flag.Parse()
	if *workDir == "" || *configPath == "" {
		panic("--work-dir and --config are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := verifyRedo(ctx, *downstream, *sinkURI, *workDir, *configPath, *cdcBinary); err != nil {
		panic(err)
	}
	fmt.Printf("verified redo order for %d transactions and atomic replay of %d rows\n", hotUpdates+coldUpdates, bulkRows)
}

// generateLogs writes production-format redo files containing an ordered backlog and a large transaction.
func generateLogs(ctx context.Context, dir string, partitioned bool) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf(redo.RedoLogFileFormatV2, "capture", "default", "ordered",
		redo.RedoRowLogFileType, uint64(resolvedTs), uuid.NewString(), redo.LogEXT)
	w, err := file.NewFileWriter(ctx, &writer.LogWriterConfig{Dir: dir, MaxLogSizeInBytes: 64 * 1024 * 1024},
		writer.WithLogFileName(func() string { return name }))
	if err != nil {
		return err
	}
	defer w.Close()
	table := model.BuildTableInfo("mysql_txn_order_redo", "t", []*model.Column{
		{Name: "id", Type: mysql.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag},
		{Name: "v", Type: mysql.TypeLong},
	}, [][]int{{0}})
	table.TableName.IsPartition = partitioned
	writeRow := func(id, before, after int, start, commit uint64, insert bool) error {
		row := &model.RowChangedEvent{StartTs: start, CommitTs: commit, PhysicalTableID: 1, TableInfo: table}
		row.Columns = model.Columns2ColumnDatas([]*model.Column{{Name: "id", Value: id}, {Name: "v", Value: after}}, table)
		if !insert {
			row.PreColumns = model.Columns2ColumnDatas([]*model.Column{{Name: "id", Value: id}, {Name: "v", Value: before}}, table)
		}
		data, err := codec.MarshalRedoLog(row.ToRedoLog(), nil)
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	}
	for v := 1; v <= hotUpdates; v++ {
		ts := uint64(1100 + 2*v)
		if err := writeRow(1, v-1, v, ts-1, ts, false); err != nil {
			return err
		}
	}
	for id := 2; id < firstBulkID; id++ {
		ts := uint64(5400 + 2*(id-2))
		if err := writeRow(id, 0, 1, ts-1, ts, false); err != nil {
			return err
		}
	}
	for id := firstBulkID; id < firstBulkID+bulkRows; id++ {
		if err := writeRow(id, 0, 1, bulkCommitTs-1, bulkCommitTs, true); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	meta := &common.LogMeta{CheckpointTs: checkpointTs, ResolvedTs: resolvedTs}
	data, err := meta.MarshalMsg(nil)
	if err != nil {
		return err
	}
	metaName := fmt.Sprintf(redo.RedoMetaFileFormat, "capture", "default", "ordered",
		redo.RedoMetaFileType, uuid.NewString(), redo.MetaEXT)
	return os.WriteFile(filepath.Join(dir, metaName), data, 0o644)
}

// lastBulkRow finds the final row in the reader's replay order to block its commit.
func lastBulkRow(ctx context.Context, storage url.URL, dir string) (int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rd, err := reader.NewRedoLogReader(ctx, "file", &reader.LogReaderConfig{URI: storage, Dir: dir, UseExternalStorage: true})
	if err != nil {
		return 0, err
	}
	done := make(chan error, 1)
	go func() { done <- rd.Run(ctx) }()
	var last, count int
	for {
		row, err := rd.ReadNextRow(ctx)
		if err != nil {
			return 0, err
		}
		if row == nil {
			break
		}
		if row.CommitTs == bulkCommitTs {
			last, err = strconv.Atoi(fmt.Sprint(row.Columns[0].Value))
			if err != nil {
				return 0, err
			}
			count++
		}
	}
	if err := <-done; err != nil {
		return 0, err
	}
	if count != bulkRows {
		return 0, fmt.Errorf("decoded %d bulk rows, want %d", count, bulkRows)
	}
	return last, nil
}

type replayState struct {
	hot, cold, bulk, rows, total int
}

// readState observes the replay in one downstream snapshot.
func readState(ctx context.Context, db *sql.DB) (s replayState, err error) {
	err = db.QueryRowContext(ctx, `SELECT MAX(IF(id=1,v,0)),SUM(IF(id>1 AND id<?,v,0)),
SUM(IF(id>=? AND v=1,1,0)),COUNT(*),SUM(v) FROM t`, firstBulkID, firstBulkID).
		Scan(&s.hot, &s.cold, &s.bulk, &s.rows, &s.total)
	return
}

// observeLocked checks that neither a later transaction nor a transaction fragment becomes visible.
func observeLocked(ctx context.Context, db *sql.DB, done <-chan struct{}, hotReleased bool) error {
	for deadline := time.Now().Add(5 * time.Second); ; {
		select {
		case <-done:
			return fmt.Errorf("redo apply exited while a required row is locked")
		default:
		}
		s, err := readState(ctx, db)
		if err != nil {
			return err
		}
		if !hotReleased && (s.hot != 0 || s.cold != 0) {
			return fmt.Errorf("redo transaction overtook the locked hot row: %+v", s)
		}
		if s.bulk != 0 {
			return fmt.Errorf("redo committed %d rows of an unfinished %d-row transaction", s.bulk, bulkRows)
		}
		if !time.Now().Before(deadline) {
			return nil
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyCommand runs the real command and preserves integration-test coverage.
func applyCommand(ctx context.Context, binary, configPath, workDir, name string, storage url.URL, sinkURI string) *exec.Cmd {
	var args []string
	if strings.HasSuffix(binary, ".test") {
		profileDir := os.Getenv("OUT_DIR")
		if profileDir == "" {
			profileDir = workDir
		}
		profileName := fmt.Sprintf("cov.mysql_txn_order.redo.%s.%d.out", name, os.Getpid())
		args = append(args, "-test.coverprofile="+filepath.Join(profileDir, profileName))
	}
	args = append(args, "redo", "apply", "--enable-profiling=false", "--config="+configPath,
		"--storage="+storage.String(), "--tmp-dir="+filepath.Join(workDir, name), "--sink-uri="+sinkURI)
	return exec.CommandContext(ctx, binary, args...)
}

// verifyRedo checks ordering, transaction atomicity, final rows, and unsupported partitions.
func verifyRedo(ctx context.Context, downstream, sinkURI, workDir, configPath, binary string) error {
	workDir, err := filepath.Abs(workDir)
	if err != nil {
		return err
	}
	storage := url.URL{Scheme: "file", Path: filepath.Join(workDir, "logs")}
	if err := generateLogs(ctx, storage.Path, false); err != nil {
		return err
	}
	last, err := lastBulkRow(ctx, storage, filepath.Join(workDir, "inspect"))
	if err != nil {
		return err
	}
	db, err := sql.Open("mysql", downstream)
	if err != nil {
		return err
	}
	defer db.Close()
	for id := 3; id < firstBulkID; id++ {
		if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES (?,0)", id); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES (?,0)", last); err != nil {
		return err
	}
	hotLock, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer hotLock.Rollback()
	var id int
	if err := hotLock.QueryRowContext(ctx, "SELECT id FROM t WHERE id=1 FOR UPDATE").Scan(&id); err != nil {
		return err
	}
	bulkLock, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer bulkLock.Rollback()
	if err := bulkLock.QueryRowContext(ctx, "SELECT id FROM t WHERE id=? FOR UPDATE", last).Scan(&id); err != nil {
		return err
	}
	out, err := os.Create(filepath.Join(workDir, "apply.log"))
	if err != nil {
		return err
	}
	defer out.Close()
	cmd := applyCommand(ctx, binary, configPath, workDir, "apply", storage, sinkURI)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	var applyErr error
	go func() { applyErr = cmd.Wait(); close(done) }()
	defer func() { _ = cmd.Process.Kill(); <-done }()
	if err := observeLocked(ctx, db, done, false); err != nil {
		return err
	}
	if err := hotLock.Commit(); err != nil {
		return err
	}
	for {
		select {
		case <-done:
			return fmt.Errorf("redo apply exited before the bulk lock was released: %v", applyErr)
		default:
		}
		s, err := readState(ctx, db)
		if err != nil {
			return err
		}
		if s.bulk != 0 {
			return fmt.Errorf("redo committed a partial bulk transaction: %+v", s)
		}
		if s.cold == coldUpdates {
			if s.hot != hotUpdates {
				return fmt.Errorf("redo transaction overtook earlier hot transactions: %+v", s)
			}
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := observeLocked(ctx, db, done, true); err != nil {
		return err
	}
	if err := bulkLock.Commit(); err != nil {
		return err
	}
	select {
	case <-done:
		if applyErr != nil {
			return fmt.Errorf("redo apply failed: %w", applyErr)
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	s, err := readState(ctx, db)
	if err != nil {
		return err
	}
	if s.hot != hotUpdates || s.cold != coldUpdates || s.bulk != bulkRows ||
		s.rows != firstBulkID+bulkRows-1 || s.total != hotUpdates+coldUpdates+bulkRows {
		return fmt.Errorf("incorrect redo final state: %+v", s)
	}
	storage.Path = filepath.Join(workDir, "partitions")
	if err := generateLogs(ctx, storage.Path, true); err != nil {
		return err
	}
	output, err := applyCommand(ctx, binary, configPath, workDir, "partition-apply", storage, sinkURI).CombinedOutput()
	if writeErr := os.WriteFile(filepath.Join(workDir, "partition-apply.log"), output, 0o644); writeErr != nil {
		return writeErr
	}
	if err == nil || !strings.Contains(string(output), "txn-order-tables does not support partitioned table") {
		return fmt.Errorf("redo partition validation: err=%v, output=%s", err, output)
	}
	return nil
}
