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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/chroma/quick"
	"github.com/fatih/color"
	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var (
	nFailed = 0
	nPassed = 0
)

func parseSQLText(data string) (res []ast.StmtNode, warns []error, err error) {
	p := parser.New()
	statements, warns, err := p.Parse(data, "utf8mb4", "utf8mb4_bin")
	return statements, warns, err
}

func readAndParseSQLText(sqlFilePath string) []ast.StmtNode {
	data, err := os.ReadFile(sqlFilePath)
	if err != nil {
		logger.Panic("Failed to read test case file",
			zap.String("case", sqlFilePath),
			zap.Error(err))
	}
	statements, warns, err := parseSQLText(string(data))
	if warns != nil {
		logger.Warn("Meet warnings when parsing SQL",
			zap.String("case", sqlFilePath),
			zap.Any("warnings", warns))
	}
	if err != nil {
		logger.Panic("Failed to parse SQL",
			zap.String("case", sqlFilePath),
			zap.Error(err))
	}
	return statements
}

func runAllTestCases(dir string) bool {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".sql") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		logger.Panic("Failed to read test case directory", zap.String("dir", dir), zap.Error(err))
	}

	for _, path := range files {
		logger.Info("Run", zap.String("case", path))
		runTestCase(path)
	}

	if nFailed > 0 {
		logger.Error(
			"Test finished with error",
			zap.Int("passed", nPassed),
			zap.Int("failed", nFailed))
	} else {
		logger.Info(
			"All tests pass",
			zap.Int("passed", nPassed),
			zap.Int("failed", nFailed))
	}

	return nFailed == 0
}

func resetDB(db *DBHelper) {
	db.MustExec("drop database if exists `" + *dbName + "`;")
	db.MustExec("create database `" + *dbName + "`;")
	db.MustExec("use `" + *dbName + "`;")
}

func runTestCase(testCasePath string) bool {
	resetDB(dbMySQL)
	resetDB(dbTiDB)

	statementKindsToWaitCDCRecord := map[string]bool{
		"Delete":         true,
		"Insert":         true,
		"Replace":        true,
		"Update":         true,
		"CreateDatabase": true,
		"DropDatabase":   true,
		"CreateTable":    true,
		"AlterTable":     true,
		"DropTable":      true,
	}

	hasError := false
	stmtAsts := readAndParseSQLText(testCasePath)
	for _, stmt := range stmtAsts {
		query := strings.TrimSpace(stmt.Text())

		waitCDCRows := false
		statementKind := ast.GetStmtLabel(stmt)
		if v, ok := statementKindsToWaitCDCRecord[statementKind]; v && ok {
			waitCDCRows = true
		}
		fmt.Printf("case: %s exec sql: %s kind: %s\n", testCasePath, query, statementKind)
		if runSingleQuery(query, waitCDCRows) {
			nPassed++
		} else {
			nFailed++
			hasError = true
		}
	}

	return hasError
}

func fetchNextCDCRecord(reader *kafka.Reader, kind Kind, timeout time.Duration) (map[string]any, error) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		m, err := reader.FetchMessage(ctx)

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, nil
			}
			return nil, fmt.Errorf("Failed to read CDC record of %s: %w", kind, err)
		}

		if err = reader.CommitMessages(context.Background(), m); err != nil {
			return nil, fmt.Errorf("Failed to commit CDC record of %s: %w", kind, err)
		}
		cancel()

		if len(m.Value) == 0 {
			continue
		}

		var obj map[string]any
		err = json.Unmarshal(m.Value, &obj)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse CDC record of %s (msg=%s): %w", kind, m.Value, err)
		}

		payload, ok := obj["payload"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Unexpected CDC record of %s: payload field not exist in %s", kind, m.Value)
		}
		if kind == KindTiDB {
			_, ok1 := payload["op"]
			_, ok2 := payload["ddl"]
			// Ignore Checkpoint events in the TiCDC's output
			if !ok1 && !ok2 {
				continue
			}
			// Only handle DDL received from partition-0 should be enough.
			if ok2 && m.Partition != 0 {
				continue
			}
		}
		if ddl, ok := payload["ddl"]; ok {
			payload["ddl"] = normalizeSQL(ddl.(string))
		}

		return obj, nil
	}
}

func fetchAllCDCRecords(reader *kafka.Reader, kind Kind) []map[string]any {
	var records []map[string]any
	for {
		waitTimeout := time.Millisecond * 1000
		if len(records) == 0 {
			// Wait a bit longer for the first record
			if kind == KindMySQL {
				waitTimeout = 10 * time.Second
			} else if kind == KindTiDB {
				waitTimeout = 20 * time.Second
			}
		}

		obj, err := fetchNextCDCRecord(reader, kind, waitTimeout)
		if err != nil {
			logger.Error(
				"Received error when fetching CDC record",
				zap.Error(err),
				zap.String("kind", string(kind)))
			break
		}
		if obj == nil {
			// No more records
			break
		}

		records = append(records, obj)
	}

	return records
}

var ignoredRecordPaths = map[string]bool{
	`{map[string]any}["schema"]`:                             true,
	`{map[string]any}["payload"].(map[string]any)["source"]`: true,
	`{map[string]any}["payload"].(map[string]any)["ts_ms"]`:  true,
	// `{map[string]any}["payload"].(map[string]any)["tableChanges"]`: true,
}

var headingColor = color.New(color.FgHiWhite, color.Bold)

func printObj(obj any) {
	v, _ := json.MarshalIndent(obj, "", "  ")
	quick.Highlight(os.Stdout, string(v), "json", "terminal16m", "vs")
	fmt.Println()
}

func normalizeSQL(sql string) string {
	p := parser.New()
	stmt, err := p.ParseOneStmt(sql, "", "")
	buf := new(bytes.Buffer)
	if err != nil {
		panic(fmt.Sprintf("parse sql failed %s", err))
	}
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
	stmt.Restore(restoreCtx)
	return buf.String()
}

func runSingleQuery(query string, waitCDCRows bool) bool {
	{
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			dbMySQL.MustExec(query)
			wg.Done()
		}()
		go func() {
			dbTiDB.MustExec(query)
			wg.Done()
		}()
		wg.Wait()
	}

	if !waitCDCRows {
		return true
	}

	testCasePassed := true
	onError := func(err error) {
		fmt.Println("==========================================")
		logger.Error("Test failed", zap.Error(err))
		headingColor.Print("\nSQL:\n\n")
		quick.Highlight(os.Stdout, query, "sql", "terminal16m", "vs")
		fmt.Println()
		testCasePassed = false
	}

	var objsDebezium []map[string]any
	var objsTiCDC []map[string]any
	{
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			objsDebezium = fetchAllCDCRecords(readerDebezium, KindMySQL)
			fmt.Println("reade Debezium done", time.Now())
			wg.Done()
		}()
		go func() {
			objsTiCDC = fetchAllCDCRecords(readerTiCDC, KindTiDB)
			fmt.Println("reade TiCDC done", time.Now())
			wg.Done()
		}()
		wg.Wait()
	}

	if len(objsDebezium) != len(objsTiCDC) {
		onError(fmt.Errorf(
			"Mismatch CDC rows: Got %d rows from Debezium and %d rows from TiCDC",
			len(objsDebezium),
			len(objsTiCDC)))
		headingColor.Print("\nDebezium output:\n\n")
		for _, obj := range objsDebezium {
			printObj(obj)
		}
		headingColor.Print("\nTiCDC output:\n\n")
		for _, obj := range objsTiCDC {
			printObj(obj)
		}
		return testCasePassed
	}

	cmpOption := cmp.FilterPath(
		func(p cmp.Path) bool {
			path := p.GoString()
			_, shouldIgnore := ignoredRecordPaths[path]
			return shouldIgnore
		},
		cmp.Ignore(),
	)

	for i := 0; i < len(objsDebezium); i++ {
		objDebezium := objsDebezium[i]
		objTiCDC := objsTiCDC[i]
		if diff := cmp.Diff(objDebezium, objTiCDC, cmpOption); diff != "" {
			onError(fmt.Errorf("Found mismatch CDC record (output row #%d)", i+1))
			headingColor.Print("\nCDC Result Diff (-debezium +ticdc):\n\n")
			quick.Highlight(os.Stdout, diff, "diff", "terminal16m", "murphy")
			fmt.Println()
			continue
		}
	}

	return testCasePassed
}
