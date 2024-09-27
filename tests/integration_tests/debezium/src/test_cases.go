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
var (
	msgKey   = "key"
	msgValue = "value"
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
		"Delete":  true,
		"Insert":  true,
		"Replace": true,
		"Update":  true,
		// "CreateDatabase": true,
		// "DropDatabase":   true,
		// "CreateTable": true,
		// "AlterTable":  true,
		// "DropTable":   true,
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
		if runSingleQuery(query, waitCDCRows) {
			nPassed++
		} else {
			nFailed++
			hasError = true
		}
	}

	return hasError
}

func fetchNextCDCRecord(reader *kafka.Reader, kind Kind, timeout time.Duration) (map[string]any, map[string]any, error) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		m, err := reader.FetchMessage(ctx)

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, nil, nil
			}
			return nil, nil, fmt.Errorf("Failed to read CDC record of %s: %w", kind, err)
		}

		if err = reader.CommitMessages(context.Background(), m); err != nil {
			return nil, nil, fmt.Errorf("Failed to commit CDC record of %s: %w", kind, err)
		}
		cancel()

		if len(m.Value) == 0 {
			continue
		}

		var keyMap map[string]any
		var obj map[string]any
		err = json.Unmarshal(m.Key, &keyMap)
		if err != nil {
			return nil, nil, fmt.Errorf("Failed to parse CDC record of %s (msg=%s): %w", kind, m.Key, err)
		}
		err = json.Unmarshal(m.Value, &obj)
		if err != nil {
			return nil, nil, fmt.Errorf("Failed to parse CDC record of %s (msg=%s): %w", kind, m.Value, err)
		}

		payload, ok := obj["payload"].(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("Unexpected CDC record of %s: payload field not exist in %s", kind, m.Value)
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

		// HACK
		if kind == KindMySQL {
			if tableChanges, ok := payload["tableChanges"]; ok {
				if tables, ok := tableChanges.([]any); ok && len(tables) > 0 {
					if table0, ok := tables[0].(map[string]any); ok {
						if table, ok := table0["table"]; ok && table != nil {
							if columns, ok := table.(map[string]any); ok {
								for _, col := range columns["columns"].([]any) {
									col := col.(map[string]any)
									switch col["typeName"].(string) {
									case "INT":
										if col["length"] == 11 {
											col["length"] = nil
										}
									case "INTEGER":
										if col["length"] == 11 {
											col["length"] = nil
										}
										col["typeName"] = replaceString(col["typeName"], "INTEGER", "INT")
										col["typeExpression"] = replaceString(col["typeExpression"], "INTEGER", "INT")
									case "NUMERIC":
										col["typeName"] = replaceString(col["typeName"], "NUMERIC", "DECIMAL")
										col["typeExpression"] = replaceString(col["typeExpression"], "NUMERIC", "DECIMAL")
										col["jdbcType"] = float64(3)
									case "NVARCHAR":
										col["typeName"] = replaceString(col["typeName"], "NVARCHAR", "VARCHAR")
										col["typeExpression"] = replaceString(col["typeExpression"], "NVARCHAR", "VARCHAR")
										col["jdbcType"] = float64(12)
									case "NCHAR":
										col["typeName"] = replaceString(col["typeName"], "NCHAR", "CHAR")
										col["typeExpression"] = replaceString(col["typeExpression"], "NCHAR", "CHAR")
										col["jdbcType"] = float64(1)
									case "REAL":
										col["typeName"] = replaceString(col["typeName"], "REAL", "DOUBLE")
										col["typeExpression"] = replaceString(col["typeExpression"], "REAL", "DOUBLE")
										col["jdbcType"] = float64(7)
									}
								}
							}
						}
					}
				}
			}
		}
		return keyMap, obj, nil
	}
}

func replaceString(s any, key any, val any) string {
	return strings.Replace(s.(string), key.(string), val.(string), 1)
}

func fetchAllCDCRecords(reader *kafka.Reader, kind Kind) ([]map[string]any, []map[string]any) {
	var records []map[string]any
	var keyMaps []map[string]any
	waitTimeout := 10 * time.Second
	for {
		keyMap, obj, err := fetchNextCDCRecord(reader, kind, waitTimeout)
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
		keyMaps = append(keyMaps, keyMap)
		waitTimeout = time.Millisecond * 1000
	}

	return keyMaps, records
}

var ignoredRecordPaths = map[string]bool{
	// `{map[string]any}["schema"]`:                             true,
	`{map[string]any}["payload"].(map[string]any)["source"]`: true,
	`{map[string]any}["payload"].(map[string]any)["ts_ms"]`:  true,
}

var headingColor = color.New(color.FgHiWhite, color.Bold)

func printRecord(obj any) {
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

	var keyMapsDebezium []map[string]any
	var objsDebezium []map[string]any
	var keyMapsTiCDC []map[string]any
	var objsTiCDC []map[string]any
	{
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			keyMapsDebezium, objsDebezium = fetchAllCDCRecords(readerDebezium, KindMySQL)
			wg.Done()
		}()
		go func() {
			keyMapsTiCDC, objsTiCDC = fetchAllCDCRecords(readerTiCDC, KindTiDB)
			wg.Done()
		}()
		wg.Wait()
	}

	diff(keyMapsDebezium, keyMapsTiCDC, onError, msgKey)
	diff(objsDebezium, objsTiCDC, onError, msgValue)

	return testCasePassed
}

func diff(recordsDebezium, recordsTiCDC []map[string]any, onError func(error), msgType string) {
	if len(recordsDebezium) != len(recordsTiCDC) {
		onError(fmt.Errorf(
			"Mismatch CDC %s: Got %d record from Debezium and %d record from TiCDC",
			msgType,
			len(recordsDebezium),
			len(recordsTiCDC)))

		headingColor.Print("\nDebezium output:\n\n")
		for _, record := range recordsDebezium {
			printRecord(record)
		}
		headingColor.Print("\nTiCDC output:\n\n")
		for _, record := range recordsTiCDC {
			printRecord(record)
		}
		return
	}
	cmpOption := cmp.FilterPath(
		func(p cmp.Path) bool {
			path := p.GoString()
			_, shouldIgnore := ignoredRecordPaths[path]
			return shouldIgnore
		},
		cmp.Ignore(),
	)

	for i := 0; i < len(recordsDebezium); i++ {
		recordDebezium := recordsDebezium[i]
		recordTiCDC := recordsTiCDC[i]
		if diff := cmp.Diff(recordDebezium, recordTiCDC, cmpOption); diff != "" {
			onError(fmt.Errorf("Found mismatch CDC record (output record #%d)", i+1))
			headingColor.Print("\nCDC Result Diff (-debezium +ticdc):\n\n")
			quick.Highlight(os.Stdout, diff, "diff", "terminal16m", "murphy")
			fmt.Println()
			continue
		}
	}
}
