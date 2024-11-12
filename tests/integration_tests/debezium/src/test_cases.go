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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

var timeOut = time.Second * 10

var (
	nFailed = 0
	nPassed = 0
)

var (
	msgKey   = "key"
	msgValue = "value"
)

var defaultLength = map[string]float64{
	"INTEGER":          11,
	"INTEGER UNSIGNED": 10,
	"INT":              11,
	"INT UNSIGNED":     10,
	"TINYINT":          4,
	"TINYINT UNSIGNED": 3,
	"BIGINT":           20,
	"BIT":              1,
	"CHAR":             1,
}

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

func runAllTestCases(dir, dbConnMySQL, dbConnTiDB string) bool {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		if strings.HasSuffix(info.Name(), ".sql") || strings.HasSuffix(info.Name(), ".ddl") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		logger.Panic("Failed to read test case directory", zap.String("dir", dir), zap.Error(err))
	}
	for _, path := range files {
		logger.Info("Run", zap.String("case", path))
		runTestCase(path, dbConnMySQL, dbConnTiDB)
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

func resetDB() {
	runSingleQuery("drop database if exists `"+*dbName+"`;", false)
	runSingleQuery("create database `"+*dbName+"`;", false)
	runSingleQuery("use `"+*dbName+"`;", false)
}

func runTestCase(testCasePath, dbConnMySQL, dbConnTiDB string) {
	dbMySQL = prepareDBConn(KindMySQL, dbConnMySQL)
	defer dbMySQL.MustClose()
	dbTiDB = prepareDBConn(KindTiDB, dbConnTiDB)
	defer dbTiDB.MustClose()

	resetDB()

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
		}
	}
}

func fetchNextCDCRecord(reader *kafka.Reader, kind Kind, timeout time.Duration) (map[string]any, map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, nil, nil
			}
			return nil, nil, fmt.Errorf("Failed to read CDC record of %s: %w", kind, err)
		}

		if err = reader.CommitMessages(ctx, m); err != nil {
			return nil, nil, fmt.Errorf("Failed to commit CDC record of %s: %w", kind, err)
		}

		if len(m.Value) == 0 {
			continue
		}

		var keyMap map[string]any
		var obj map[string]any
		err = json.Unmarshal(m.Key, &keyMap)
		if err != nil {
			return nil, nil, fmt.Errorf("Failed to parse CDC record of %s (msg key=%s): %w", kind, m.Key, err)
		}
		err = json.Unmarshal(m.Value, &obj)
		if err != nil {
			return nil, nil, fmt.Errorf("Failed to parse CDC record of %s (msg value=%s): %w", kind, m.Value, err)
		}

		payload, ok := obj["payload"].(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("Unexpected CDC record of %s: payload field not exist in %s", kind, m.Value)
		}
		if kind == KindTiDB {
			op, ok := payload["op"]
			// Ignore Checkpoint events in the TiCDC's output
			if ok && op == "m" {
				continue
			}
			// Only handle DDL received from partition-0 should be enough.
			if !ok && m.Partition != 0 {
				continue
			}
		}
		if ddl, ok := payload["ddl"]; ok {
			payload["ddl"] = normalizeSQL(ddl.(string))
		}

		// HACK
		// In DDL events, some data types can't decode correctly, but the query is exact.
		if kind == KindMySQL {
			if tableChanges, ok := payload["tableChanges"]; ok {
				if tables, ok := tableChanges.([]any); ok && len(tables) > 0 {
					if table0, ok := tables[0].(map[string]any); ok {
						if table, ok := table0["table"]; ok && table != nil {
							if columns, ok := table.(map[string]any); ok {
								for _, col := range columns["columns"].([]any) {
									col := col.(map[string]any)
									v := col["typeName"].(string)
									switch v {
									case "INT", "INT UNSIGNED", "INTEGER", "INTEGER UNSIGNED", "TINYINT", "TINYINT UNSIGNED", "BIGINT",
										"BIT", "CHAR":
										if col["length"] == defaultLength[v] {
											col["length"] = nil
										}
									}
									switch v {
									case "INTEGER", "INTEGER UNSIGNED":
										col["typeName"] = replaceString(col["typeName"], "INTEGER", "INT")
										col["typeExpression"] = replaceString(col["typeExpression"], "INTEGER", "INT")
									case "NUMERIC":
										col["typeName"] = replaceString(col["typeName"], "NUMERIC", "DECIMAL")
										col["typeExpression"] = replaceString(col["typeExpression"], "NUMERIC", "DECIMAL")
										col["jdbcType"] = float64(3)
									case "REAL":
										col["typeName"] = replaceString(col["typeName"], "REAL", "FLOAT")
										col["typeExpression"] = replaceString(col["typeExpression"], "REAL", "FLOAT")
										col["jdbcType"] = float64(6)
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

func replaceString(s any, old any, new any) string {
	return strings.Replace(s.(string), old.(string), new.(string), 1)
}

var ignoredRecordPaths = map[string]bool{
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
	p.SetSQLMode(mysql.ModeRealAsFloat) // necessary
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
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			if _, _, err := fetchNextCDCRecord(readerDebezium, KindMySQL, timeOut); err != nil {
				logger.Error("fetch record failed", zap.Error(err))
			}
			wg.Done()
		}()
		go func() {
			if _, _, err := fetchNextCDCRecord(readerTiCDC, KindTiDB, timeOut); err != nil {
				logger.Error("fetch record failed", zap.Error(err))
			}
			wg.Done()
		}()
		wg.Wait()
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

	var keyMapsDebezium map[string]any
	var objsDebezium map[string]any
	var keyMapsTiCDC map[string]any
	var objsTiCDC map[string]any
	{
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go func() {
			var err error
			keyMapsDebezium, objsDebezium, err = fetchNextCDCRecord(readerDebezium, KindMySQL, timeOut)
			if err != nil {
				logger.Error("fetch record failed", zap.Error(err))
			}
			wg.Done()
		}()
		go func() {
			var err error
			keyMapsTiCDC, objsTiCDC, err = fetchNextCDCRecord(readerTiCDC, KindTiDB, timeOut)
			if err != nil {
				logger.Error("fetch record failed", zap.Error(err))
			}
			wg.Done()
		}()
		wg.Wait()
	}
	cmpOption := cmp.FilterPath(
		func(p cmp.Path) bool {
			path := p.GoString()
			_, shouldIgnore := ignoredRecordPaths[path]
			return shouldIgnore
		},
		cmp.Ignore(),
	)

	if diff := cmp.Diff(keyMapsDebezium, keyMapsTiCDC, cmpOption); diff != "" {
		onError(fmt.Errorf("Found mismatch CDC record (msg type %s)", msgKey))
		headingColor.Print("\nCDC Result Diff (-debezium +ticdc):\n\n")
		quick.Highlight(os.Stdout, diff, "diff", "terminal16m", "murphy")
		fmt.Println()
	}
	if diff := cmp.Diff(objsDebezium, objsTiCDC, cmpOption); diff != "" {
		onError(fmt.Errorf("Found mismatch CDC record (msg type %s)", msgValue))
		headingColor.Print("\nCDC Result Diff (-debezium +ticdc):\n\n")
		quick.Highlight(os.Stdout, diff, "diff", "terminal16m", "murphy")
		fmt.Println()
	}
	return testCasePassed
}
