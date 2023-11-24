package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/alecthomas/chroma/quick"
	"github.com/fatih/color"
	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

var (
	statementKindsToWaitCDCRecord = map[string]bool{
		"Delete":  true,
		"Insert":  true,
		"Replace": true,
		"Update":  true,
	}
)

func resetDB(db *DBHelper) {
	db.MustExec("drop database if exists `" + *dbName + "`;")
	db.MustExec("create database `" + *dbName + "`;")
	db.MustExec("use `" + *dbName + "`;")
}

func runTestCase(testCasePath string) bool {
	resetDB(dbMySQL)
	resetDB(dbTiDB)

	hasError := false
	stmtAsts := readAndParseSQLText(testCasePath)
	for _, stmt := range stmtAsts {
		query := strings.TrimSpace(stmt.Text())
		statementKind := ast.GetStmtLabel(stmt)
		waitCDC := false
		if v, ok := statementKindsToWaitCDCRecord[statementKind]; ok {
			waitCDC = v
		}
		if runSingleQuery(query, waitCDC) {
			nPassed++
		} else {
			nFailed++
			hasError = true
		}
	}
	return hasError
}

func fetchNextCDCRecord(reader *kafka.Reader, kind Kind) (map[string]any, string, error) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			cancel()
			return nil, "", fmt.Errorf("Failed to read CDC record of %s: %w", kind, err)
		}
		cancel()

		var obj map[string]any
		err = json.Unmarshal(m.Value, &obj)
		if err != nil {
			return nil, "", fmt.Errorf("Failed to parse CDC record of %s (msg=%s): %w", kind, m.Value, err)
		}

		// Ignore DDL events in the Debezium's output
		if kind == KindMySQL {
			schema, ok := obj["schema"]
			if !ok {
				return nil, "", fmt.Errorf("Unexpected CDC record of %s: schema field not exist in %s", kind, m.Value)
			}
			if schema.(map[string]any)["name"] == "io.debezium.connector.mysql.SchemaChangeValue" {
				continue
			}
		}

		return obj, string(m.Value), nil
	}
}

var (
	ignoredRecordPaths = map[string]bool{
		`{map[string]any}["schema"]`:                             true,
		`{map[string]any}["payload"].(map[string]any)["source"]`: true,
		`{map[string]any}["payload"].(map[string]any)["ts_ms"]`:  true,
	}
)

var (
	headingColor = color.New(color.FgHiWhite, color.Bold)
)

func runSingleQuery(query string, waitCDC bool) bool {
	dbMySQL.MustExec(query)
	dbTiDB.MustExec(query)

	if !waitCDC {
		return true
	}

	onError := func(err error) {
		fmt.Println("==========================================")
		logger.Error("Test failed", zap.Error(err))
		headingColor.Print("\nSQL:\n\n")
		quick.Highlight(os.Stdout, query, "sql", "terminal16m", "vs")
		fmt.Println()
	}

	objDebezium, _, err := fetchNextCDCRecord(readerDebezium, KindMySQL)
	if err != nil {
		onError(err)
		return false
	}
	// logger.Info("Debezium output", zap.Any("record", objDebezium))

	objTiCDC, _, err := fetchNextCDCRecord(readerTiCDC, KindTiDB)
	if err != nil {
		onError(err)
		return false
	}
	// logger.Info("TiCDC output", zap.Any("record", objTiCDC))

	cmpOption := cmp.FilterPath(
		func(p cmp.Path) bool {
			path := p.GoString()
			_, shouldIgnore := ignoredRecordPaths[path]
			return shouldIgnore
		},
		cmp.Ignore(),
	)

	if diff := cmp.Diff(objDebezium, objTiCDC, cmpOption); diff != "" {
		err = fmt.Errorf("Found mismatch CDC record")
		onError(err)
		headingColor.Print("\nCDC Result Diff (-debezium +ticdc):\n\n")
		quick.Highlight(os.Stdout, diff, "diff", "terminal16m", "murphy")
		fmt.Println()
		return false
	}

	return true
}
