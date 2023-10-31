// Copyright 2021 PingCAP, Inc.
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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/transformer/columnselector"
	cmdUtil "github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

func main() {
	var upstreamURI string
	var downstreamURI string
	var dbNames []string
	var configFile string

	upstreamDB, err := openDB(upstreamURI)
	if err != nil {
		log.Panic("cannot open db for the upstream", zap.Error(err))
	}

	downstreamDB, err := openDB(downstreamURI)
	if err != nil {
		log.Panic("cannot open db for the downstream", zap.Error(err))
	}

	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile != "" {
		err = cmdUtil.StrictDecodeFile(configFile, "checksum checker", replicaConfig)
		if err != nil {
			log.Panic("cannot decode config file", zap.Error(err))
		}
	}

	columnFilter, err := columnselector.New(replicaConfig)
	if err != nil {
		log.Panic("cannot create column filter", zap.Error(err))
	}

	err = compareCRC32CheckSum(upstreamDB, downstreamDB, dbNames, columnFilter)
	if err != nil {
		log.Panic("compare checksum failed", zap.Error(err))
	}
	log.Info("compare checksum passed")
}

func compareCRC32CheckSum(
	upstream, downstream *sql.DB, dbNames []string, selector *columnselector.ColumnSelector,
) error {
	start := time.Now()
	source, err := getChecksum(upstream, dbNames, selector)
	if err != nil {
		log.Warn("get checksum for the upstream failed", zap.Error(err))
		return errors.Trace(err)
	}
	log.Info("get checksum for the upstream success",
		zap.Duration("elapsed", time.Since(start)))

	start = time.Now()
	sink, err := getChecksum(downstream, dbNames, selector)
	if err != nil {
		log.Warn("get checksum for the downstream failed", zap.Error(err))
		return errors.Trace(err)
	}
	log.Info("get checksum for the downstream success",
		zap.Duration("elapsed", time.Since(start)))

	if len(source) != len(sink) {
		log.Error("source and sink have different crc32 size",
			zap.Int("source", len(source)), zap.Int("sink", len(sink)))
		return fmt.Errorf("source and sink have different crc32 size, source: %d, sink: %d",
			len(source), len(sink))
	}

	for tableName, expected := range source {
		actual, ok := sink[tableName]
		if !ok {
			return fmt.Errorf("table not found at sink, table: %s", tableName)
		}
		if expected != actual {
			log.Error("crc32 mismatch",
				zap.String("table", tableName), zap.Uint32("source", expected), zap.Uint32("sink", actual))
			return fmt.Errorf("crc32 mismatch, table: %s, source: %d, sink: %d", tableName, expected, actual)
		}
	}
	return nil
}

func getChecksum(
	db *sql.DB, dbNames []string, selector *columnselector.ColumnSelector,
) (map[string]uint32, error) {
	result := make(map[string]uint32)
	for _, dbName := range dbNames {
		tables, err := getAllTables(db, dbName)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			tx, err := db.Begin()
			if err != nil {
				_ = tx.Rollback()
				return nil, errors.Trace(err)
			}
			columns, err := getColumns(tx, dbName, table, selector)
			if err != nil {
				_ = tx.Rollback()
				return nil, errors.Trace(err)
			}
			checksum, err := doChecksum(tx, dbName, table, columns)
			if err != nil {
				_ = tx.Rollback()
				return nil, errors.Trace(err)
			}
			_ = tx.Commit()
			result[dbName+"."+table] = checksum
		}
	}
	return result, nil
}

func doChecksum(tx *sql.Tx, schema, table string, columns []string) (uint32, error) {
	a := strings.Join(columns, "`,`")

	concat := fmt.Sprintf("CONCAT_WS(',', `%s`)", a)
	tableName := schema + "." + table
	query := fmt.Sprintf("SELECT BIT_XOR(CRC32(%s)) AS checksum FROM %s", concat, tableName)
	var checkSum uint32
	rows := tx.QueryRow(query)
	err := rows.Scan(&checkSum)
	if err != nil {
		log.Error("get crc32 checksum failed",
			zap.Error(err), zap.String("table", tableName), zap.String("query", query))
		return 0, errors.Trace(err)
	}
	log.Info("do checkSum success", zap.String("table", tableName), zap.Uint32("checkSum", checkSum))
	return checkSum, nil
}

func getColumns(tx *sql.Tx, schema, table string, selector *columnselector.ColumnSelector) (result []string, err error) {
	rows, err := tx.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", schema+"."+table))
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Warn("close rows failed", zap.Error(err))
		}
	}()

	for rows.Next() {
		var t columnInfo
		if err := rows.Scan(&t.Field, &t.Type, &t.Null, &t.Key, &t.Default, &t.Extra); err != nil {
			return result, errors.Trace(err)
		}
		if selector.Match(schema, table, t.Field) {
			result = append(result, t.Field)
		}
	}
	return result, nil
}

type columnInfo struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   string
}

func getAllTables(db *sql.DB, dbName string) ([]string, error) {
	var result []string
	dbName = strings.TrimSpace(dbName)
	tx, err := db.Begin()
	if err != nil {
		_ = tx.Rollback()
		return nil, errors.Trace(err)
	}
	query := fmt.Sprintf(`show full tables from %s where table_type != "VIEW"`, dbName)
	rows, err := tx.Query(query)
	if err != nil {
		_ = tx.Rollback()
		return nil, errors.Trace(err)
	}
	for rows.Next() {
		var t string
		var tt string
		if err := rows.Scan(&t, &tt); err != nil {
			_ = tx.Rollback()
			return nil, errors.Trace(err)
		}
		result = append(result, t)
	}
	_ = rows.Close()
	_ = tx.Commit()
	return result, nil
}

func openDB(uri string) (*sql.DB, error) {
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := db.Ping(); err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}
