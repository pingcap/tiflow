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

package util

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/diff"
	"github.com/pingcap/tidb/util/dbutil"
	"go.uber.org/zap"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Name string `toml:"name" json:"name"`

	Port int `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// CreateDB create a mysql fd
func CreateDB(cfg DBConfig) (*sql.DB, error) {
	// just set to the same timezone so the timestamp field of mysql will return the same value
	// timestamp field will be display as the time zone of the Local time of drainer when write to kafka, so we set it to local time to pass CI now
	_, offset := time.Now().Zone()
	zone := fmt.Sprintf("'+%02d:00'", offset/3600)
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&interpolateParams=true&multiStatements=true&time_zone=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name, url.QueryEscape(zone))
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

// CloseDB close the mysql fd
func CloseDB(db *sql.DB) error {
	return errors.Trace(db.Close())
}

// CloseDBs close the mysql fd
func CloseDBs(dbs []*sql.DB) error {
	for _, db := range dbs {
		err := db.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CheckSyncState check if srouceDB and targetDB has the same table and data
func CheckSyncState(sourceDB, targetDB *sql.DB, schema string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tables, err := dbutil.GetTables(ctx, sourceDB, schema)
	if err != nil {
		log.Error("get tables", zap.Error(err))
		return false
	}

	for _, table := range tables {
		sourceTableInstance := &diff.TableInstance{
			Conn:   sourceDB,
			Schema: schema,
			Table:  table,
		}

		targetTableInstance := &diff.TableInstance{
			Conn:   targetDB,
			Schema: schema,
			Table:  table,
		}
		tableDiff := &diff.TableDiff{
			SourceTables: []*diff.TableInstance{sourceTableInstance},
			TargetTable:  targetTableInstance,
			UseChecksum:  true,
			ChunkSize:    1000,
			CpDB:         targetDB,
		}
		structEqual, dataEqual, err := tableDiff.Equal(context.Background(), func(sql string) error {
			log.Info("check equal", zap.String("sql", sql))
			return nil
		})
		if err != nil {
			log.Error("check equal", zap.String("err", errors.ErrorStack(err)))
			return false
		}
		if !structEqual || !dataEqual {
			return false
		}
	}

	// check whether the tables in the targetDB is match that in the sourceDB
	targetTables, err := dbutil.GetTables(ctx, targetDB, schema)
	if err != nil {
		log.Error("get tables", zap.Error(err))
		return false
	}
	sourceTableMap := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		sourceTableMap[table] = struct{}{}
	}
	for _, table := range targetTables {
		if _, exist := sourceTableMap[table]; !exist {
			log.Info("The table in target db does not exist in source db", zap.String("table", table))
			return false
		}
	}

	return true
}

// MustExec executes sqls
func MustExec(db *sql.DB, sql string, args ...interface{}) {
	_, err := db.Exec(sql, args...)
	if err != nil {
		log.S().Fatalf("exec failed, sql: %s args: %v, err: %+v", sql, args, err)
	}
}

// MustExecWithConn executes sqls with context
func MustExecWithConn(ctx context.Context, conn *sql.Conn, sql string, args ...interface{}) {
	var err error
	_, err = conn.ExecContext(ctx, sql, args...)
	if err != nil && errors.Cause(err) == context.DeadlineExceeded && errors.Cause(err) == context.Canceled {
		log.S().Fatal(err)
	}
}

// CreateSourceDBs return source sql.DB for test
// we create two TiDB instance now in tests/integration_tests/run.sh, change it if needed
func CreateSourceDBs() (dbs []*sql.DB, err error) {
	cfg := DBConfig{
		Host:     "127.0.0.1",
		User:     "root",
		Password: "",
		Name:     "test",
		Port:     4000,
	}

	src1, err := CreateDB(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg.Port = 4001
	src2, err := CreateDB(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbs = append(dbs, src1, src2)
	return
}

// CreateSourceDB return source sql.DB for test
func CreateSourceDB() (db *sql.DB, err error) {
	cfg := DBConfig{
		Host:     "127.0.0.1",
		User:     "root",
		Password: "",
		Name:     "test",
		Port:     4000,
	}

	return CreateDB(cfg)
}

// CreateSinkDB return sink sql.DB for test
func CreateSinkDB() (db *sql.DB, err error) {
	cfg := DBConfig{
		Host:     "127.0.0.1",
		User:     "root",
		Password: "",
		Name:     "test",
		Port:     3306,
	}

	return CreateDB(cfg)
}
