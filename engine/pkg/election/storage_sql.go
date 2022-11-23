// Copyright 2022 PingCAP, Inc.
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

package election

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/pingcap/tiflow/pkg/errors"
)

const (
	sqlRecordID     = 1
	sqlCreateTable  = "CREATE TABLE IF NOT EXISTS %s (id int NOT NULL, version bigint NOT NULL, record text NOT NULL, PRIMARY KEY (id))"
	sqlQueryRecord  = "SELECT version, record FROM %s WHERE id = ?"
	sqlInsertRecord = "INSERT INTO %s (id, version, record) VALUES (?, ?, ?)"
	sqlUpdateRecord = "UPDATE %s SET version = ?, record = ? WHERE id = ? AND version = ?"
)

// SQLStorage is a storage implementation based on SQL database.
type SQLStorage struct {
	db        *sql.DB
	tableName string
}

// NewSQLStorage creates a new SQLStorage.
func NewSQLStorage(db *sql.DB, tableName string) (*SQLStorage, error) {
	if _, err := db.Exec(fmt.Sprintf(sqlCreateTable, tableName)); err != nil {
		return nil, errors.Trace(err)
	}
	return &SQLStorage{
		db:        db,
		tableName: tableName,
	}, nil
}

// Get implements Storage.Get.
func (s *SQLStorage) Get(ctx context.Context) (*Record, error) {
	var (
		version     int64
		recordBytes []byte
	)
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf(sqlQueryRecord, s.tableName), sqlRecordID).
		Scan(&version, &recordBytes)
	if err != nil {
		if err == sql.ErrNoRows {
			err = nil
		}
		return &Record{}, errors.Trace(err)
	}
	var record Record
	if err := json.Unmarshal(recordBytes, &record); err != nil {
		return nil, errors.Trace(err)
	}
	record.Version = version
	return &record, nil
}

// Update implements Storage.Update.
func (s *SQLStorage) Update(ctx context.Context, record *Record) error {
	recordBytes, err := json.Marshal(&record)
	if err != nil {
		return errors.Trace(err)
	}

	if record.Version == 0 {
		_, err := s.db.ExecContext(ctx,
			fmt.Sprintf(sqlInsertRecord, s.tableName), sqlRecordID, record.Version+1, recordBytes)
		return errors.Trace(err)
	}

	result, err := s.db.ExecContext(ctx,
		fmt.Sprintf(sqlUpdateRecord, s.tableName), record.Version+1, recordBytes, sqlRecordID, record.Version)
	if err != nil {
		return errors.Trace(err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Trace(err)
	}
	if rowsAffected != 1 {
		return errors.ErrElectionRecordConflict.GenWithStackByArgs()
	}
	return nil
}
