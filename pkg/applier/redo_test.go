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

package applier

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/reader"
	"github.com/pingcap/tiflow/cdc/sink/mysql"
	"github.com/stretchr/testify/require"
)

// MockReader is a mock redo log reader that implements LogReader interface
type MockReader struct {
	checkpointTs uint64
	resolvedTs   uint64
	redoLogCh    chan *model.RedoRowChangedEvent
	ddlEventCh   chan *model.RedoDDLEvent
}

// NewMockReader creates a new MockReader
func NewMockReader(
	checkpointTs uint64,
	resolvedTs uint64,
	redoLogCh chan *model.RedoRowChangedEvent,
	ddlEventCh chan *model.RedoDDLEvent,
) *MockReader {
	return &MockReader{
		checkpointTs: checkpointTs,
		resolvedTs:   resolvedTs,
		redoLogCh:    redoLogCh,
		ddlEventCh:   ddlEventCh,
	}
}

// ResetReader implements LogReader.ReadLog
func (br *MockReader) ResetReader(ctx context.Context, startTs, endTs uint64) error {
	return nil
}

// ReadNextLog implements LogReader.ReadNextLog
func (br *MockReader) ReadNextLog(ctx context.Context, maxNumberOfMessages uint64) ([]*model.RedoRowChangedEvent, error) {
	cached := make([]*model.RedoRowChangedEvent, 0)
	for {
		select {
		case <-ctx.Done():
			return cached, nil
		case redoLog, ok := <-br.redoLogCh:
			if !ok {
				return cached, nil
			}
			cached = append(cached, redoLog)
			if len(cached) >= int(maxNumberOfMessages) {
				return cached, nil
			}
		}
	}
}

// ReadNextDDL implements LogReader.ReadNextDDL
func (br *MockReader) ReadNextDDL(ctx context.Context, maxNumberOfDDLs uint64) ([]*model.RedoDDLEvent, error) {
	cached := make([]*model.RedoDDLEvent, 0)
	for {
		select {
		case <-ctx.Done():
			return cached, nil
		case ddl, ok := <-br.ddlEventCh:
			if !ok {
				return cached, nil
			}
			cached = append(cached, ddl)
			if len(cached) >= int(maxNumberOfDDLs) {
				return cached, nil
			}
		}
	}
}

// ReadMeta implements LogReader.ReadMeta
func (br *MockReader) ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error) {
	return br.checkpointTs, br.resolvedTs, nil
}

// Close implements LogReader.Close.
func (br *MockReader) Close() error {
	return nil
}

func TestApplyDMLs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkpointTs := uint64(1000)
	resolvedTs := uint64(2000)
	redoLogCh := make(chan *model.RedoRowChangedEvent, 1024)
	ddlEventCh := make(chan *model.RedoDDLEvent, 1024)
	createMockReader := func(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
		return NewMockReader(checkpointTs, resolvedTs, redoLogCh, ddlEventCh), nil
	}

	dbIndex := 0
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
		if dbIndex == 0 {
			// mock for test db, which is used querying TiDB session variable
			db, mock, err := sqlmock.New()
			if err != nil {
				return nil, err
			}
			mock.ExpectQuery("SELECT @@SESSION.sql_mode;").
				WillReturnRows(sqlmock.NewRows([]string{"@@SESSION.sql_mode"}).
					AddRow("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE"))
			columns := []string{"Variable_name", "Value"}
			mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
				sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
			)
			mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
				sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
			)
			mock.ExpectQuery("show session variables like 'transaction_isolation';").WillReturnRows(
				sqlmock.NewRows(columns).AddRow("transaction_isolation", "REPEATED-READ"),
			)
			mock.ExpectQuery("show session variables like 'tidb_placement_mode';").
				WillReturnRows(
					sqlmock.NewRows(columns).
						AddRow("tidb_placement_mode", "IGNORE"),
				)
			mock.ExpectQuery("select character_set_name from information_schema.character_sets " +
				"where character_set_name = 'gbk';").WillReturnRows(
				sqlmock.NewRows([]string{"character_set_name"}).AddRow("gbk"),
			)
			mock.ExpectQuery("select tidb_version()").WillReturnError(&dmysql.MySQLError{
				Number:  1305,
				Message: "FUNCTION test.tidb_version does not exist",
			})
			mock.ExpectClose()
			return db, nil
		}
		// normal db
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.Nil(t, err)
		mock.ExpectBegin()
		mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(1, "2").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()

		mock.ExpectBegin()
		mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a`) IN ((?))").
			WithArgs(1).
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(2, "3").
			WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
		mock.ExpectClose()
		return db, nil
	}

	getDBConnBak := mysql.GetDBConnImpl
	mysql.GetDBConnImpl = mockGetDBConn
	createRedoReaderBak := createRedoReader
	createRedoReader = createMockReader
	defer func() {
		createRedoReader = createRedoReaderBak
		mysql.GetDBConnImpl = getDBConnBak
	}()

	dmls := []*model.RowChangedEvent{
		{
			StartTs:  1100,
			CommitTs: 1200,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 1,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "2",
					Flag:  0,
				},
			},
			IndexColumns: [][]int{{0}},
		},
		{
			StartTs:  1200,
			CommitTs: 1300,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			PreColumns: []*model.Column{
				{
					Name:  "a",
					Value: 1,
					Flag:  model.HandleKeyFlag | model.UniqueKeyFlag | model.PrimaryKeyFlag,
				}, {
					Name:  "b",
					Value: "2",
					Flag:  0,
				},
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 2,
					Flag:  model.HandleKeyFlag | model.UniqueKeyFlag | model.PrimaryKeyFlag,
				}, {
					Name:  "b",
					Value: "3",
					Flag:  0,
				},
			},
			IndexColumns: [][]int{{0}},
		},
	}
	for _, dml := range dmls {
		redoLogCh <- common.RowToRedo(dml)
	}
	close(redoLogCh)
	close(ddlEventCh)

	cfg := &RedoApplierConfig{
		SinkURI: "mysql://127.0.0.1:4000/?worker-count=1&max-txn-row=1&tidb_placement_mode=ignore&safe-mode=true",
	}
	ap := NewRedoApplier(cfg)
	err := ap.Apply(ctx)
	require.Nil(t, err)
}

func TestApplyMeetSinkError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	port, err := freeport.GetFreePort()
	require.Nil(t, err)
	cfg := &RedoApplierConfig{
		Storage: "blackhole://",
		SinkURI: fmt.Sprintf("mysql://127.0.0.1:%d/?read-timeout=1s&timeout=1s", port),
	}
	ap := NewRedoApplier(cfg)
	err = ap.Apply(ctx)
	require.Regexp(t, "CDC:ErrMySQLConnectionError", err)
}
