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
	"github.com/phayes/freeport"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/reader"
<<<<<<< HEAD
	"github.com/pingcap/tiflow/cdc/sink/mysql"
=======
	mysqlDDL "github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/txn"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	"github.com/stretchr/testify/require"
)

var _ reader.RedoLogReader = &MockReader{}

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
func (br *MockReader) Run(ctx context.Context) error {
	return nil
}

<<<<<<< HEAD
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
=======
// ReadNextRow implements LogReader.ReadNextRow
func (br *MockReader) ReadNextRow(ctx context.Context) (*model.RowChangedEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case row := <-br.redoLogCh:
		return row, nil
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	}
}

// ReadNextDDL implements LogReader.ReadNextDDL
<<<<<<< HEAD
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
=======
func (br *MockReader) ReadNextDDL(ctx context.Context) (*model.DDLEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ddl := <-br.ddlEventCh:
		return ddl, nil
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	}
}

// ReadMeta implements LogReader.ReadMeta
func (br *MockReader) ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error) {
	return br.checkpointTs, br.resolvedTs, nil
}

func TestApply(t *testing.T) {
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
	// DML sink and DDL sink share the same db
	db := getMockDB(t)
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
<<<<<<< HEAD
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
=======
		if dbIndex%2 == 0 {
			testDB, err := pmysql.MockTestDB(true)
			require.Nil(t, err)
			return testDB, nil
		}
		return db, nil
	}

	getDMLDBConnBak := txn.GetDBConnImpl
	txn.GetDBConnImpl = mockGetDBConn
	getDDLDBConnBak := mysqlDDL.GetDBConnImpl
	mysqlDDL.GetDBConnImpl = mockGetDBConn
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
	createRedoReaderBak := createRedoReader
	createRedoReader = createMockReader
	defer func() {
		createRedoReader = createRedoReaderBak
<<<<<<< HEAD
		mysql.GetDBConnImpl = getDBConnBak
=======
		txn.GetDBConnImpl = getDMLDBConnBak
		mysqlDDL.GetDBConnImpl = getDDLDBConnBak
>>>>>>> 9499d6200d (redo(ticdc): support for applying ddl event in applier (#8362))
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
			CommitTs: resolvedTs,
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
	ddls := []*model.DDLEvent{
		{
			CommitTs: checkpointTs,
			TableInfo: &model.TableInfo{
				TableName: model.TableName{
					Schema: "test", Table: "checkpoint",
				},
			},
			Query: "create table checkpoint(id int)",
			Type:  timodel.ActionCreateTable,
		},
		{
			CommitTs: resolvedTs,
			TableInfo: &model.TableInfo{
				TableName: model.TableName{
					Schema: "test", Table: "resolved",
				},
			},
			Query: "create table resolved(id int)",
			Type:  timodel.ActionCreateTable,
		},
	}
	for _, ddl := range ddls {
		ddlEventCh <- ddl
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

func getMockDB(t *testing.T) *sql.DB {
	// normal db
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)

	// Before we write data to downstream, we need to check whether the downstream is TiDB.
	// So we mock a select tidb_version() query.
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table checkpoint(id int)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(1, "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// First, apply row which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a`,`b`) IN ((?,?))").
		WithArgs(1, "2").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(2, "3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Then, apply ddl which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table resolved(id int)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectClose()
	return db
}
