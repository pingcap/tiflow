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
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/phayes/freeport"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/reader"
	mysqlDDL "github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/txn"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
)

var _ reader.RedoLogReader = &MockReader{}

// MockReader is a mock redo log reader that implements LogReader interface
type MockReader struct {
	checkpointTs uint64
	resolvedTs   uint64
	redoLogCh    chan *model.RowChangedEvent
	ddlEventCh   chan *model.DDLEvent
}

// NewMockReader creates a new MockReader
func NewMockReader(
	checkpointTs uint64,
	resolvedTs uint64,
	redoLogCh chan *model.RowChangedEvent,
	ddlEventCh chan *model.DDLEvent,
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

// ReadNextRow implements LogReader.ReadNextRow
func (br *MockReader) ReadNextRow(ctx context.Context) (*model.RowChangedEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case row := <-br.redoLogCh:
		return row, nil
	}
}

// ReadNextDDL implements LogReader.ReadNextDDL
func (br *MockReader) ReadNextDDL(ctx context.Context) (*model.DDLEvent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ddl := <-br.ddlEventCh:
		return ddl, nil
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
	redoLogCh := make(chan *model.RowChangedEvent, 1024)
	ddlEventCh := make(chan *model.DDLEvent, 1024)
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
	createRedoReaderBak := createRedoReader
	createRedoReader = createMockReader
	defer func() {
		createRedoReader = createRedoReaderBak
		txn.GetDBConnImpl = getDMLDBConnBak
		mysqlDDL.GetDBConnImpl = getDDLDBConnBak
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
		},
		// update event which doesn't modify handle key
		{
			StartTs:  1120,
			CommitTs: 1220,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			PreColumns: []*model.Column{
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
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 1,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "3",
					Flag:  0,
				},
			},
		},
		{
			StartTs:  1150,
			CommitTs: 1250,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 10,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "20",
					Flag:  0,
				},
			},
		},
		{
			StartTs:  1150,
			CommitTs: 1250,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 100,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "200",
					Flag:  0,
				},
			},
		},
		{
			StartTs:  1200,
			CommitTs: resolvedTs,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			PreColumns: []*model.Column{
				{
					Name:  "a",
					Value: 10,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "20",
					Flag:  0,
				},
			},
		},
		{
			StartTs:  1200,
			CommitTs: resolvedTs,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			PreColumns: []*model.Column{
				{
					Name:  "a",
					Value: 1,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "3",
					Flag:  0,
				},
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 2,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "3",
					Flag:  0,
				},
			},
		},
		{
			StartTs:  1200,
			CommitTs: resolvedTs,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			PreColumns: []*model.Column{
				{
					Name:  "a",
					Value: 100,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "200",
					Flag:  0,
				},
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 200,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: "300",
					Flag:  0,
				},
			},
		},
	}
	for _, dml := range dmls {
		redoLogCh <- dml
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
			Query: "create table resolved(id int not null unique key)",
			Type:  timodel.ActionCreateTable,
		},
	}
	for _, ddl := range ddls {
		ddlEventCh <- ddl
	}
	close(redoLogCh)
	close(ddlEventCh)

	dir, err := os.Getwd()
	require.Nil(t, err)
	cfg := &RedoApplierConfig{
		SinkURI: "mysql://127.0.0.1:4000/?worker-count=1&max-txn-row=1" +
			"&tidb_placement_mode=ignore&safe-mode=true&cache-prep-stmts=false" +
			"&multi-stmt-enable=false",
		Dir: dir,
	}
	ap := NewRedoApplier(cfg)
	err = ap.Apply(ctx)
	require.Nil(t, err)
}

func TestApplyBigTxn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkpointTs := uint64(1000)
	resolvedTs := uint64(2000)
	redoLogCh := make(chan *model.RowChangedEvent, 1024)
	ddlEventCh := make(chan *model.DDLEvent, 1024)
	createMockReader := func(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
		return NewMockReader(checkpointTs, resolvedTs, redoLogCh, ddlEventCh), nil
	}

	dbIndex := 0
	// DML sink and DDL sink share the same db
	db := getMockDBForBigTxn(t)
	mockGetDBConn := func(ctx context.Context, dsnStr string) (*sql.DB, error) {
		defer func() {
			dbIndex++
		}()
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
	createRedoReaderBak := createRedoReader
	createRedoReader = createMockReader
	defer func() {
		createRedoReader = createRedoReaderBak
		txn.GetDBConnImpl = getDMLDBConnBak
		mysqlDDL.GetDBConnImpl = getDDLDBConnBak
	}()

	dmls := make([]*model.RowChangedEvent, 0)
	// insert some rows
	for i := 1; i <= 60; i++ {
		dml := &model.RowChangedEvent{
			StartTs:  1100,
			CommitTs: 1200,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: i,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: fmt.Sprintf("%d", i+1),
					Flag:  0,
				},
			},
		}
		dmls = append(dmls, dml)
	}
	// update
	for i := 1; i <= 60; i++ {
		dml := &model.RowChangedEvent{
			StartTs:  1200,
			CommitTs: 1300,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			PreColumns: []*model.Column{
				{
					Name:  "a",
					Value: i,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: fmt.Sprintf("%d", i+1),
					Flag:  0,
				},
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: i * 10,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: fmt.Sprintf("%d", i*10+1),
					Flag:  0,
				},
			},
		}
		dmls = append(dmls, dml)
	}
	// delete and update
	for i := 1; i <= 30; i++ {
		dml := &model.RowChangedEvent{
			StartTs:  1300,
			CommitTs: resolvedTs,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			PreColumns: []*model.Column{
				{
					Name:  "a",
					Value: i * 10,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: fmt.Sprintf("%d", i*10+1),
					Flag:  0,
				},
			},
		}
		dmls = append(dmls, dml)
	}
	for i := 31; i <= 60; i++ {
		dml := &model.RowChangedEvent{
			StartTs:  1300,
			CommitTs: resolvedTs,
			Table:    &model.TableName{Schema: "test", Table: "t1"},
			PreColumns: []*model.Column{
				{
					Name:  "a",
					Value: i * 10,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: fmt.Sprintf("%d", i*10+1),
					Flag:  0,
				},
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: i * 100,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: fmt.Sprintf("%d", i*100+1),
					Flag:  0,
				},
			},
		}
		dmls = append(dmls, dml)
	}
	for _, dml := range dmls {
		redoLogCh <- dml
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
			Query: "create table resolved(id int not null unique key)",
			Type:  timodel.ActionCreateTable,
		},
	}
	for _, ddl := range ddls {
		ddlEventCh <- ddl
	}
	close(redoLogCh)
	close(ddlEventCh)

	dir, err := os.Getwd()
	require.Nil(t, err)
	cfg := &RedoApplierConfig{
		SinkURI: "mysql://127.0.0.1:4000/?worker-count=1&max-txn-row=1" +
			"&tidb_placement_mode=ignore&safe-mode=true&cache-prep-stmts=false" +
			"&multi-stmt-enable=false",
		Dir: dir,
	}
	ap := NewRedoApplier(cfg)
	err = ap.Apply(ctx)
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
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
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

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE `test`.`t1` SET `a` = ?, `b` = ? WHERE `a` = ? LIMIT 1").
		WithArgs(1, "3", 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(10, "20").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(100, "200").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// First, apply row which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a`,`b`) IN ((?,?))").
		WithArgs(10, "20").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a`,`b`) IN ((?,?))").
		WithArgs(1, "3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a`,`b`) IN ((?,?))").
		WithArgs(100, "200").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(2, "3").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
		WithArgs(200, "300").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Then, apply ddl which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table resolved(id int not null unique key)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectClose()
	return db
}

func getMockDBForBigTxn(t *testing.T) *sql.DB {
	// normal db
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.Nil(t, err)

	// Before we write data to downstream, we need to check whether the downstream is TiDB.
	// So we mock a select tidb_version() query.
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})
	mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{
		Number:  1305,
		Message: "FUNCTION test.tidb_version does not exist",
	})

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table checkpoint(id int)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	for i := 1; i <= 60; i++ {
		mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i, fmt.Sprintf("%d", i+1)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	mock.ExpectBegin()
	for i := 1; i <= 60; i++ {
		mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a`,`b`) IN ((?,?))").
			WithArgs(i, fmt.Sprintf("%d", i+1)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	for i := 1; i <= 60; i++ {
		mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i*10, fmt.Sprintf("%d", i*10+1)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	// First, apply row which commitTs equal to resolvedTs
	mock.ExpectBegin()
	for i := 1; i <= 60; i++ {
		mock.ExpectExec("DELETE FROM `test`.`t1` WHERE (`a`,`b`) IN ((?,?))").
			WithArgs(i*10, fmt.Sprintf("%d", i*10+1)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	for i := 31; i <= 60; i++ {
		mock.ExpectExec("REPLACE INTO `test`.`t1` (`a`,`b`) VALUES (?,?)").
			WithArgs(i*100, fmt.Sprintf("%d", i*100+1)).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	// Then, apply ddl which commitTs equal to resolvedTs
	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("create table resolved(id int not null unique key)").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectClose()
	return db
}
