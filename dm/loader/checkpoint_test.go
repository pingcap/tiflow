// Copyright 2019 PingCAP, Inc.
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

package loader

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/suite"
)

type lightningCpListSuite struct {
	suite.Suite
	mock   sqlmock.Sqlmock
	cpList *LightningCheckpointList
}

func TestLightningCpListSuite(t *testing.T) {
	suite.Run(t, new(lightningCpListSuite))
}

func (s *lightningCpListSuite) SetupTest() {
	s.mock = conn.InitMockDB(s.T())

	baseDB, err := conn.GetDownstreamDB(&dbconfig.DBConfig{})
	s.Require().NoError(err)

	metaSchema := "dm_meta"
	cpList := NewLightningCheckpointList(baseDB, "test_lightning", "source1", metaSchema, log.L())

	s.cpList = cpList
}

func (s *lightningCpListSuite) TearDownTest() {
	s.Require().NoError(s.mock.ExpectationsWereMet())
}

func (s *lightningCpListSuite) TestLightningCheckpointListPrepare() {
	ctx := context.Background()
	s.mock.ExpectBegin()
	s.mock.ExpectExec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.*", s.cpList.schema)).WillReturnResult(sqlmock.NewResult(1, 1))
	s.mock.ExpectCommit()
	s.mock.ExpectBegin()
	s.mock.ExpectExec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.*", s.cpList.tableName)).WillReturnResult(sqlmock.NewResult(1, 1))
	s.mock.ExpectCommit()
	err := s.cpList.Prepare(ctx)
	s.Require().NoError(err)
}

func (s *lightningCpListSuite) TestLightningCheckpointListStatusInit() {
	// no rows in target table, will return default status
	s.mock.ExpectQuery(fmt.Sprintf("SELECT status FROM %s WHERE `task_name` = \\? AND `source_name` = \\?", s.cpList.tableName)).
		WithArgs(s.cpList.taskName, s.cpList.sourceName).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).RowError(0, sql.ErrNoRows))
	status, err := s.cpList.taskStatus(context.Background())
	s.Require().NoError(err)
	s.Require().Equal(lightningStatusInit, status)
}

func (s *lightningCpListSuite) TestLightningCheckpointListStatusRunning() {
	s.mock.ExpectQuery(fmt.Sprintf("SELECT status FROM %s WHERE `task_name` = \\? AND `source_name` = \\?", s.cpList.tableName)).
		WithArgs(s.cpList.taskName, s.cpList.sourceName).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("running"))
	status, err := s.cpList.taskStatus(context.Background())
	s.Require().NoError(err)
	s.Require().Equal(lightningStatusRunning, status)
}

func (s *lightningCpListSuite) TestLightningCheckpointListRegister() {
	s.mock.ExpectBegin()
	s.mock.ExpectExec(fmt.Sprintf("INSERT IGNORE INTO %s \\(`task_name`, `source_name`\\) VALUES \\(\\?, \\?\\)", s.cpList.tableName)).
		WithArgs(s.cpList.taskName, s.cpList.sourceName).
		WillReturnResult(sqlmock.NewResult(2, 1))
	s.mock.ExpectCommit()
	err := s.cpList.RegisterCheckPoint(context.Background())
	s.Require().NoError(err)
}

func (s *lightningCpListSuite) TestLightningCheckpointListUpdateStatus() {
	s.mock.ExpectBegin()
	s.mock.ExpectExec(fmt.Sprintf("UPDATE %s set status = \\? WHERE `task_name` = \\? AND `source_name` = \\?", s.cpList.tableName)).
		WithArgs("running", s.cpList.taskName, s.cpList.sourceName).
		WillReturnResult(sqlmock.NewResult(3, 1))
	s.mock.ExpectCommit()
	err := s.cpList.UpdateStatus(context.Background(), lightningStatusRunning)
	s.Require().NoError(err)
}
