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

package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/stretchr/testify/suite"
)

func TestDBSuite(t *testing.T) {
	suite.Run(t, new(testDBSuite))
}

type testDBSuite struct {
	suite.Suite
	db       *sql.DB
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
	cfg      *config.SubTaskConfig
}

func (s *testDBSuite) SetupSuite() {
	s.cfg = &config.SubTaskConfig{
		From:       config.GetDBConfigForTest(),
		To:         config.GetDBConfigForTest(),
		ServerID:   102,
		MetaSchema: "db_test",
		Name:       "db_ut",
		Mode:       config.ModeIncrement,
		Flavor:     "mysql",
	}
	s.cfg.From.Adjust()
	s.cfg.To.Adjust()

	dir := s.T().TempDir()
	s.cfg.RelayDir = dir

	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", dbAddr)
	s.Require().NoError(err)

	s.resetBinlogSyncer()
	_, err = s.db.Exec("SET GLOBAL binlog_format = 'ROW';")
	s.Require().NoError(err)
}

func (s *testDBSuite) resetBinlogSyncer() {
	cfg := replication.BinlogSyncerConfig{
		ServerID:       s.cfg.ServerID,
		Flavor:         "mysql",
		Host:           s.cfg.From.Host,
		Port:           uint16(s.cfg.From.Port),
		User:           s.cfg.From.User,
		Password:       s.cfg.From.Password,
		UseDecimal:     false,
		VerifyChecksum: true,
	}
	cfg.TimestampStringLocation = time.UTC

	if s.syncer != nil {
		s.syncer.Close()
	}

	pos, _, err := conn.GetPosAndGs(tcontext.Background(), conn.NewBaseDBForTest(s.db), "mysql")
	s.Require().NoError(err)

	s.syncer = replication.NewBinlogSyncer(cfg)
	s.streamer, err = s.syncer.StartSync(pos)
	s.Require().NoError(err)
}

func (s *testDBSuite) TestGetServerUUID() {
	u, err := conn.GetServerUUID(tcontext.Background(), conn.NewBaseDBForTest(s.db), "mysql")
	s.Require().NoError(err)
	_, err = uuid.Parse(u)
	s.Require().NoError(err)
}

func (s *testDBSuite) TestGetServerID() {
	id, err := conn.GetServerID(tcontext.Background(), conn.NewBaseDBForTest(s.db))
	s.Require().NoError(err)
	s.Require().Greater(id, uint32(0))
}

func (s *testDBSuite) TestGetServerUnixTS() {
	id, err := conn.GetServerUnixTS(context.Background(), conn.NewBaseDBForTest(s.db))
	s.Require().NoError(err)
	s.Require().Greater(id, int64(0))
}
