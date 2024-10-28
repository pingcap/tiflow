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
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
)

var _ = check.Suite(&testDBSuite{})

type testDBSuite struct {
	db       *sql.DB
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
	cfg      *config.SubTaskConfig
}

func (s *testDBSuite) SetUpSuite(c *check.C) {
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

	dir := c.MkDir()
	s.cfg.RelayDir = dir

	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", dbAddr)
	c.Assert(err, check.IsNil)

	s.resetBinlogSyncer(c)
	_, err = s.db.Exec("SET GLOBAL binlog_format = 'ROW';")
	c.Assert(err, check.IsNil)
}

func (s *testDBSuite) resetBinlogSyncer(c *check.C) {
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
	c.Assert(err, check.IsNil)

	s.syncer = replication.NewBinlogSyncer(cfg)
	s.streamer, err = s.syncer.StartSync(pos)
	c.Assert(err, check.IsNil)
}

func (s *testDBSuite) TestGetServerUUID(c *check.C) {
	u, err := conn.GetServerUUID(tcontext.Background(), conn.NewBaseDBForTest(s.db), "mysql")
	c.Assert(err, check.IsNil)
	_, err = uuid.Parse(u)
	c.Assert(err, check.IsNil)
}

func (s *testDBSuite) TestGetServerID(c *check.C) {
	id, err := conn.GetServerID(tcontext.Background(), conn.NewBaseDBForTest(s.db))
	c.Assert(err, check.IsNil)
	c.Assert(id, check.Greater, uint32(0))
}

func (s *testDBSuite) TestGetServerUnixTS(c *check.C) {
	id, err := conn.GetServerUnixTS(context.Background(), conn.NewBaseDBForTest(s.db))
	c.Assert(err, check.IsNil)
	c.Assert(id, check.Greater, int64(0))
}
