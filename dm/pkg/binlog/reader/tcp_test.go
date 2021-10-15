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

package reader

import (
	"context"
	"strings"
	"testing"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/pkg/binlog/common"
	"github.com/pingcap/dm/pkg/gtid"
)

var (
	_         = Suite(&testTCPReaderSuite{})
	flavor    = gmysql.MySQLFlavor
	serverIDs = []uint32{3251, 3252}
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testTCPReaderSuite struct{}

func (t *testTCPReaderSuite) SetUpSuite(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderStartSyncByPos", "return(true)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderStartSyncByGTID", "return(true)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderClose", "return(true)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderGetEvent", "return(true)"), IsNil)
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderStatus", "return(true)"), IsNil)
}

func (t *testTCPReaderSuite) TearDownSuite(c *C) {
	c.Assert(failpoint.Disable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderStartSyncByPos"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderStartSyncByGTID"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderClose"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderGetEvent"), IsNil)
	c.Assert(failpoint.Disable("github.com/pingcap/dm/pkg/binlog/reader/MockTCPReaderStatus"), IsNil)
}

func (t *testTCPReaderSuite) TestSyncPos(c *C) {
	var (
		cfg = replication.BinlogSyncerConfig{ServerID: serverIDs[0], Flavor: flavor}
		pos gmysql.Position // empty position
	)

	// the first reader
	r := NewTCPReader(cfg)
	c.Assert(r, NotNil)

	// not prepared
	e, err := r.GetEvent(context.Background())
	c.Assert(err, NotNil)
	c.Assert(e, IsNil)

	// prepare
	err = r.StartSyncByPos(pos)
	c.Assert(err, IsNil)

	// check status, stagePrepared
	status := r.Status()
	trStatus, ok := status.(*TCPReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(trStatus.Stage, Equals, common.StagePrepared.String())
	c.Assert(trStatus.ConnID, Greater, uint32(0))
	trStatusStr := trStatus.String()
	c.Assert(strings.Contains(trStatusStr, common.StagePrepared.String()), IsTrue)

	// re-prepare is invalid
	err = r.StartSyncByPos(pos)
	c.Assert(err, NotNil)

	// close the reader
	err = r.Close()
	c.Assert(err, IsNil)

	// already closed
	err = r.Close()
	c.Assert(err, NotNil)
}

func (t *testTCPReaderSuite) TestSyncGTID(c *C) {
	var (
		cfg  = replication.BinlogSyncerConfig{ServerID: serverIDs[1], Flavor: flavor}
		gSet gtid.Set // nil GTID set
	)

	// the first reader
	r := NewTCPReader(cfg)
	c.Assert(r, NotNil)

	// check status, stageNew
	status := r.Status()
	trStatus, ok := status.(*TCPReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(trStatus.Stage, Equals, common.StageNew.String())

	// not prepared
	e, err := r.GetEvent(context.Background())
	c.Assert(err, NotNil)
	c.Assert(e, IsNil)

	// nil GTID set
	err = r.StartSyncByGTID(gSet)
	c.Assert(err, NotNil)

	// empty GTID set
	gSet, err = gtid.ParserGTID(flavor, "")
	c.Assert(err, IsNil)

	// prepare
	err = r.StartSyncByGTID(gSet)
	c.Assert(err, IsNil)

	// re-prepare is invalid
	err = r.StartSyncByGTID(gSet)
	c.Assert(err, NotNil)

	// close the reader
	err = r.Close()
	c.Assert(err, IsNil)

	// check status, stageClosed
	status = r.Status()
	trStatus, ok = status.(*TCPReaderStatus)
	c.Assert(ok, IsTrue)
	c.Assert(trStatus.Stage, Equals, common.StageClosed.String())
	c.Assert(trStatus.ConnID, Greater, uint32(0))

	// already closed
	err = r.Close()
	c.Assert(err, NotNil)
}
