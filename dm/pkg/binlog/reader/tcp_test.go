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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/binlog/common"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var (
	flavor    = gmysql.MySQLFlavor
	serverIDs = []uint32{3251, 3252}
)

func TestTCPReaderSuite(t *testing.T) {
	suite.Run(t, new(testTCPReaderSuite))
}

type testTCPReaderSuite struct {
	suite.Suite
}

func (t *testTCPReaderSuite) SetupSuite() {
	require.Nil(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStartSyncByPos", "return(true)"))
	require.Nil(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStartSyncByGTID", "return(true)"))
	require.Nil(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderClose", "return(true)"))
	require.Nil(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderGetEvent", "return(true)"))
	require.Nil(t.T(), failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStatus", "return(true)"))
}

func (t *testTCPReaderSuite) TearDownSuite() {
	require.Nil(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStartSyncByPos"))
	require.Nil(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStartSyncByGTID"))
	require.Nil(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderClose"))
	require.Nil(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderGetEvent"))
	require.Nil(t.T(), failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStatus"))
}

func (t *testTCPReaderSuite) TestSyncPos() {
	var (
		cfg = replication.BinlogSyncerConfig{ServerID: serverIDs[0], Flavor: flavor}
		pos gmysql.Position // empty position
	)

	// the first reader
	r := NewTCPReader(cfg)
	require.NotNil(t.T(), r)

	// not prepared
	e, err := r.GetEvent(context.Background())
	require.NotNil(t.T(), err)
	require.Nil(t.T(), e)

	// prepare
	err = r.StartSyncByPos(pos)
	require.Nil(t.T(), err)

	// check status, stagePrepared
	status := r.Status()
	trStatus, ok := status.(*TCPReaderStatus)
	require.True(t.T(), ok)
	require.Equal(t.T(), common.StagePrepared.String(), trStatus.Stage)
	require.Greater(t.T(), trStatus.ConnID, uint32(0))
	trStatusStr := trStatus.String()
	require.True(t.T(), strings.Contains(trStatusStr, common.StagePrepared.String()))
	// re-prepare is invalid
	err = r.StartSyncByPos(pos)
	require.NotNil(t.T(), err)

	// close the reader
	err = r.Close()
	require.Nil(t.T(), err)

	// already closed
	err = r.Close()
	require.NotNil(t.T(), err)
}

func (t *testTCPReaderSuite) TestSyncGTID() {
	var (
		cfg  = replication.BinlogSyncerConfig{ServerID: serverIDs[1], Flavor: flavor}
		gSet gmysql.GTIDSet // nil GTID set
	)

	// the first reader
	r := NewTCPReader(cfg)
	require.NotNil(t.T(), r)

	// check status, stageNew
	status := r.Status()
	trStatus, ok := status.(*TCPReaderStatus)
	require.True(t.T(), ok)
	require.Equal(t.T(), common.StageNew.String(), trStatus.Stage)

	// not prepared
	e, err := r.GetEvent(context.Background())
	require.NotNil(t.T(), err)
	require.Nil(t.T(), e)

	// nil GTID set
	err = r.StartSyncByGTID(gSet)
	require.NotNil(t.T(), err)

	// empty GTID set
	gSet, err = gtid.ParserGTID(flavor, "")
	require.Nil(t.T(), err)

	// prepare
	err = r.StartSyncByGTID(gSet)
	require.Nil(t.T(), err)

	// re-prepare is invalid
	err = r.StartSyncByGTID(gSet)
	require.NotNil(t.T(), err)

	// close the reader
	err = r.Close()
	require.Nil(t.T(), err)

	// check status, stageClosed
	status = r.Status()
	trStatus, ok = status.(*TCPReaderStatus)
	require.True(t.T(), ok)
	require.Equal(t.T(), common.StageClosed.String(), trStatus.Stage)
	require.Greater(t.T(), trStatus.ConnID, uint32(0))

	// already closed
	err = r.Close()
	require.NotNil(t.T(), err)
}
