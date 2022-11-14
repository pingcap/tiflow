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
)

var (
	flavor    = gmysql.MySQLFlavor
	serverIDs = []uint32{3251, 3252}
)

func SetUpSuite(t *testing.T) {
	require.Nil(t, failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStartSyncByPos", "return(true)"))
	require.Nil(t, failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStartSyncByGTID", "return(true)"))
	require.Nil(t, failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderClose", "return(true)"))
	require.Nil(t, failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderGetEvent", "return(true)"))
	require.Nil(t, failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStatus", "return(true)"))
}

func TearDownSuite(t *testing.T) {
	require.Nil(t, failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStartSyncByPos"))
	require.Nil(t, failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStartSyncByGTID"))
	require.Nil(t, failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderClose"))
	require.Nil(t, failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderGetEvent"))
	require.Nil(t, failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockTCPReaderStatus"))
}

func TestSyncPos(t *testing.T) {
	t.Parallel()
	var (
		cfg = replication.BinlogSyncerConfig{ServerID: serverIDs[0], Flavor: flavor}
		pos gmysql.Position // empty position
	)

	// the first reader
	r := NewTCPReader(cfg)
	require.NotNil(t, r)

	// not prepared
	e, err := r.GetEvent(context.Background())
	require.NotNil(t, err)
	require.Nil(t, e)

	// prepare
	err = r.StartSyncByPos(pos)
	require.Nil(t, err)

	// check status, stagePrepared
	status := r.Status()
	trStatus, ok := status.(*TCPReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StagePrepared.String(), trStatus.Stage)
	require.Greater(t, trStatus.ConnID, uint32(0))
	trStatusStr := trStatus.String()
	require.True(t, strings.Contains(trStatusStr, common.StagePrepared.String()))
	// re-prepare is invalid
	err = r.StartSyncByPos(pos)
	require.NotNil(t, err)

	// close the reader
	err = r.Close()
	require.Nil(t, err)

	// already closed
	err = r.Close()
	require.NotNil(t, err)
}

func TestSyncGTID(t *testing.T) {
	t.Parallel()
	var (
		cfg  = replication.BinlogSyncerConfig{ServerID: serverIDs[1], Flavor: flavor}
		gSet gmysql.GTIDSet // nil GTID set
	)

	// the first reader
	r := NewTCPReader(cfg)
	require.NotNil(t, r)

	// check status, stageNew
	status := r.Status()
	trStatus, ok := status.(*TCPReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StageNew.String(), trStatus.Stage)

	// not prepared
	e, err := r.GetEvent(context.Background())
	require.NotNil(t, err)
	require.Nil(t, e)

	// nil GTID set
	err = r.StartSyncByGTID(gSet)
	require.NotNil(t, err)

	// empty GTID set
	gSet, err = gtid.ParserGTID(flavor, "")
	require.Nil(t, err)

	// prepare
	err = r.StartSyncByGTID(gSet)
	require.Nil(t, err)

	// re-prepare is invalid
	err = r.StartSyncByGTID(gSet)
	require.NotNil(t, err)

	// close the reader
	err = r.Close()
	require.Nil(t, err)

	// check status, stageClosed
	status = r.Status()
	trStatus, ok = status.(*TCPReaderStatus)
	require.True(t, ok)
	require.Equal(t, common.StageClosed.String(), trStatus.Stage)
	require.Greater(t, trStatus.ConnID, uint32(0))

	// already closed
	err = r.Close()
	require.NotNil(t, err)
}
