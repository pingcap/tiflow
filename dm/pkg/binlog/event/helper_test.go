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

package event

import (
	"testing"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestGTIDsFromPreviousGTIDsEvent(t *testing.T) {
	t.Parallel()
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
	)

	// invalid binlog type, QueryEvent
	queryEv, err := GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	require.Nil(t, err)
	gSet, err := GTIDsFromPreviousGTIDsEvent(queryEv)
	require.True(t, terror.ErrBinlogPrevGTIDEvNotValid.Equal(err))
	require.Nil(t, gSet)

	// valid MySQL GTIDs
	gtidStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383"
	gSetExpect, err := gtid.ParserGTID(gmysql.MySQLFlavor, gtidStr)
	require.Nil(t, err)
	previousGTIDEv, err := GenPreviousGTIDsEvent(header, latestPos, gSetExpect)
	require.Nil(t, err)
	gSet, err = GTIDsFromPreviousGTIDsEvent(previousGTIDEv)
	require.Nil(t, err)
	require.Equal(t, gSetExpect, gSet)
}

func TestGTIDsFromMariaDBGTIDListEvent(t *testing.T) {
	t.Parallel()
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		latestPos uint32 = 4
	)

	// invalid binlog type, QueryEvent
	queryEv, err := GenQueryEvent(header, latestPos, 0, 0, 0, nil, []byte("schema"), []byte("BEGIN"))
	require.Nil(t, err)
	gSet, err := GTIDsFromMariaDBGTIDListEvent(queryEv)
	require.Regexp(t, ".*should be a MariadbGTIDListEvent.*", err)
	require.Nil(t, gSet)

	// valid MariaDB GTIDs
	gtidStr := "1-1-1,2-2-2"
	gSetExpect, err := gtid.ParserGTID(gmysql.MariaDBFlavor, gtidStr)
	require.Nil(t, err)
	mariaGTIDListEv, err := GenMariaDBGTIDListEvent(header, latestPos, gSetExpect)
	require.Nil(t, err)
	gSet, err = GTIDsFromMariaDBGTIDListEvent(mariaGTIDListEv)
	require.Nil(t, err)
	require.Equal(t, gSetExpect, gSet)
}
