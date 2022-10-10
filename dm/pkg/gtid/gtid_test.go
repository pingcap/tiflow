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

package gtid

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/require"
)

func TestGTDISetSorted(t *testing.T) {
	t.Parallel()

	// check mysql
	gSet, err := ParserGTID(mysql.MySQLFlavor, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423")
	require.NoError(t, err)
	sortedGTIDSet := "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454,3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190"
	require.Equal(t, sortedGTIDSet, gSet.String())
	// check mariadb
	gSet, err = ParserGTID(mysql.MariaDBFlavor, "0-0-1,1-1-1,4-20-1,3-1-1,10-10-10")
	require.NoError(t, err)
	sortedGTIDSet = "0-0-1,1-1-1,10-10-10,3-1-1,4-20-1"
	require.Equal(t, sortedGTIDSet, gSet.String())
}

func TestMinGTIDSet(t *testing.T) {
	t.Parallel()

	gset, err := ZeroGTIDSet(mysql.MySQLFlavor)
	require.NoError(t, err)
	require.IsType(t, &mysql.MysqlGTIDSet{}, gset)
	require.Len(t, gset.String(), 0)

	gset, err = ZeroGTIDSet(mysql.MariaDBFlavor)
	require.NoError(t, err)
	require.IsType(t, &mysql.MariadbGTIDSet{}, gset)
	require.Len(t, gset.String(), 0)

	_, err = ZeroGTIDSet("wrong flavor")
	require.Error(t, err)
}

func TestParseGTIDNoFlavor(t *testing.T) {
	t.Parallel()

	gset, err := ParserGTID("", "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14")
	require.NoError(t, err)
	require.IsType(t, &mysql.MysqlGTIDSet{}, gset)

	gset, err = ParserGTID("", "0-0-1,1-1-1,4-20-1,3-1-1,10-10-10")
	require.NoError(t, err)
	require.IsType(t, &mysql.MariadbGTIDSet{}, gset)

	_, err = ParserGTID("", "")
	require.Error(t, err)
}

func TestIsNilGTIDSet(t *testing.T) {
	t.Parallel()

	require.False(t, IsZeroMySQLGTIDSet(""))
	require.False(t, IsZeroMySQLGTIDSet("xxxxx"))
	require.False(t, IsZeroMySQLGTIDSet("xxxxx:0,yyyy:0"))
	require.False(t, IsZeroMySQLGTIDSet("xxxxx:1-2"))
	require.False(t, IsZeroMySQLGTIDSet("xxxxx:0-0"))
	require.True(t, IsZeroMySQLGTIDSet("xxxxx:0"))
	require.True(t, IsZeroMySQLGTIDSet(" xxxxx:0 "))
}

func TestParseZeroAsEmptyGTIDSet(t *testing.T) {
	t.Parallel()

	gset, err := ParserGTID(mysql.MariaDBFlavor, "0-0-0")
	require.NoError(t, err)
	require.Equal(t, "", gset.String())

	gset, err = ParserGTID(mysql.MySQLFlavor, "")
	require.NoError(t, err)
	require.Equal(t, "", gset.String())

	gset, err = ParserGTID(mysql.MySQLFlavor, "3ccc475b-2343-11e7-be21-6c0b84d59f30:0")
	require.NoError(t, err)
	require.Equal(t, "", gset.String())
}
