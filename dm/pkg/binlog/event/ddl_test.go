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
	"bytes"
	"fmt"
	"testing"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/stretchr/testify/require"
)

func TestGenDDLEvent(t *testing.T) {
	t.Parallel()
	var (
		serverID  uint32 = 101
		latestPos uint32 = 123
		schema           = "test_db"
		table            = "test_tbl"
	)

	// only some simple tests in this case and we can test parsing a binlog file including common header, DDL and DML in another case.

	// test CREATE/DROP table for MariaDB
	flavor := gmysql.MariaDBFlavor
	gSetStr := fmt.Sprintf("1-%d-3", serverID)
	latestGTID, err := gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)

	// ALTER TABLE
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` CHANGE COLUMN `c2` `c2` decimal(10,3)", schema, table)
	result, err := GenDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query, true, false, 0)
	require.Nil(t, err)
	require.Len(t, result.Events, 2)
	require.True(t, bytes.Contains(result.Data, []byte("ALTER TABLE")))
	require.True(t, bytes.Contains(result.Data, []byte(table)))
	require.Equal(t, latestPos+uint32(len(result.Data)), result.LatestPos)
	require.Equal(t, fmt.Sprintf("1-%d-4", serverID), result.LatestGTID.String())
}
