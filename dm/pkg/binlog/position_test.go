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

package binlog

import (
	"testing"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/stretchr/testify/require"
)

func TestPositionFromStr(t *testing.T) {
	t.Parallel()
	emptyPos := gmysql.Position{}
	cases := []struct {
		str      string
		pos      gmysql.Position
		hasError bool
	}{
		{
			str:      "mysql-bin.000001",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "234",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:abc",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:234:567",
			pos:      emptyPos,
			hasError: true,
		},
		{
			str:      "mysql-bin.000001:234",
			pos:      gmysql.Position{Name: "mysql-bin.000001", Pos: 234},
			hasError: false,
		},
	}

	for _, cs := range cases {
		pos, err := PositionFromStr(cs.str)
		if cs.hasError {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
		}
		require.Equal(t, cs.pos, pos)
	}
}

func TestRealMySQLPos(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pos       gmysql.Position
		expect    gmysql.Position
		errMsgReg string
	}{
		{
			pos:    gmysql.Position{Name: "mysql-bin.000001", Pos: 154},
			expect: gmysql.Position{Name: "mysql-bin.000001", Pos: 154},
		},
		{
			pos:    gmysql.Position{Name: "mysql-bin|000002.000003", Pos: 154},
			expect: gmysql.Position{Name: "mysql-bin.000003", Pos: 154},
		},
		{
			pos:       gmysql.Position{Name: "", Pos: 154},
			expect:    gmysql.Position{Name: "", Pos: 154},
			errMsgReg: ".*invalid binlog filename.*",
		},
		{
			pos:    gmysql.Position{Name: "mysql|bin|000002.000003", Pos: 154},
			expect: gmysql.Position{Name: "mysql|bin.000003", Pos: 154},
		},
		{
			pos:    gmysql.Position{Name: "mysql-bin|invalid-suffix.000003", Pos: 154},
			expect: gmysql.Position{Name: "mysql-bin|invalid-suffix.000003", Pos: 154},
		},
	}

	for _, cs := range cases {
		pos, err := RealMySQLPos(cs.pos)
		if len(cs.errMsgReg) > 0 {
			require.Regexp(t, cs.errMsgReg, err)
		} else {
			require.Nil(t, err)
		}
		require.Equal(t, cs.expect, pos)
	}
}

func TestExtractPos(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pos            gmysql.Position
		uuids          []string
		uuidWithSuffix string
		uuidSuffix     string
		realPos        gmysql.Position
		errMsgReg      string
	}{
		{
			// empty UUIDs
			pos:       gmysql.Position{Name: "mysql-bin.000001", Pos: 666},
			errMsgReg: ".*empty UUIDs.*",
		},
		{
			// invalid UUID in UUIDs
			pos:       gmysql.Position{Name: "mysql-bin.000002", Pos: 666},
			uuids:     []string{"invalid-uuid"},
			errMsgReg: ".*not valid.*",
		},
		{
			// real pos
			pos:            gmysql.Position{Name: "mysql-bin.000003", Pos: 666},
			uuids:          []string{"server-a-uuid.000001", "server-b-uuid.000002", "server-c-uuid.000003"},
			uuidWithSuffix: "server-c-uuid.000003", // use the latest
			uuidSuffix:     "000003",
			realPos:        gmysql.Position{Name: "mysql-bin.000003", Pos: 666},
		},
		{
			// pos match one of UUIDs
			pos:            gmysql.Position{Name: "mysql-bin|000002.000004", Pos: 666},
			uuids:          []string{"server-a-uuid.000001", "server-b-uuid.000002", "server-c-uuid.000003"},
			uuidWithSuffix: "server-b-uuid.000002", // use the latest
			uuidSuffix:     "000002",
			realPos:        gmysql.Position{Name: "mysql-bin.000004", Pos: 666},
		},
		{
			// pos not match one of UUIDs
			pos:       gmysql.Position{Name: "mysql-bin|000111.000005", Pos: 666},
			uuids:     []string{"server-a-uuid.000001", "server-b-uuid.000002", "server-c-uuid.000003"},
			errMsgReg: ".*UUID suffix.*with UUIDs.*",
		},
		{
			// multi `|` exist
			pos:            gmysql.Position{Name: "mysql|bin|000002.000006", Pos: 666},
			uuids:          []string{"server-a-uuid.000001", "server-b-uuid.000002", "server-c-uuid.000003"},
			uuidWithSuffix: "server-b-uuid.000002",
			uuidSuffix:     "000002",
			realPos:        gmysql.Position{Name: "mysql|bin.000006", Pos: 666},
		},
		{
			// invalid UUID suffix
			pos:       gmysql.Position{Name: "mysql-bin|abcdef.000007", Pos: 666},
			uuids:     []string{"server-a-uuid.000001", "server-b-uuid.000002", "server-c-uuid.000003"},
			errMsgReg: ".*invalid UUID suffix.*",
		},
	}

	for _, cs := range cases {
		uuidWithSuffix, uuidSuffix, realPos, err := ExtractPos(cs.pos, cs.uuids)
		if len(cs.errMsgReg) > 0 {
			require.Regexp(t, cs.errMsgReg, err)
		} else {
			require.Nil(t, err)
		}
		require.Equal(t, cs.uuidWithSuffix, uuidWithSuffix)
		require.Equal(t, cs.uuidSuffix, uuidSuffix)
		require.Equal(t, cs.realPos, realPos)
	}
}

func TestVerifyUUIDSuffix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		suffix string
		valid  bool
	}{
		{
			suffix: "000666",
			valid:  true,
		},
		{
			suffix: "666888",
			valid:  true,
		},
		{
			// == 0
			suffix: "000000",
		},
		{
			// < 0
			suffix: "-123456",
		},
		{
			// float
			suffix: "123.456",
		},
		{
			// empty
			suffix: "",
		},
		{
			suffix: "abc",
		},
		{
			suffix: "abc666",
		},
		{
			suffix: "666abc",
		},
	}

	for _, cs := range cases {
		require.Equal(t, cs.valid, verifyRelaySubDirSuffix(cs.suffix))
	}
}

func TestAdjustPosition(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pos         gmysql.Position
		adjustedPos gmysql.Position
	}{
		{
			gmysql.Position{
				Name: "mysql-bin.00001",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin.00001",
				Pos:  123,
			},
		}, {
			gmysql.Position{
				Name: "mysql-bin|00001.00002",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin.00002",
				Pos:  123,
			},
		}, {
			gmysql.Position{
				Name: "mysql-bin|00001.00002.00003",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin|00001.00002.00003",
				Pos:  123,
			},
		},
	}

	for _, cs := range cases {
		adjustedPos := RemoveRelaySubDirSuffix(cs.pos)
		require.Equal(t, cs.adjustedPos.Name, adjustedPos.Name)
		require.Equal(t, cs.adjustedPos.Pos, adjustedPos.Pos)
	}
}

func TestComparePosition(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pos1 gmysql.Position
		pos2 gmysql.Position
		cmp  int
	}{
		{
			gmysql.Position{
				Name: "mysql-bin.00001",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin.00002",
				Pos:  123,
			},
			-1,
		}, {
			gmysql.Position{
				Name: "mysql-bin.00001",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin.00001",
				Pos:  123,
			},
			0,
		}, {
			gmysql.Position{
				Name: "mysql-bin.00002",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin.00001",
				Pos:  123,
			},
			1,
		}, {
			gmysql.Position{
				Name: "mysql-bin|00001.00002",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin|00002.00001",
				Pos:  123,
			},
			-1,
		}, {
			gmysql.Position{
				Name: "mysql-bin|00001.00002",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin|00001.00002",
				Pos:  123,
			},
			0,
		}, {
			gmysql.Position{
				Name: "mysql-bin|00002.00001",
				Pos:  123,
			},
			gmysql.Position{
				Name: "mysql-bin|00001.00002",
				Pos:  123,
			},
			1,
		},
	}

	for _, cs := range cases {
		cmp := ComparePosition(cs.pos1, cs.pos2)
		require.Equal(t, cs.cmp, cmp)
	}
}

func TestCompareCompareLocation(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		flavor  string
		pos1    gmysql.Position
		gset1   string
		suffix1 int
		pos2    gmysql.Position
		gset2   string
		suffix2 int
		cmpGTID int
		cmpPos  int
	}{
		{
			// both gset1 and gset2 is nil, gset1 = gset2, pos1 = pos2
			gmysql.MySQLFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"",
			0,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"",
			0,
			0,
			0,
		}, {
			// both gset1 and gset2 is nil, gset1 = gset2, pos1 = pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"",
			0,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"",
			0,
			0,
			0,
		}, {
			// both gset1 and gset2 is nil, gset1 = gset2, pos1 < pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  122,
			},
			"",
			0,
			0,
			-1,
		}, {
			// pos1 > pos2, gset is nil
			gmysql.MySQLFlavor,
			gmysql.Position{
				Name: "binlog.00003",
				Pos:  123,
			},
			"",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  122,
			},
			"",
			0,
			0,
			1,
		}, {
			// gset1 = gset2, pos1 < pos2
			gmysql.MySQLFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73:1-4",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  122,
			},
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73:1-4",
			0,
			0,
			-1,
		}, {
			// gset1 < gset2, pos1 < pos2
			gmysql.MySQLFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73:1-2,53ea0ed1-9bf8-11e6-8bea-64006a897c74:1-2",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73:1-4,53ea0ed1-9bf8-11e6-8bea-64006a897c74:1-3",
			0,
			-1,
			-1,
		}, {
			// gset1 > gset2, pos1 < pos1
			gmysql.MySQLFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73:1-2,53ea0ed1-9bf8-11e6-8bea-64006a897c74:1-3",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73:1-2",
			0,
			1,
			-1,
		}, {
			// can't compare by gtid set, pos1 < pos2
			gmysql.MySQLFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73:1-2,53ea0ed1-9bf8-11e6-8bea-64006a897c74:2-4",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"53ea0ed1-9bf8-11e6-8bea-64006a897c73:1-2,53ea0ed1-9bf8-11e6-8bea-64006a897c74:1-3",
			0,
			-1,
			-1,
		}, {
			// gset1 = gset2, pos1 < pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  122,
			},
			"1-1-1,2-2-2",
			0,
			0,
			-1,
		}, {
			// gset1 < gset2, pos1 < pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"1-1-1,2-2-2,3-3-3",
			0,
			-1,
			-1,
		}, {
			// gset1 > gset2, pos1 < pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-3",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"1-1-1,2-2-2",
			0,
			1,
			-1,
		}, {
			// can't compare by gtid set, pos1 < pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"2-2-2,3-3-3",
			0,
			-1,
			-1,
		}, {
			// gset1 is nil < gset2, pos1 < pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"2-2-2,3-3-3",
			0,
			-1,
			-1,
		}, {
			// both gset1 and gset2 is nil, gset1 = gset2, pos1 < pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"",
			0,
			0,
			-1,
		}, {
			// both gset1 and gset2 is nil, gset1 = gset2, pos1 < pos2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"",
			0,
			gmysql.Position{
				Name: "binlog.00002",
				Pos:  124,
			},
			"",
			0,
			0,
			-1,
		}, {
			// gset1 = gset2, pos1 = pos2, suffix1 < suffix2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			0,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			1,
			-1,
			-1,
		}, {
			// gset1 = gset2, pos1 = pos2, suffix1 = suffix2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			1,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			1,
			0,
			0,
		}, {
			// gset1 = gset2, pos1 = pos2, suffix1 > suffix2
			gmysql.MariaDBFlavor,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			2,
			gmysql.Position{
				Name: "binlog.00001",
				Pos:  123,
			},
			"1-1-1,2-2-2",
			1,
			1,
			1,
		},
	}

	for _, cs := range testCases {
		t.Log(cs)
		gset1, err := gtid.ParserGTID(cs.flavor, cs.gset1)
		require.Nil(t, err)
		gset2, err := gtid.ParserGTID(cs.flavor, cs.gset2)
		require.Nil(t, err)

		cmpGTID := CompareLocation(Location{cs.pos1, gset1, cs.suffix1}, Location{cs.pos2, gset2, cs.suffix2}, true)
		require.Equal(t, cs.cmpGTID, cmpGTID)

		cmpPos := CompareLocation(Location{cs.pos1, gset1, cs.suffix1}, Location{cs.pos2, gset2, cs.suffix2}, false)
		require.Equal(t, cs.cmpPos, cmpPos)
	}
}

func TestVerifyBinlogPos(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input  string
		hasErr bool
		pos    *gmysql.Position
	}{
		{
			`"mysql-bin.000001:2345"`,
			false,
			&gmysql.Position{Name: "mysql-bin.000001", Pos: 2345},
		},
		{
			`mysql-bin.000001:2345`,
			false,
			&gmysql.Position{Name: "mysql-bin.000001", Pos: 2345},
		},
		{
			`"mysql-bin.000001"`,
			true,
			nil,
		},
		{
			`mysql-bin.000001`,
			true,
			nil,
		},
	}

	for _, ca := range cases {
		ret, err := VerifyBinlogPos(ca.input)
		if ca.hasErr {
			require.NotNil(t, err)
		} else {
			require.Equal(t, ca.pos, ret)
		}
	}
}

func TestSetGTID(t *testing.T) {
	t.Parallel()
	GTIDSetStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14"
	GTIDSetStr2 := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-15"
	gset, _ := gtid.ParserGTID("mysql", GTIDSetStr)
	gset2, _ := gtid.ParserGTID("mysql", GTIDSetStr2)
	mysqlSet := gset
	mysqlSet2 := gset2

	loc := Location{
		Position: gmysql.Position{
			Name: "mysql-bin.00002",
			Pos:  2333,
		},
		gtidSet: gset,
		Suffix:  0,
	}
	loc2 := loc

	require.Equal(t, 0, CompareLocation(loc, loc2, false))
	loc2.Position.Pos++
	require.Equal(t, uint32(2333), loc.Position.Pos)
	require.Equal(t, -1, CompareLocation(loc, loc2, false))

	loc2.Position.Name = "mysql-bin.00001"
	require.Equal(t, "mysql-bin.00002", loc.Position.Name)
	require.Equal(t, 1, CompareLocation(loc, loc2, false))

	// will not change other location's gtid
	loc2.gtidSet = mysqlSet2
	require.NotEqual(t, GTIDSetStr2, loc.gtidSet.String())
	require.Equal(t, GTIDSetStr2, loc2.gtidSet.String())

	err := loc2.SetGTID(mysqlSet2)
	require.Nil(t, err)
	require.Equal(t, GTIDSetStr, loc.gtidSet.String())
	require.Equal(t, GTIDSetStr2, loc2.gtidSet.String())
	require.Equal(t, -1, CompareLocation(loc, loc2, true))

	loc2.gtidSet = nil
	err = loc2.SetGTID(mysqlSet)
	require.Nil(t, err)
	require.Equal(t, GTIDSetStr, loc2.gtidSet.String())
}

func TestSetGTIDMariaDB(t *testing.T) {
	t.Parallel()
	gSetStr := "1-1-1,2-2-2"
	gSet, err := gtid.ParserGTID("mariadb", gSetStr)
	require.Nil(t, err)
	gSetOrigin := gSet

	loc := Location{
		Position: gmysql.Position{
			Name: "mysql-bin.00002",
			Pos:  2333,
		},
		gtidSet: nil,
		Suffix:  0,
	}
	err = loc.SetGTID(gSetOrigin)
	require.Nil(t, err)
	require.Equal(t, gSetStr, loc.gtidSet.String())
}

func TestExtractSuffix(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		suffix int
	}{
		{
			"",
			MinRelaySubDirSuffix,
		},
		{
			"mysql-bin.00005",
			MinRelaySubDirSuffix,
		},
		{
			"mysql-bin|000001.000001",
			1,
		},
		{
			"mysql-bin|000005.000004",
			5,
		},
	}

	for _, tc := range testCases {
		suffix, err := ExtractSuffix(tc.name)
		require.Nil(t, err)
		require.Equal(t, tc.suffix, suffix)
	}
}

func TestIsFreshPosition(t *testing.T) {
	t.Parallel()
	mysqlPos := gmysql.Position{
		Name: "mysql-binlog.00001",
		Pos:  123,
	}
	mysqlGTIDSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, "e8e592a6-7a59-11eb-8da1-0242ac110002:1-36")
	require.Nil(t, err)
	mariaGTIDSet, err := gtid.ParserGTID(gmysql.MariaDBFlavor, "0-1001-233")
	require.Nil(t, err)
	testCases := []struct {
		loc     Location
		flavor  string
		cmpGTID bool
		fresh   bool
	}{
		{
			NewLocation(mysqlPos, mysqlGTIDSet),
			gmysql.MySQLFlavor,
			true,
			false,
		},
		{
			NewLocation(mysqlPos, gtid.MustZeroGTIDSet(gmysql.MySQLFlavor)),
			gmysql.MySQLFlavor,
			true,
			false,
		},
		{
			NewLocation(MinPosition, mysqlGTIDSet),
			gmysql.MySQLFlavor,
			true,
			false,
		},
		{
			NewLocation(MinPosition, mysqlGTIDSet),
			gmysql.MySQLFlavor,
			false,
			true,
		},
		{
			NewLocation(MinPosition, gtid.MustZeroGTIDSet(gmysql.MySQLFlavor)),
			gmysql.MySQLFlavor,
			true,
			true,
		},

		{
			NewLocation(mysqlPos, mariaGTIDSet),
			gmysql.MariaDBFlavor,
			true,
			false,
		},
		{
			NewLocation(mysqlPos, gtid.MustZeroGTIDSet(gmysql.MariaDBFlavor)),
			gmysql.MariaDBFlavor,
			true,
			false,
		},
		{
			NewLocation(MinPosition, mariaGTIDSet),
			gmysql.MariaDBFlavor,
			true,
			false,
		},
		{
			NewLocation(MinPosition, mariaGTIDSet),
			gmysql.MariaDBFlavor,
			false,
			true,
		},
		{
			NewLocation(MinPosition, gtid.MustZeroGTIDSet(gmysql.MariaDBFlavor)),
			gmysql.MariaDBFlavor,
			true,
			true,
		},
	}

	for _, tc := range testCases {
		fresh := IsFreshPosition(tc.loc, tc.flavor, tc.cmpGTID)
		require.Equal(t, tc.fresh, fresh)
	}
}
