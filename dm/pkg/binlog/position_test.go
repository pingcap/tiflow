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
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/gtid"
)

var _ = Suite(&testPositionSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testPositionSuite struct{}

func (t *testPositionSuite) TestPositionFromStr(c *C) {
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
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(pos, DeepEquals, cs.pos)
	}
}

func (t *testPositionSuite) TestRealMySQLPos(c *C) {
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
			c.Assert(err, ErrorMatches, cs.errMsgReg)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(pos, DeepEquals, cs.expect)
	}
}

func (t *testPositionSuite) TestExtractPos(c *C) {
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
			c.Assert(err, ErrorMatches, cs.errMsgReg)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(uuidWithSuffix, Equals, cs.uuidWithSuffix)
		c.Assert(uuidSuffix, Equals, cs.uuidSuffix)
		c.Assert(realPos, DeepEquals, cs.realPos)
	}
}

func (t *testPositionSuite) TestVerifyUUIDSuffix(c *C) {
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
		c.Assert(verifyUUIDSuffix(cs.suffix), Equals, cs.valid)
	}
}

func (t *testPositionSuite) TestAdjustPosition(c *C) {
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
		adjustedPos := AdjustPosition(cs.pos)
		c.Assert(adjustedPos.Name, Equals, cs.adjustedPos.Name)
		c.Assert(adjustedPos.Pos, Equals, cs.adjustedPos.Pos)
	}
}

func (t *testPositionSuite) TestComparePosition(c *C) {
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
		c.Assert(cmp, Equals, cs.cmp)
	}
}

func (t *testPositionSuite) TestCompareCompareLocation(c *C) {
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
		c.Log(cs)
		gset1, err := gtid.ParserGTID(cs.flavor, cs.gset1)
		c.Assert(err, IsNil)
		gset2, err := gtid.ParserGTID(cs.flavor, cs.gset2)
		c.Assert(err, IsNil)

		cmpGTID := CompareLocation(Location{cs.pos1, gset1, cs.suffix1}, Location{cs.pos2, gset2, cs.suffix2}, true)
		c.Assert(cmpGTID, Equals, cs.cmpGTID)

		cmpPos := CompareLocation(Location{cs.pos1, gset1, cs.suffix1}, Location{cs.pos2, gset2, cs.suffix2}, false)
		c.Assert(cmpPos, Equals, cs.cmpPos)
	}
}

func (t *testPositionSuite) TestVerifyBinlogPos(c *C) {
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
			c.Assert(err, NotNil)
		} else {
			c.Assert(ret, DeepEquals, ca.pos)
		}
	}
}

func (t *testPositionSuite) TestSetGTID(c *C) {
	GTIDSetStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14"
	GTIDSetStr2 := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-15"
	gset, _ := gtid.ParserGTID("mysql", GTIDSetStr)
	gset2, _ := gtid.ParserGTID("mysql", GTIDSetStr2)
	mysqlSet := gset.Origin()
	mysqlSet2 := gset2.Origin()

	loc := Location{
		Position: gmysql.Position{
			Name: "mysql-bin.00002",
			Pos:  2333,
		},
		gtidSet: gset,
		Suffix:  0,
	}
	loc2 := loc

	c.Assert(CompareLocation(loc, loc2, false), Equals, 0)

	loc2.Position.Pos++
	c.Assert(loc.Position.Pos, Equals, uint32(2333))
	c.Assert(CompareLocation(loc, loc2, false), Equals, -1)

	loc2.Position.Name = "mysql-bin.00001"
	c.Assert(loc.Position.Name, Equals, "mysql-bin.00002")
	c.Assert(CompareLocation(loc, loc2, false), Equals, 1)

	// WARN: will change other location's gtid
	err := loc2.gtidSet.Set(mysqlSet2)
	c.Assert(err, IsNil)
	c.Assert(loc.gtidSet.String(), Equals, GTIDSetStr2)
	c.Assert(loc2.gtidSet.String(), Equals, GTIDSetStr2)
	c.Assert(CompareLocation(loc, loc2, true), Equals, 0)

	err = loc2.SetGTID(mysqlSet)
	c.Assert(err, IsNil)
	c.Assert(loc.gtidSet.String(), Equals, GTIDSetStr2)
	c.Assert(loc2.gtidSet.String(), Equals, GTIDSetStr)
	c.Assert(CompareLocation(loc, loc2, true), Equals, 1)
}

func (t *testPositionSuite) TestExtractSuffix(c *C) {
	testCases := []struct {
		name   string
		suffix int
	}{
		{
			"",
			MinUUIDSuffix,
		},
		{
			"mysql-bin.00005",
			MinUUIDSuffix,
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
		c.Assert(err, IsNil)
		c.Assert(suffix, Equals, tc.suffix)
	}
}

func (t *testPositionSuite) TestIsFreshPosition(c *C) {
	mysqlPos := gmysql.Position{
		Name: "mysql-binlog.00001",
		Pos:  123,
	}
	mysqlGTIDSet, err := gtid.ParserGTID(gmysql.MySQLFlavor, "e8e592a6-7a59-11eb-8da1-0242ac110002:1-36")
	c.Assert(err, IsNil)
	mariaGTIDSet, err := gtid.ParserGTID(gmysql.MariaDBFlavor, "0-1001-233")
	c.Assert(err, IsNil)
	testCases := []struct {
		loc     Location
		flavor  string
		cmpGTID bool
		fresh   bool
	}{
		{
			InitLocation(mysqlPos, mysqlGTIDSet),
			gmysql.MySQLFlavor,
			true,
			false,
		},
		{
			InitLocation(mysqlPos, gtid.MinGTIDSet(gmysql.MySQLFlavor)),
			gmysql.MySQLFlavor,
			true,
			false,
		},
		{

			InitLocation(MinPosition, mysqlGTIDSet),
			gmysql.MySQLFlavor,
			true,
			false,
		},
		{
			InitLocation(MinPosition, mysqlGTIDSet),
			gmysql.MySQLFlavor,
			false,
			true,
		},
		{
			InitLocation(MinPosition, gtid.MinGTIDSet(gmysql.MySQLFlavor)),
			gmysql.MySQLFlavor,
			true,
			true,
		},

		{
			InitLocation(mysqlPos, mariaGTIDSet),
			gmysql.MariaDBFlavor,
			true,
			false,
		},
		{
			InitLocation(mysqlPos, gtid.MinGTIDSet(gmysql.MariaDBFlavor)),
			gmysql.MariaDBFlavor,
			true,
			false,
		},
		{

			InitLocation(MinPosition, mariaGTIDSet),
			gmysql.MariaDBFlavor,
			true,
			false,
		},
		{
			InitLocation(MinPosition, mariaGTIDSet),
			gmysql.MariaDBFlavor,
			false,
			true,
		},
		{
			InitLocation(MinPosition, gtid.MinGTIDSet(gmysql.MariaDBFlavor)),
			gmysql.MariaDBFlavor,
			true,
			true,
		},
	}

	for _, tc := range testCases {
		fresh := IsFreshPosition(tc.loc, tc.flavor, tc.cmpGTID)
		c.Assert(fresh, Equals, tc.fresh)
	}
}
