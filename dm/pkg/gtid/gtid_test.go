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
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/pkg/terror"
)

var _ = Suite(&testGTIDSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testGTIDSuite struct{}

func (s *testGTIDSuite) TestGTID(c *C) {
	matserUUIDs := []string{
		"53ea0ed1-9bf8-11e6-8bea-64006a897c73",
		"53ea0ed1-9bf8-11e6-8bea-64006a897c72",
		"53ea0ed1-9bf8-11e6-8bea-64006a897c71",
	}

	cases := []struct {
		flavor        string
		masterIDs     []interface{}
		selfGTIDstr   string
		masterGTIDStr string
		exepctedStr   string
	}{
		{"mariadb", []interface{}{uint32(1)}, "1-1-1,2-2-2", "1-1-12,4-4-4", "1-1-1,4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "2-2-2", "1-2-12,2-2-3,4-4-4", "2-2-2,4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "", "1-1-12,4-4-4", "4-4-4"},
		{"mariadb", []interface{}{uint32(1)}, "2-2-2", "", ""},
		{"mariadb", []interface{}{uint32(1), uint32(3)}, "1-1-1,3-3-4,2-2-2", "1-1-12,3-3-8,4-4-4", "1-1-1,3-3-4,4-4-4"},
		{"mariadb", []interface{}{uint32(1), uint32(3)}, "2-2-2", "1-2-12,2-2-3,3-3-8,4-4-4", "2-2-2,4-4-4"},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2,%s:1-2", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-4", matserUUIDs[0], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-4", matserUUIDs[0], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-3,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-4", matserUUIDs[1], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0]}, "", fmt.Sprintf("%s:1-12,%s:1-4", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-4", matserUUIDs[1])},
		{"mysql", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), "", ""},
		{"mysql", []interface{}{matserUUIDs[0], matserUUIDs[1]}, fmt.Sprintf("%s:1-2,%s:1-2", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-4,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-2,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2])},
		{"mysql", []interface{}{matserUUIDs[0], matserUUIDs[2]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-3,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2", matserUUIDs[1])},
		{"", []interface{}{uint32(1)}, "1-1-1,2-2-2", "1-1-12,4-4-4", "1-1-1,4-4-4"},
		{"", []interface{}{uint32(1)}, "2-2-2", "1-2-12,2-2-3,4-4-4", "2-2-2,4-4-4"},
		{"", []interface{}{uint32(1), uint32(3)}, "1-1-1,3-3-4,2-2-2", "1-1-12,3-3-8,4-4-4", "1-1-1,3-3-4,4-4-4"},
		{"", []interface{}{uint32(1), uint32(3)}, "2-2-2", "1-2-12,2-2-3,3-3-8,4-4-4", "2-2-2,4-4-4"},
		{"", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2,%s:1-2", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-4", matserUUIDs[0], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-4", matserUUIDs[0], matserUUIDs[2])},
		{"", []interface{}{matserUUIDs[0]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-3,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-4", matserUUIDs[1], matserUUIDs[2])},
		{"", []interface{}{matserUUIDs[0], matserUUIDs[1]}, fmt.Sprintf("%s:1-2,%s:1-2", matserUUIDs[0], matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-4,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2,%s:1-2,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2])},
		{"", []interface{}{matserUUIDs[0], matserUUIDs[2]}, fmt.Sprintf("%s:1-2", matserUUIDs[1]), fmt.Sprintf("%s:1-12,%s:1-3,%s:1-4", matserUUIDs[0], matserUUIDs[1], matserUUIDs[2]), fmt.Sprintf("%s:1-2", matserUUIDs[1])},
	}

	for _, cs := range cases {
		selfGTIDSet, err := ParserGTID(cs.flavor, cs.selfGTIDstr)
		c.Assert(err, IsNil)
		newGTIDSet, err := ParserGTID(cs.flavor, cs.masterGTIDStr)
		c.Assert(err, IsNil)
		excepted, err := ParserGTID(cs.flavor, cs.exepctedStr)
		c.Assert(err, IsNil)

		err = selfGTIDSet.Replace(newGTIDSet, cs.masterIDs)
		c.Logf("%s %s %s", selfGTIDSet, newGTIDSet, excepted)
		c.Assert(err, IsNil)
		c.Assert(selfGTIDSet.Origin().Equal(excepted.Origin()), IsTrue)
		c.Assert(newGTIDSet.Origin().Equal(excepted.Origin()), IsTrue)
	}
}

func (s *testGTIDSuite) TestSortingGTIDSet(c *C) {
	// check mysql
	gSet, err := ParserGTID(mysql.MySQLFlavor, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423")
	c.Assert(err, IsNil)
	sortedGTIDSet := "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454,3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190"
	c.Assert(gSet.String(), Equals, sortedGTIDSet)
	// check mariadb
	gSet, err = ParserGTID(mysql.MariaDBFlavor, "0-0-1,1-1-1,4-20-1,3-1-1,10-10-10")
	c.Assert(err, IsNil)
	sortedGTIDSet = "0-0-1,1-1-1,10-10-10,3-1-1,4-20-1"
	c.Assert(gSet.String(), Equals, sortedGTIDSet)
}

func TestIsNilGTIDSet(t *testing.T) {
	require.False(t, IsNilMySQLGTIDSet(""))
	require.False(t, IsNilMySQLGTIDSet("xxxxx"))
	require.False(t, IsNilMySQLGTIDSet("xxxxx:0,yyyy:0"))
	require.False(t, IsNilMySQLGTIDSet("xxxxx:1-2"))
	require.False(t, IsNilMySQLGTIDSet("xxxxx:0-0"))
	require.True(t, IsNilMySQLGTIDSet("xxxxx:0"))
	require.True(t, IsNilMySQLGTIDSet(" xxxxx:0 "))
}

func (s *testGTIDSuite) TestMinGTIDSet(c *C) {
	gset := MinGTIDSet(mysql.MySQLFlavor)
	_, ok := gset.(*MySQLGTIDSet)
	c.Assert(ok, IsTrue)

	gset = MinGTIDSet(mysql.MariaDBFlavor)
	_, ok = gset.(*MariadbGTIDSet)
	c.Assert(ok, IsTrue)

	// will treat as mysql gtid set
	gset = MinGTIDSet("wrong flavor")
	_, ok = gset.(*MySQLGTIDSet)
	c.Assert(ok, IsTrue)
}

func (s *testGTIDSuite) TestMySQLGTIDEqual(c *C) {
	var (
		g1     *MySQLGTIDSet
		g2     *MySQLGTIDSet
		gMaria *MariadbGTIDSet
	)

	c.Assert(g1.Equal(nil), IsTrue)
	c.Assert(g1.Equal(g2), IsTrue)
	c.Assert(g1.Equal(gMaria), IsFalse)

	gSet, err := ParserGTID("mysql", "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454")
	c.Assert(err, IsNil)
	g1 = gSet.(*MySQLGTIDSet)
	c.Assert(g1.Equal(g2), IsFalse)

	gSet, err = ParserGTID("mysql", "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454,3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190")
	c.Assert(err, IsNil)
	g2 = gSet.(*MySQLGTIDSet)
	c.Assert(g1.Equal(g2), IsTrue)
}

func (s *testGTIDSuite) TestMariaGTIDEqual(c *C) {
	var (
		g1     *MariadbGTIDSet
		g2     *MariadbGTIDSet
		gMySQL *MySQLGTIDSet
	)

	c.Assert(g1.Equal(nil), IsTrue)
	c.Assert(g1.Equal(g2), IsTrue)
	c.Assert(g1.Equal(gMySQL), IsFalse)

	gSet, err := ParserGTID("mariadb", "1-1-1,2-2-2")
	c.Assert(err, IsNil)
	g1 = gSet.(*MariadbGTIDSet)
	c.Assert(g1.Equal(g2), IsFalse)

	gSet, err = ParserGTID("mariadb", "2-2-2,1-1-1")
	c.Assert(err, IsNil)
	g2 = gSet.(*MariadbGTIDSet)
	c.Assert(g1.Equal(g2), IsTrue)
}

// nolint:dupl
func (s *testGTIDSuite) TestMySQLGTIDContain(c *C) {
	var (
		g1     *MySQLGTIDSet
		g2     *MySQLGTIDSet
		gMaria *MariadbGTIDSet
	)
	c.Assert(g1.Contain(g2), IsTrue)      // all nil
	c.Assert(g1.Contain(gMaria), IsFalse) // incompatible

	// one nil
	gSet, err := ParserGTID("mysql", "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-10,406a3f61-690d-11e7-87c5-6c92bf46f384:1-10")
	c.Assert(err, IsNil)
	g1 = gSet.(*MySQLGTIDSet)
	c.Assert(g1.Contain(g2), IsTrue)
	c.Assert(g2.Contain(g1), IsFalse)

	// contain
	gSet, err = ParserGTID("mysql", "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-5,406a3f61-690d-11e7-87c5-6c92bf46f384:1-10")
	c.Assert(err, IsNil)
	g2 = gSet.(*MySQLGTIDSet)
	c.Assert(g1.Contain(g2), IsTrue)
	c.Assert(g2.Contain(g1), IsFalse)

	// not contain
	gSet, err = ParserGTID("mysql", "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-5,406a3f61-690d-11e7-87c5-6c92bf46f384:1-10")
	c.Assert(err, IsNil)
	g2 = gSet.(*MySQLGTIDSet)
	c.Assert(g1.Contain(g2), IsFalse)
	c.Assert(g2.Contain(g1), IsFalse)
}

// nolint:dupl
func (s *testGTIDSuite) TestMairaGTIDContain(c *C) {
	var (
		g1     *MariadbGTIDSet
		g2     *MariadbGTIDSet
		gMySQL *MySQLGTIDSet
	)
	c.Assert(g1.Contain(g2), IsTrue)      // all nil
	c.Assert(g1.Contain(gMySQL), IsFalse) // incompatible

	// one nil
	gSet, err := ParserGTID("mariadb", "1-1-1,2-2-2")
	c.Assert(err, IsNil)
	g1 = gSet.(*MariadbGTIDSet)
	c.Assert(g1.Contain(g2), IsTrue)
	c.Assert(g2.Contain(g1), IsFalse)

	// contain
	gSet, err = ParserGTID("mariadb", "1-1-1,2-2-1")
	c.Assert(err, IsNil)
	g2 = gSet.(*MariadbGTIDSet)
	c.Assert(g1.Contain(g2), IsTrue)
	c.Assert(g2.Contain(g1), IsFalse)

	// not contain
	gSet, err = ParserGTID("mariadb", "1-1-2,2-2-1")
	c.Assert(err, IsNil)
	g2 = gSet.(*MariadbGTIDSet)
	c.Assert(g1.Contain(g2), IsFalse)
	c.Assert(g2.Contain(g1), IsFalse)
}

func (s *testGTIDSuite) TestMySQLGTIDTruncate(c *C) {
	var (
		flavor      = "mysql"
		g1, _       = ParserGTID(flavor, "00c04543-f584-11e9-a765-0242ac120002:100")
		g2, _       = ParserGTID(flavor, "00c04543-f584-11e9-a765-0242ac120002:100")
		gNil        *MySQLGTIDSet
		gEmpty, _   = ParserGTID(flavor, "")
		gMariaDBNil *MariadbGTIDSet
	)
	// truncate to nil or empty GTID sets has no effect
	c.Assert(g1.Truncate(nil), IsNil)
	c.Assert(g1, DeepEquals, g2)
	c.Assert(g1.Truncate(gNil), IsNil)
	c.Assert(g1, DeepEquals, g2)
	c.Assert(g1.Truncate(gEmpty), IsNil)
	c.Assert(g1, DeepEquals, g2)

	// nil truncate to nil has no effect
	c.Assert(gNil.Truncate(nil), IsNil)
	c.Assert(gNil.Truncate(gNil), IsNil)

	// nil truncate to not nil report an error
	c.Assert(terror.ErrGTIDTruncateInvalid.Equal(gNil.Truncate(g1)), IsTrue)

	// truncate with invalid MySQL GTID sets report an error
	c.Assert(terror.ErrGTIDTruncateInvalid.Equal(g1.Truncate(gMariaDBNil)), IsTrue)

	cases := []struct {
		before   string
		end      string
		after    string
		hasError bool
	}{
		// before not contain end
		{
			before:   "00c04543-f584-11e9-a765-0242ac120002:100",
			end:      "00c04543-f584-11e9-a765-0242ac120002:99",
			hasError: true,
		},
		{
			before:   "00c04543-f584-11e9-a765-0242ac120002:40-60",
			end:      "00c04543-f584-11e9-a765-0242ac120002:50-70",
			hasError: true,
		},
		{
			before:   "00c04543-f584-11e9-a765-0242ac120002:40-60",
			end:      "00c04543-f584-11e9-a765-0242ac120002:30-50",
			hasError: true,
		},
		// truncate take effect
		{
			before: "00c04543-f584-11e9-a765-0242ac120002:100",
			end:    "00c04543-f584-11e9-a765-0242ac120002:100",
			after:  "00c04543-f584-11e9-a765-0242ac120002:100",
		},
		{
			before: "00c04543-f584-11e9-a765-0242ac120002:40-60",
			end:    "00c04543-f584-11e9-a765-0242ac120002:45-55",
			after:  "00c04543-f584-11e9-a765-0242ac120002:40-55",
		},
		{
			before: "00c04543-f584-11e9-a765-0242ac120002:40-60:70:80-100",
			end:    "00c04543-f584-11e9-a765-0242ac120002:45-55:85-95",
			after:  "00c04543-f584-11e9-a765-0242ac120002:40-55:70:80-95",
		},
		{
			before: "00c04543-f584-11e9-a765-0242ac120002:40-60:70:80-100",
			end:    "00c04543-f584-11e9-a765-0242ac120002:45-55:70:85-95",
			after:  "00c04543-f584-11e9-a765-0242ac120002:40-55:70:80-95",
		},
		{
			before: "00c04543-f584-11e9-a765-0242ac120002:40-60,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-100",
			end:    "00c04543-f584-11e9-a765-0242ac120002:45-55",
			after:  "00c04543-f584-11e9-a765-0242ac120002:40-55,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-100",
		},
		{
			before: "00c04543-f584-11e9-a765-0242ac120002:40-60,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-100",
			end:    "00c04543-f584-11e9-a765-0242ac120002:45-55,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-80",
			after:  "00c04543-f584-11e9-a765-0242ac120002:40-55,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-80",
		},
	}

	for _, cs := range cases {
		bg, err := ParserGTID(flavor, cs.before)
		c.Assert(err, IsNil)
		eg, err := ParserGTID(flavor, cs.end)
		c.Assert(err, IsNil)
		ag, err := ParserGTID(flavor, cs.after)
		c.Assert(err, IsNil)
		err = bg.Truncate(eg)
		if cs.hasError {
			c.Assert(terror.ErrGTIDTruncateInvalid.Equal(err), IsTrue)
		} else {
			c.Assert(bg, DeepEquals, ag)
		}
	}
}

func (s *testGTIDSuite) TestMariaDBGTIDTruncate(c *C) {
	var (
		flavor    = "mariadb"
		g1, _     = ParserGTID(flavor, "1-2-3")
		g2, _     = ParserGTID(flavor, "1-2-3")
		gNil      *MariadbGTIDSet
		gEmpty, _ = ParserGTID(flavor, "")
		gMySQLNil *MySQLGTIDSet
	)
	// truncate to nil or empty GTID sets has no effect
	c.Assert(g1.Truncate(nil), IsNil)
	c.Assert(g1, DeepEquals, g2)
	c.Assert(g1.Truncate(gNil), IsNil)
	c.Assert(g1, DeepEquals, g2)
	c.Assert(g1.Truncate(gEmpty), IsNil)
	c.Assert(g1, DeepEquals, g2)

	// nil truncate to nil has no effect
	c.Assert(gNil.Truncate(nil), IsNil)
	c.Assert(gNil.Truncate(gNil), IsNil)

	// nil truncate to not nil report an error
	c.Assert(terror.ErrGTIDTruncateInvalid.Equal(gNil.Truncate(g1)), IsTrue)

	// truncate with invalid MariaDB GTID sets report an error
	c.Assert(terror.ErrGTIDTruncateInvalid.Equal(g1.Truncate(gMySQLNil)), IsTrue)

	cases := []struct {
		before   string
		end      string
		after    string
		hasError bool
	}{
		// before not contain end
		{
			before:   "1-2-3",
			end:      "2-2-3",
			hasError: true,
		},
		{
			before:   "1-2-3",
			end:      "1-2-4",
			hasError: true,
		},

		// truncate take effect
		{
			before: "1-2-3",
			end:    "1-2-3",
			after:  "1-2-3",
		},
		{
			before: "1-2-10",
			end:    "1-2-8",
			after:  "1-2-8",
		},
		{
			before: "1-2-10",
			end:    "1-3-8",
			after:  "1-3-8",
		},
		{
			before: "1-2-10,2-2-10",
			end:    "1-2-8",
			after:  "1-2-8,2-2-10",
		},
		{
			before: "1-2-10,2-2-10",
			end:    "1-3-8,2-2-6",
			after:  "1-3-8,2-2-6",
		},
	}

	for _, cs := range cases {
		bg, err := ParserGTID(flavor, cs.before)
		c.Assert(err, IsNil)
		eg, err := ParserGTID(flavor, cs.end)
		c.Assert(err, IsNil)
		ag, err := ParserGTID(flavor, cs.after)
		c.Assert(err, IsNil)
		err = bg.Truncate(eg)
		if cs.hasError {
			c.Assert(terror.ErrGTIDTruncateInvalid.Equal(err), IsTrue)
		} else {
			c.Assert(bg, DeepEquals, ag)
		}
	}
}
