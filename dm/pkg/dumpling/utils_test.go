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

package dumpling

import (
	"os"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

var _ = Suite(&testSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

func (t *testSuite) TestParseMetaData(c *C) {
	f, err := os.CreateTemp("", "metadata")
	c.Assert(err, IsNil)
	defer os.Remove(f.Name())

	testCases := []struct {
		source   string
		pos      mysql.Position
		gsetStr  string
		loc2     bool
		pos2     mysql.Position
		gsetStr2 string
	}{
		{
			`Started dump at: 2018-12-28 07:20:49
SHOW MASTER STATUS:
        Log: bin.000001
        Pos: 2479
        GTID:97b5142f-e19c-11e8-808c-0242ac110005:1-13

Finished dump at: 2018-12-28 07:20:51`,
			mysql.Position{
				Name: "bin.000001",
				Pos:  2479,
			},
			"97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			false,
			mysql.Position{},
			"",
		},
		{
			`Started dump at: 2018-12-27 19:51:22
SHOW MASTER STATUS:
        Log: mysql-bin.000003
        Pos: 3295817
        GTID:

SHOW SLAVE STATUS:
        Host: 10.128.27.98
        Log: mysql-bin.000003
        Pos: 329635
        GTID:

Finished dump at: 2018-12-27 19:51:22`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  3295817,
			},
			"",
			false,
			mysql.Position{},
			"",
		},
		{ // with empty line after multiple GTID sets
			`Started dump at: 2020-05-21 18:14:49
SHOW MASTER STATUS:
	Log: mysql-bin.000003
	Pos: 1274
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW SLAVE STATUS:
	Host: 192.168.100.100
	Log: mysql-bin.000003
	Pos: 700
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

Finished dump at: 2020-05-21 18:14:49`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1274,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			false,
			mysql.Position{},
			"",
		},
		{ // without empty line after mutlple GTID sets
			`Started dump at: 2020-05-21 18:02:33
SHOW MASTER STATUS:
		Log: mysql-bin.000003
		Pos: 1274
		GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13
Finished dump at: 2020-05-21 18:02:44`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1274,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			false,
			mysql.Position{},
			"",
		},
		{ // with empty line after multiple GTID sets
			`Started dump at: 2020-05-21 18:14:49
SHOW MASTER STATUS:
	Log: mysql-bin.000003
	Pos: 1274
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW SLAVE STATUS:
	Host: 192.168.100.100
	Log: mysql-bin.000003
	Pos: 700
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */
	Log: mysql-bin.000003
	Pos: 1280
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-14

Finished dump at: 2020-05-21 18:14:49`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1274,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			true,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1280,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-14",
		},
		{ // with empty line after multiple GTID sets
			`Started dump at: 2020-05-21 18:14:49
SHOW MASTER STATUS:
	Log: mysql-bin.000003
	Pos: 1274
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW SLAVE STATUS:
	Host: 192.168.100.100
	Log: mysql-bin.000003
	Pos: 700
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */
	Log: mysql-bin.000004
	Pos: 4
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-9,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

Finished dump at: 2020-05-21 18:14:49`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1274,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			true,
			mysql.Position{
				Name: "mysql-bin.000004",
				Pos:  4,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-9,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
		},
		{ // no GTID sets
			`Started dump at: 2020-09-30 12:16:49
SHOW MASTER STATUS:
	Log: mysql-bin-changelog.000003
	Pos: 12470000

SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */
	Log: mysql-bin-changelog.000003
	Pos: 12470000

Finished dump at: 2020-09-30 12:16:49
`,
			mysql.Position{
				Name: "mysql-bin-changelog.000003",
				Pos:  12470000,
			},
			"",
			true,
			mysql.Position{
				Name: "mysql-bin-changelog.000003",
				Pos:  12470000,
			},
			"",
		},
	}

	for _, tc := range testCases {
		err2 := os.WriteFile(f.Name(), []byte(tc.source), 0o644)
		c.Assert(err2, IsNil)
		loc, loc2, err2 := ParseMetaData(f.Name(), "mysql")
		c.Assert(err2, IsNil)
		c.Assert(loc.Position, DeepEquals, tc.pos)
		gs, _ := gtid.ParserGTID("mysql", tc.gsetStr)
		c.Assert(loc.GetGTID(), DeepEquals, gs)
		if tc.loc2 {
			c.Assert(loc2.Position, DeepEquals, tc.pos2)
			gs2, _ := gtid.ParserGTID("mysql", tc.gsetStr2)
			c.Assert(loc2.GetGTID(), DeepEquals, gs2)
		} else {
			c.Assert(loc2, IsNil)
		}
	}

	noBinlogLoc := `Started dump at: 2020-12-02 17:13:56
Finished dump at: 2020-12-02 17:13:56
`
	err = os.WriteFile(f.Name(), []byte(noBinlogLoc), 0o644)
	c.Assert(err, IsNil)
	_, _, err = ParseMetaData(f.Name(), "mysql")
	c.Assert(terror.ErrMetadataNoBinlogLoc.Equal(err), IsTrue)
}
