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

package relay

import (
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/pkg/gtid"
)

var _ = Suite(&testMetaSuite{})

type testMetaSuite struct{}

type MetaTestCase struct {
	uuid           string
	uuidWithSuffix string
	pos            mysql.Position
	gset           gtid.Set
}

func (r *testMetaSuite) TestLocalMeta(c *C) {
	dir, err := os.MkdirTemp("", "test_local_meta")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)

	gset0, _ := gtid.ParserGTID("mysql", "")
	gset1, _ := gtid.ParserGTID("mysql", "85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-12")
	gset2, _ := gtid.ParserGTID("mysql", "85ab69d1-b21f-11e6-9c5e-64006a8978d2:13-23")
	gset3, _ := gtid.ParserGTID("mysql", "85ab69d1-b21f-11e6-9c5e-64006a8978d2:24-33")
	gset4, _ := gtid.ParserGTID("mysql", "85ab69d1-b21f-11e6-9c5e-64006a8978d2:34-46")
	gset5, _ := gtid.ParserGTID("mysql", "85ab69d1-b21f-11e6-9c5e-64006a8978d2:45-56")

	cases := []MetaTestCase{
		{
			uuid:           "server-a-uuid",
			uuidWithSuffix: "server-a-uuid.000001",
			pos:            mysql.Position{Name: "mysql-bin.000003", Pos: 123},
			gset:           gset1,
		},
		{
			uuid:           "server-b-uuid",
			uuidWithSuffix: "server-b-uuid.000002",
			pos:            mysql.Position{Name: "mysql-bin.000001", Pos: 234},
			gset:           gset2,
		},
		{
			uuid:           "server-b-uuid", // server-b-uuid again
			uuidWithSuffix: "server-b-uuid.000003",
			pos:            mysql.Position{Name: "mysql-bin.000002", Pos: 345},
			gset:           gset3,
		},
		{
			uuid:           "server-c-uuid",
			uuidWithSuffix: "server-c-uuid.000004",
			pos:            mysql.Position{Name: "mysql-bin.000004", Pos: 678},
			gset:           gset4,
		},
	}

	// load, but empty
	lm := NewLocalMeta("mysql", dir)
	err = lm.Load()
	c.Assert(err, IsNil)

	uuid, pos := lm.Pos()
	c.Assert(uuid, Equals, "")
	c.Assert(pos, DeepEquals, minCheckpoint)
	uuid, gset := lm.GTID()
	c.Assert(uuid, Equals, "")
	c.Assert(gset, DeepEquals, gset0)

	err = lm.Save(minCheckpoint, nil)
	c.Assert(err, NotNil)

	err = lm.Flush()
	c.Assert(err, NotNil)

	dirty := lm.Dirty()
	c.Assert(dirty, IsFalse)

	// set currentUUID because lm.doFlush need it
	currentUUID := "uuid.000001"
	c.Assert(os.MkdirAll(path.Join(dir, currentUUID), 0o777), IsNil)
	setLocalMetaWithCurrentUUID := func() {
		lm = NewLocalMeta("mysql", dir)
		lm.(*LocalMeta).currentUUID = currentUUID
	}

	// adjust to start pos
	setLocalMetaWithCurrentUUID()
	latestBinlogName := "mysql-bin.000009"
	latestGTIDStr := "85ab69d1-b21f-11e6-9c5e-64006a8978d2:45-57"
	cs0 := cases[0]
	adjusted, err := lm.AdjustWithStartPos(cs0.pos.Name, cs0.gset.String(), false, latestBinlogName, latestGTIDStr)
	c.Assert(err, IsNil)
	c.Assert(adjusted, IsTrue)
	uuid, pos = lm.Pos()
	c.Assert(uuid, Equals, currentUUID)
	c.Assert(pos.Name, Equals, cs0.pos.Name)
	uuid, gset = lm.GTID()
	c.Assert(uuid, Equals, currentUUID)
	c.Assert(gset.String(), Equals, "")

	// adjust to start pos with enableGTID
	setLocalMetaWithCurrentUUID()
	adjusted, err = lm.AdjustWithStartPos(cs0.pos.Name, cs0.gset.String(), true, latestBinlogName, latestGTIDStr)
	c.Assert(err, IsNil)
	c.Assert(adjusted, IsTrue)
	uuid, pos = lm.Pos()
	c.Assert(uuid, Equals, currentUUID)
	c.Assert(pos.Name, Equals, cs0.pos.Name)
	uuid, gset = lm.GTID()
	c.Assert(uuid, Equals, currentUUID)
	c.Assert(gset, DeepEquals, cs0.gset)

	// adjust to the last binlog if start pos is empty
	setLocalMetaWithCurrentUUID()
	adjusted, err = lm.AdjustWithStartPos("", cs0.gset.String(), false, latestBinlogName, latestGTIDStr)
	c.Assert(err, IsNil)
	c.Assert(adjusted, IsTrue)
	uuid, pos = lm.Pos()
	c.Assert(uuid, Equals, currentUUID)
	c.Assert(pos.Name, Equals, latestBinlogName)
	uuid, gset = lm.GTID()
	c.Assert(uuid, Equals, currentUUID)
	c.Assert(gset.String(), Equals, "")

	setLocalMetaWithCurrentUUID()
	adjusted, err = lm.AdjustWithStartPos("", "", true, latestBinlogName, latestGTIDStr)
	c.Assert(err, IsNil)
	c.Assert(adjusted, IsTrue)
	uuid, pos = lm.Pos()
	c.Assert(uuid, Equals, currentUUID)
	c.Assert(pos.Name, Equals, latestBinlogName)
	uuid, gset = lm.GTID()
	c.Assert(uuid, Equals, currentUUID)
	c.Assert(gset.String(), Equals, latestGTIDStr)

	// reset
	lm.(*LocalMeta).currentUUID = ""

	for _, cs := range cases {
		err = lm.AddDir(cs.uuid, nil, nil, 0)
		c.Assert(err, IsNil)

		err = lm.Save(cs.pos, cs.gset)
		c.Assert(err, IsNil)

		currentUUID2, pos2 := lm.Pos()
		c.Assert(currentUUID2, Equals, cs.uuidWithSuffix)
		c.Assert(pos2, DeepEquals, cs.pos)

		currentUUID, gset = lm.GTID()
		c.Assert(currentUUID, Equals, cs.uuidWithSuffix)
		c.Assert(gset, DeepEquals, cs.gset)

		dirty = lm.Dirty()
		c.Assert(dirty, IsTrue)

		currentDir := lm.Dir()
		c.Assert(strings.HasSuffix(currentDir, cs.uuidWithSuffix), IsTrue)
	}

	err = lm.Flush()
	c.Assert(err, IsNil)

	dirty = lm.Dirty()
	c.Assert(dirty, IsFalse)

	// try adjust to start pos again
	csn1 := cases[len(cases)-1]
	adjusted, err = lm.AdjustWithStartPos(cs0.pos.Name, cs0.gset.String(), false, "", "")
	c.Assert(err, IsNil)
	c.Assert(adjusted, IsFalse)
	uuid, pos = lm.Pos()
	c.Assert(uuid, Equals, csn1.uuidWithSuffix)
	c.Assert(pos.Name, Equals, csn1.pos.Name)
	uuid, gset = lm.GTID()
	c.Assert(uuid, Equals, csn1.uuidWithSuffix)
	c.Assert(gset, DeepEquals, csn1.gset)

	// create a new LocalMeta, and load it
	lm2 := NewLocalMeta("mysql", dir)
	err = lm2.Load()
	c.Assert(err, IsNil)

	lastCase := cases[len(cases)-1]

	uuid, pos = lm2.Pos()
	c.Assert(uuid, Equals, lastCase.uuidWithSuffix)
	c.Assert(pos, DeepEquals, lastCase.pos)
	uuid, gset = lm2.GTID()
	c.Assert(uuid, Equals, lastCase.uuidWithSuffix)
	c.Assert(gset, DeepEquals, lastCase.gset)

	// another case for AddDir, specify pos and GTID
	cs := MetaTestCase{
		uuid:           "server-c-uuid",
		uuidWithSuffix: "server-c-uuid.000005",
		pos:            mysql.Position{Name: "mysql-bin.000005", Pos: 789},
		gset:           gset5,
	}
	err = lm.AddDir(cs.uuid, &cs.pos, cs.gset, 0)
	c.Assert(err, IsNil)

	dirty = lm.Dirty()
	c.Assert(dirty, IsFalse)

	currentUUID, pos = lm.Pos()
	c.Assert(currentUUID, Equals, cs.uuidWithSuffix)
	c.Assert(pos, DeepEquals, cs.pos)

	currentUUID, gset = lm.GTID()
	c.Assert(currentUUID, Equals, cs.uuidWithSuffix)
	c.Assert(gset, DeepEquals, cs.gset)

	currentDir := lm.Dir()
	c.Assert(strings.HasSuffix(currentDir, cs.uuidWithSuffix), IsTrue)
}

func (r *testMetaSuite) TestLocalMetaPotentialDataRace(c *C) {
	var err error
	lm := NewLocalMeta("mysql", "/FAKE_DIR")
	uuidStr := "85ab69d1-b21f-11e6-9c5e-64006a8978d2"
	initGSet, _ := gtid.ParserGTID("mysql", fmt.Sprintf("%s:1", uuidStr))
	lm.(*LocalMeta).currentUUID = uuidStr
	err = lm.Save(
		mysql.Position{Name: "mysql-bin.000001", Pos: 234},
		initGSet,
	)
	c.Assert(err, IsNil)

	ch1 := make(chan error)
	ch2 := make(chan error)
	pendingCh := make(chan struct{})
	go func() {
		<-pendingCh
		var err error
		defer func() {
			ch1 <- err
		}()
		_, lastGTID := lm.GTID()
		var theMGSet mysql.GTIDSet
		for i := 2; i < 100; i++ {
			theMGSet, err = mysql.ParseGTIDSet("mysql", fmt.Sprintf("%s:1-%d", uuidStr, i*10))
			if err != nil {
				return
			}

			err = lastGTID.Set(theMGSet)
			if err != nil {
				return
			}
			err = lm.Save(
				mysql.Position{Name: fmt.Sprintf("mysql-bin.%06d", i), Pos: 123},
				lastGTID,
			)
			if err != nil {
				return
			}
		}
	}()
	var gtidString string
	go func() {
		<-pendingCh
		var err error
		defer func() {
			ch2 <- err
		}()
		for i := 0; i < 100; i++ {
			_, currentGTID := lm.GTID()
			gtidString = currentGTID.String()
		}
	}()
	close(pendingCh)
	ch1Err := <-ch1
	ch2Err := <-ch2
	c.Assert(ch1Err, IsNil)
	c.Assert(ch2Err, IsNil)
	c.Logf("GTID string from the go routine: %s", gtidString)
}
