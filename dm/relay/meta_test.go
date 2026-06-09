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
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/stretchr/testify/require"
)

type MetaTestCase struct {
	uuid           string
	uuidWithSuffix string
	pos            mysql.Position
	gset           mysql.GTIDSet
}

func TestLocalMeta(t *testing.T) {
	dir := t.TempDir()

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
	err := lm.Load()
	require.NoError(t, err)

	uuid, pos := lm.Pos()
	require.Equal(t, "", uuid)
	require.Equal(t, minCheckpoint, pos)
	uuid, gset := lm.GTID()
	require.Equal(t, "", uuid)
	require.Equal(t, gset0, gset)

	err = lm.Save(minCheckpoint, nil)
	require.Error(t, err)

	err = lm.Flush()
	require.Error(t, err)

	dirty := lm.Dirty()
	require.False(t, dirty)

	// set currentSubDir because lm.doFlush need it
	currentUUID := "uuid.000001"
	require.NoError(t, os.MkdirAll(path.Join(dir, currentUUID), 0o777))
	setLocalMetaWithCurrentUUID := func() {
		lm = NewLocalMeta("mysql", dir)
		lm.(*LocalMeta).currentSubDir = currentUUID
	}

	// adjust to start pos
	setLocalMetaWithCurrentUUID()
	latestBinlogName := "mysql-bin.000009"
	latestGTIDStr := "85ab69d1-b21f-11e6-9c5e-64006a8978d2:45-57"
	cs0 := cases[0]
	adjusted, err := lm.AdjustWithStartPos(cs0.pos.Name, cs0.gset.String(), false, latestBinlogName, latestGTIDStr)
	require.NoError(t, err)
	require.True(t, adjusted)
	uuid, pos = lm.Pos()
	require.Equal(t, currentUUID, uuid)
	require.Equal(t, cs0.pos.Name, pos.Name)
	uuid, gset = lm.GTID()
	require.Equal(t, currentUUID, uuid)
	require.Equal(t, "", gset.String())

	// adjust to start pos with enableGTID
	setLocalMetaWithCurrentUUID()
	adjusted, err = lm.AdjustWithStartPos(cs0.pos.Name, cs0.gset.String(), true, latestBinlogName, latestGTIDStr)
	require.NoError(t, err)
	require.True(t, adjusted)
	uuid, pos = lm.Pos()
	require.Equal(t, currentUUID, uuid)
	require.Equal(t, cs0.pos.Name, pos.Name)
	uuid, gset = lm.GTID()
	require.Equal(t, currentUUID, uuid)
	require.Equal(t, cs0.gset, gset)

	// adjust to the last binlog if start pos is empty
	setLocalMetaWithCurrentUUID()
	adjusted, err = lm.AdjustWithStartPos("", cs0.gset.String(), false, latestBinlogName, latestGTIDStr)
	require.NoError(t, err)
	require.True(t, adjusted)
	uuid, pos = lm.Pos()
	require.Equal(t, currentUUID, uuid)
	require.Equal(t, latestBinlogName, pos.Name)
	uuid, gset = lm.GTID()
	require.Equal(t, currentUUID, uuid)
	require.Equal(t, "", gset.String())

	setLocalMetaWithCurrentUUID()
	adjusted, err = lm.AdjustWithStartPos("", "", true, latestBinlogName, latestGTIDStr)
	require.NoError(t, err)
	require.True(t, adjusted)
	uuid, pos = lm.Pos()
	require.Equal(t, currentUUID, uuid)
	require.Equal(t, latestBinlogName, pos.Name)
	uuid, gset = lm.GTID()
	require.Equal(t, currentUUID, uuid)
	require.Equal(t, latestGTIDStr, gset.String())

	// reset
	lm.(*LocalMeta).currentSubDir = ""

	for _, cs := range cases {
		err = lm.AddDir(cs.uuid, nil, nil, 0)
		require.NoError(t, err)

		err = lm.Save(cs.pos, cs.gset)
		require.NoError(t, err)

		currentUUID2, pos2 := lm.Pos()
		require.Equal(t, cs.uuidWithSuffix, currentUUID2)
		require.Equal(t, cs.pos, pos2)

		currentUUID, gset = lm.GTID()
		require.Equal(t, cs.uuidWithSuffix, currentUUID)
		require.Equal(t, cs.gset, gset)

		dirty = lm.Dirty()
		require.True(t, dirty)

		currentDir := lm.Dir()
		require.True(t, strings.HasSuffix(currentDir, cs.uuidWithSuffix))
	}

	err = lm.Flush()
	require.NoError(t, err)

	dirty = lm.Dirty()
	require.False(t, dirty)

	// try adjust to start pos again
	csn1 := cases[len(cases)-1]
	adjusted, err = lm.AdjustWithStartPos(cs0.pos.Name, cs0.gset.String(), false, "", "")
	require.NoError(t, err)
	require.False(t, adjusted)
	uuid, pos = lm.Pos()
	require.Equal(t, csn1.uuidWithSuffix, uuid)
	require.Equal(t, csn1.pos.Name, pos.Name)
	uuid, gset = lm.GTID()
	require.Equal(t, csn1.uuidWithSuffix, uuid)
	require.Equal(t, csn1.gset, gset)

	// create a new LocalMeta, and load it
	lm2 := NewLocalMeta("mysql", dir)
	err = lm2.Load()
	require.NoError(t, err)

	lastCase := cases[len(cases)-1]

	uuid, pos = lm2.Pos()
	require.Equal(t, lastCase.uuidWithSuffix, uuid)
	require.Equal(t, lastCase.pos, pos)
	uuid, gset = lm2.GTID()
	require.Equal(t, lastCase.uuidWithSuffix, uuid)
	require.Equal(t, lastCase.gset, gset)

	// another case for AddDir, specify pos and GTID
	cs := MetaTestCase{
		uuid:           "server-c-uuid",
		uuidWithSuffix: "server-c-uuid.000005",
		pos:            mysql.Position{Name: "mysql-bin.000005", Pos: 789},
		gset:           gset5,
	}
	err = lm.AddDir(cs.uuid, &cs.pos, cs.gset, 0)
	require.NoError(t, err)

	dirty = lm.Dirty()
	require.False(t, dirty)

	currentUUID, pos = lm.Pos()
	require.Equal(t, cs.uuidWithSuffix, currentUUID)
	require.Equal(t, cs.pos, pos)

	currentUUID, gset = lm.GTID()
	require.Equal(t, cs.uuidWithSuffix, currentUUID)
	require.Equal(t, cs.gset, gset)

	currentDir := lm.Dir()
	require.True(t, strings.HasSuffix(currentDir, cs.uuidWithSuffix))
}

func TestLocalMetaPotentialDataRace(t *testing.T) {
	var err error
	lm := NewLocalMeta("mysql", "/FAKE_DIR")
	uuidStr := "85ab69d1-b21f-11e6-9c5e-64006a8978d2"
	initGSet, _ := gtid.ParserGTID("mysql", fmt.Sprintf("%s:1", uuidStr))
	lm.(*LocalMeta).currentSubDir = uuidStr
	err = lm.Save(
		mysql.Position{Name: "mysql-bin.000001", Pos: 234},
		initGSet,
	)
	require.NoError(t, err)

	ch1 := make(chan error)
	ch2 := make(chan error)
	pendingCh := make(chan struct{})
	go func() {
		<-pendingCh
		var err error
		defer func() {
			ch1 <- err
		}()
		var theMGSet mysql.GTIDSet
		for i := 2; i < 100; i++ {
			theMGSet, err = mysql.ParseGTIDSet("mysql", fmt.Sprintf("%s:1-%d", uuidStr, i*10))
			if err != nil {
				return
			}

			lastGTID := theMGSet
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
	require.NoError(t, ch1Err)
	require.NoError(t, ch2Err)
	t.Logf("GTID string from the go routine: %s", gtidString)
}
