// Copyright 2021 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

var _ = Suite(&testPosFinderSuite{})

type testPosFinderSuite struct{}

func genBinlogFile(generator *event.Generator, start time.Time, nextFile string) ([]*replication.BinlogEvent, []byte) {
	insertDMLData := []*event.DMLData{
		{
			TableID:    uint64(1),
			Schema:     fmt.Sprintf("db_%d", 1),
			Table:      strconv.Itoa(1),
			ColumnType: []byte{mysql.MYSQL_TYPE_INT24},
			Rows:       [][]interface{}{{int32(1)}, {int32(2)}},
		},
	}
	allEvents := make([]*replication.BinlogEvent, 0)
	var buf bytes.Buffer
	events, data, _ := generator.GenFileHeader(start.Add(1 * time.Second).Unix())
	allEvents = append(allEvents, events...)
	buf.Write(data)

	events, data, _ = generator.GenDDLEvents("test", "create table t(id int)", start.Add(2*time.Second).Unix())
	allEvents = append(allEvents, events...)
	buf.Write(data)

	events, data, _ = generator.GenDMLEvents(replication.WRITE_ROWS_EVENTv2, insertDMLData, start.Add(3*time.Second).Unix())
	allEvents = append(allEvents, events...)
	buf.Write(data)

	events, data, _ = generator.GenDMLEvents(replication.WRITE_ROWS_EVENTv2, insertDMLData, start.Add(5*time.Second).Unix())
	allEvents = append(allEvents, events...)
	buf.Write(data)

	ev, data, _ := generator.Rotate(nextFile, start.Add(5*time.Second).Unix())
	allEvents = append(allEvents, ev)
	buf.Write(data)

	return allEvents, buf.Bytes()
}

func (t *testPosFinderSuite) TestTransBoundary(c *C) {
	flavor := "mysql"
	relayDir := c.MkDir()
	beforeTime := time.Now()
	latestGTIDStr := "ffffffff-ffff-ffff-ffff-ffffffffffff:1"
	generator, _ := event.NewGeneratorV2(flavor, "5.6.0", latestGTIDStr, false)
	insertDMLData := []*event.DMLData{
		{
			TableID:    uint64(1),
			Schema:     fmt.Sprintf("db_%d", 1),
			Table:      strconv.Itoa(1),
			ColumnType: []byte{mysql.MYSQL_TYPE_INT24},
			Rows:       [][]interface{}{{int32(1)}, {int32(2)}},
		},
	}
	var buf bytes.Buffer
	_, data, err := generator.GenFileHeader(beforeTime.Add(1 * time.Second).Unix())
	c.Assert(err, IsNil)
	buf.Write(data)

	// first transaction, timestamp of BEGIN = beforeTime.Add(2*time.Second)
	// timestamp of other events inside this transaction = beforeTime.Add(3 * time.Second)
	ts := beforeTime.Add(2 * time.Second).Unix()
	header := &replication.EventHeader{
		Timestamp: uint32(ts),
		ServerID:  11,
		Flags:     0x01,
	}
	beginEvent, _ := event.GenQueryEvent(header, generator.LatestPos, 1, 1, 0, []byte("0"), []byte("test"), []byte("BEGIN"))
	buf.Write(beginEvent.RawData)

	ts = beforeTime.Add(3 * time.Second).Unix()
	header.Timestamp = uint32(ts)
	mapEvent, _ := event.GenTableMapEvent(header, beginEvent.Header.LogPos, 1, []byte("test"), []byte("t"), []byte{mysql.MYSQL_TYPE_INT24})
	buf.Write(mapEvent.RawData)
	rowsEvent, _ := event.GenRowsEvent(header, mapEvent.Header.LogPos, replication.WRITE_ROWS_EVENTv2, 1, 1, [][]interface{}{{int32(1)}, {int32(2)}}, []byte{mysql.MYSQL_TYPE_INT24}, mapEvent)
	buf.Write(rowsEvent.RawData)
	xidEvent, _ := event.GenXIDEvent(header, rowsEvent.Header.LogPos, 1)
	buf.Write(xidEvent.RawData)

	// second transaction, timestamp of all events = beforeTime.Add(3 * time.Second)
	generator.LatestPos = xidEvent.Header.LogPos
	dmlEvents, data, _ := generator.GenDMLEvents(replication.WRITE_ROWS_EVENTv2, insertDMLData, ts)
	buf.Write(data)

	c.Assert(dmlEvents[len(dmlEvents)-1].Header.LogPos, Equals, uint32(buf.Len()))
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000001"), buf.Bytes(), 0o644)

	{
		tcctx := tcontext.NewContext(context.Background(), log.L())
		finder := NewLocalBinlogPosFinder(tcctx, false, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(ts)
		c.Assert(err, IsNil)
		// start of second transaction
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000001", Pos: xidEvent.Header.LogPos})
		c.Assert(location.GTIDSetStr(), Equals, "")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
}

func (t *testPosFinderSuite) TestMySQL56NoGTID(c *C) {
	flavor := "mysql"
	relayDir := c.MkDir()
	beforeTime := time.Now()
	latestGTIDStr := "ffffffff-ffff-ffff-ffff-ffffffffffff:1"

	generator, _ := event.NewGeneratorV2(flavor, "5.6.0", latestGTIDStr, false)

	file1Events, data := genBinlogFile(generator, beforeTime, "mysql-bin.000002")
	c.Assert(len(file1Events), Equals, 11)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000001"), data, 0o644)
	file2Events, data := genBinlogFile(generator, beforeTime.Add(5*time.Second), "mysql-bin.000003")
	c.Assert(len(file2Events), Equals, 11)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000002"), data, 0o644)
	file3Events, data := genBinlogFile(generator, beforeTime.Add(10*time.Second), "mysql-bin.000004")
	c.Assert(len(file3Events), Equals, 11)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000003"), data, 0o644)

	tcctx := tcontext.NewContext(context.Background(), log.L())
	{
		var targetEventStart uint32
		var targetEvent *replication.BinlogEvent
		for _, ev := range file1Events {
			if e, ok := ev.Event.(*replication.QueryEvent); ok && string(e.Query) == "BEGIN" {
				targetEvent = ev
				break
			}
			targetEventStart = ev.Header.LogPos
		}
		finder := NewLocalBinlogPosFinder(tcctx, false, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(targetEvent.Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000001", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
	{
		targetEventStart := file2Events[len(file2Events)-1].Header.LogPos
		finder := NewLocalBinlogPosFinder(tcctx, false, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(file3Events[0].Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000002", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
	{
		var targetEventStart uint32
		var targetEvent *replication.BinlogEvent
		for _, ev := range file3Events {
			if _, ok := ev.Event.(*replication.QueryEvent); ok {
				targetEvent = ev
				break
			}
			targetEventStart = ev.Header.LogPos
		}
		finder := NewLocalBinlogPosFinder(tcctx, false, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(targetEvent.Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000003", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
}

func (t *testPosFinderSuite) TestMySQL57NoGTID(c *C) {
	flavor := "mysql"
	relayDir := c.MkDir()
	beforeTime := time.Now()
	latestGTIDStr := "ffffffff-ffff-ffff-ffff-ffffffffffff:1"

	generator, _ := event.NewGeneratorV2(flavor, "5.7.0", latestGTIDStr, false)

	file1Events, data := genBinlogFile(generator, beforeTime, "mysql-bin.000002")
	c.Assert(len(file1Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000001"), data, 0o644)
	file2Events, data := genBinlogFile(generator, beforeTime.Add(5*time.Second), "mysql-bin.000003")
	c.Assert(len(file2Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000002"), data, 0o644)
	file3Events, data := genBinlogFile(generator, beforeTime.Add(10*time.Second), "mysql-bin.000004")
	c.Assert(len(file3Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000003"), data, 0o644)

	tcctx := tcontext.NewContext(context.Background(), log.L())
	{
		var targetEventStart uint32
		var targetEvent *replication.BinlogEvent
		cnt := 0
		for _, ev := range file3Events {
			if ev.Header.EventType == replication.ANONYMOUS_GTID_EVENT {
				targetEvent = ev
				// second GTID event
				cnt++
				if cnt == 2 {
					break
				}
			}
			targetEventStart = ev.Header.LogPos
		}
		finder := NewLocalBinlogPosFinder(tcctx, false, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(targetEvent.Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000003", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
}

func (t *testPosFinderSuite) TestErrorCase(c *C) {
	flavor := "mysql"
	relayDir := c.MkDir()
	beforeTime := time.Now()
	tcctx := tcontext.NewContext(context.Background(), log.L())
	{
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir+"not-exist")
		_, _, err := finder.FindByTimestamp(beforeTime.Add(-time.Minute).Unix())
		c.Assert(err.Error(), Matches, ".*no such file or directory.*")
	}
	{
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, c.MkDir())
		_, _, err := finder.FindByTimestamp(beforeTime.Add(-time.Minute).Unix())
		c.Assert(err.Error(), Matches, ".*cannot find binlog files.*")
	}
	{
		file, err := os.Create(path.Join(relayDir, "mysql-bin.000001"))
		c.Assert(err, IsNil)
		file.Close()
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		_, _, err = finder.FindByTimestamp(beforeTime.Add(-time.Minute).Unix())
		c.Assert(err.Error(), Matches, "EOF")
	}
}

func (t *testPosFinderSuite) TestMySQL57GTID(c *C) {
	flavor := "mysql"
	relayDir := c.MkDir()
	beforeTime := time.Now()
	latestGTIDStr := "ffffffff-ffff-ffff-ffff-ffffffffffff:1"

	generator, _ := event.NewGeneratorV2(flavor, "5.7.0", latestGTIDStr, true)

	file1Events, data := genBinlogFile(generator, beforeTime, "mysql-bin.000002")
	c.Assert(len(file1Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000001"), data, 0o644)
	file2Events, data := genBinlogFile(generator, beforeTime.Add(5*time.Second), "mysql-bin.000003")
	c.Assert(len(file2Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000002"), data, 0o644)
	file3Events, data := genBinlogFile(generator, beforeTime.Add(10*time.Second), "mysql-bin.000004")
	c.Assert(len(file3Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000003"), data, 0o644)

	tcctx := tcontext.NewContext(context.Background(), log.L())

	{
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(beforeTime.Add(-time.Minute).Unix())
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000001", Pos: 4})
		c.Assert(location.GTIDSetStr(), Equals, "ffffffff-ffff-ffff-ffff-ffffffffffff:1")
		c.Assert(posType, Equals, BelowLowerBoundBinlogPos)
	}
	{
		gtids := []string{
			"ffffffff-ffff-ffff-ffff-ffffffffffff:1",
			"ffffffff-ffff-ffff-ffff-ffffffffffff:1-2",
			"ffffffff-ffff-ffff-ffff-ffffffffffff:1-3",
		}
		var targetEventStart uint32
		var targetEvent *replication.BinlogEvent
		cnt := 0
		for _, ev := range file1Events {
			if ev.Header.EventType == replication.GTID_EVENT {
				targetEvent = ev

				finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
				location, posType, err := finder.FindByTimestamp(int64(targetEvent.Header.Timestamp))
				c.Assert(err, IsNil)
				c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000001", Pos: targetEventStart})
				c.Assert(location.GTIDSetStr(), Equals, gtids[cnt])
				c.Assert(posType, Equals, InRangeBinlogPos)

				cnt++
			}
			targetEventStart = ev.Header.LogPos
		}
	}
	{
		targetEventStart := file2Events[len(file2Events)-1].Header.LogPos
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(file3Events[0].Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000002", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "ffffffff-ffff-ffff-ffff-ffffffffffff:1-7")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
	{
		var targetEventStart uint32
		var targetEvent *replication.BinlogEvent
		cnt := 0
		for _, ev := range file3Events {
			if ev.Header.EventType == replication.GTID_EVENT {
				targetEvent = ev
				// third GTID event
				cnt++
				if cnt == 3 {
					break
				}
			}
			targetEventStart = ev.Header.LogPos
		}
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(targetEvent.Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000003", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "ffffffff-ffff-ffff-ffff-ffffffffffff:1-9")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
	{
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(beforeTime.Add(+time.Minute).Unix())
		c.Assert(err, IsNil)
		c.Assert(location, IsNil)
		c.Assert(posType, Equals, AboveUpperBoundBinlogPos)
	}
}

func (t *testPosFinderSuite) TestMariadbGTID(c *C) {
	flavor := "mariadb"
	relayDir := c.MkDir()
	beforeTime := time.Now()
	latestGTIDStr := "1-1-1"

	generator, _ := event.NewGeneratorV2(flavor, "10.0.2", latestGTIDStr, true)

	file1Events, data := genBinlogFile(generator, beforeTime, "mysql-bin.000002")
	c.Assert(len(file1Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000001"), data, 0o644)
	file2Events, data := genBinlogFile(generator, beforeTime.Add(5*time.Second), "mysql-bin.000003")
	c.Assert(len(file2Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000002"), data, 0o644)
	file3Events, data := genBinlogFile(generator, beforeTime.Add(10*time.Second), "mysql-bin.000004")
	c.Assert(len(file3Events), Equals, 15)
	_ = os.WriteFile(path.Join(relayDir, "mysql-bin.000003"), data, 0o644)

	tcctx := tcontext.NewContext(context.Background(), log.L())

	{
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(beforeTime.Add(-time.Minute).Unix())
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000001", Pos: 4})
		c.Assert(location.GTIDSetStr(), Equals, "1-1-1")
		c.Assert(posType, Equals, BelowLowerBoundBinlogPos)
	}
	{
		var targetEventStart uint32
		var targetEvent *replication.BinlogEvent
		cnt := 0
		for _, ev := range file1Events {
			if ev.Header.EventType == replication.MARIADB_GTID_EVENT {
				targetEvent = ev
				// second GTID event
				cnt++
				if cnt == 2 {
					break
				}
			}
			targetEventStart = ev.Header.LogPos
		}
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(targetEvent.Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000001", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "1-1-2")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
	{
		targetEventStart := file2Events[len(file2Events)-1].Header.LogPos
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(file3Events[0].Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000002", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "1-1-7")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
	{
		var targetEventStart uint32
		var targetEvent *replication.BinlogEvent
		cnt := 0
		for _, ev := range file3Events {
			if ev.Header.EventType == replication.MARIADB_GTID_EVENT {
				targetEvent = ev
				// second GTID event
				cnt++
				if cnt == 2 {
					break
				}
			}
			targetEventStart = ev.Header.LogPos
		}
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(int64(targetEvent.Header.Timestamp))
		c.Assert(err, IsNil)
		c.Assert(location.Position, Equals, mysql.Position{Name: "mysql-bin.000003", Pos: targetEventStart})
		c.Assert(location.GTIDSetStr(), Equals, "1-1-8")
		c.Assert(posType, Equals, InRangeBinlogPos)
	}
	{
		finder := NewLocalBinlogPosFinder(tcctx, true, flavor, relayDir)
		location, posType, err := finder.FindByTimestamp(beforeTime.Add(+time.Minute).Unix())
		c.Assert(err, IsNil)
		c.Assert(location, IsNil)
		c.Assert(posType, Equals, AboveUpperBoundBinlogPos)
	}
}
