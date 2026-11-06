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
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// singleMySQLGTID is the untagged transaction supported by the event generator.
// Keep a value here rather than retaining an interval owned by the input set.
type singleMySQLGTID struct {
	sid uuid.UUID
	gno int64
}

// DDLDMLResult represents a binlog event result for generated DDL/DML.
type DDLDMLResult struct {
	Events     []*replication.BinlogEvent
	Data       []byte // data contain all events
	LatestPos  uint32
	LatestGTID gmysql.GTIDSet
}

// GenCommonFileHeader generates a common binlog file header.
// for MySQL:
//  1. BinLogFileHeader, [ fe `bin` ]
//  2. FormatDescriptionEvent
//  3. PreviousGTIDsEvent, depends on genGTID
//
// for MariaDB:
//  1. BinLogFileHeader, [ fe `bin` ]
//  2. FormatDescriptionEvent
//  3. MariadbGTIDListEvent, depends on genGTID
func GenCommonFileHeader(flavor string, serverID uint32, gSet gmysql.GTIDSet, genGTID bool, ts int64) ([]*replication.BinlogEvent, []byte, error) {
	if ts == 0 {
		ts = time.Now().Unix()
	}
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(ts),
			ServerID:  serverID,
			Flags:     defaultHeaderFlags,
		}
		latestPos   uint32
		prevGTIDsEv *replication.BinlogEvent
		buf         bytes.Buffer
		events      []*replication.BinlogEvent
	)

	_, err := buf.Write(replication.BinLogFileHeader)
	if err != nil {
		return nil, nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write binlog file header % X", replication.BinLogFileHeader)
	}
	latestPos += uint32(len(replication.BinLogFileHeader))

	formatDescEv, err := GenFormatDescriptionEvent(header, latestPos)
	if err != nil {
		return nil, nil, terror.Annotate(err, "generate FormatDescriptionEvent")
	}
	_, err = buf.Write(formatDescEv.RawData)
	if err != nil {
		return nil, nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write FormatDescriptionEvent % X", formatDescEv.RawData)
	}
	latestPos += uint32(len(formatDescEv.RawData)) // update latestPos
	events = append(events, formatDescEv)

	if genGTID {
		switch flavor {
		case gmysql.MySQLFlavor:
			prevGTIDsEv, err = GenPreviousGTIDsEvent(header, latestPos, gSet)
		case gmysql.MariaDBFlavor:
			prevGTIDsEv, err = GenMariaDBGTIDListEvent(header, latestPos, gSet)
		default:
			return nil, nil, terror.ErrBinlogFlavorNotSupport.Generate(flavor)
		}
		if err != nil {
			return nil, nil, terror.Annotate(err, "generate PreviousGTIDsEvent/MariadbGTIDListEvent")
		}

		_, err = buf.Write(prevGTIDsEv.RawData)
		if err != nil {
			return nil, nil, terror.ErrBinlogWriteDataToBuffer.AnnotateDelegate(err, "write PreviousGTIDsEvent/MariadbGTIDListEvent % X", prevGTIDsEv.RawData)
		}
		events = append(events, prevGTIDsEv)
	}

	return events, buf.Bytes(), nil
}

// GenCommonGTIDEvent generates a common GTID event.
func GenCommonGTIDEvent(flavor string, serverID uint32, latestPos uint32, gSet gmysql.GTIDSet, anonymous bool, ts int64) (*replication.BinlogEvent, error) {
	singleGTID, err := verifySingleGTID(flavor, gSet)
	if err != nil {
		return nil, terror.Annotate(err, "verify single GTID in set")
	}

	if ts == 0 {
		ts = time.Now().Unix()
	}
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(ts),
			ServerID:  serverID,
			Flags:     defaultHeaderFlags,
		}
		gtidEv *replication.BinlogEvent
	)

	switch flavor {
	case gmysql.MySQLFlavor:
		gtid := singleGTID.(singleMySQLGTID)
		if anonymous {
			gtidEv, err = GenAnonymousGTIDEvent(header, latestPos, defaultGTIDFlags, defaultLastCommitted, defaultSequenceNumber)
		} else {
			gtidEv, err = GenGTIDEvent(header, latestPos, defaultGTIDFlags, gtid.sid.String(), gtid.gno, defaultLastCommitted, defaultSequenceNumber)
		}
	case gmysql.MariaDBFlavor:
		mariaGTID := singleGTID.(*gmysql.MariadbGTID)
		if mariaGTID.ServerID != header.ServerID {
			return nil, terror.ErrBinlogMariaDBServerIDMismatch.Generate(mariaGTID.ServerID, header.ServerID)
		}
		gtidEv, err = GenMariaDBGTIDEvent(header, latestPos, mariaGTID.SequenceNumber, mariaGTID.DomainID)
		if err != nil {
			return gtidEv, err
		}
		// in go-mysql, set ServerID in parseEvent. we try to set it directly
		gtidEvBody := gtidEv.Event.(*replication.MariadbGTIDEvent)
		gtidEvBody.GTID.ServerID = header.ServerID
	default:
		err = terror.ErrBinlogGTIDSetNotValid.Generate(gSet, flavor)
	}
	return gtidEv, err
}

// GTIDIncrease returns a new GTID with GNO/SequenceNumber +1.
func GTIDIncrease(flavor string, gSet gmysql.GTIDSet) (gmysql.GTIDSet, error) {
	singleGTID, err := verifySingleGTID(flavor, gSet)
	if err != nil {
		return nil, terror.Annotate(err, "verify single GTID in set")
	}
	switch flavor {
	case gmysql.MySQLFlavor:
		gtid := singleGTID.(singleMySQLGTID)
		gtidSet := gmysql.NewMysqlGTIDSet()
		gtidSet.AddGTID(gtid.sid, gtid.gno+1)
		return &gtidSet, nil
	case gmysql.MariaDBFlavor:
		mariaGTID := singleGTID.(*gmysql.MariadbGTID).Clone()
		mariaGTID.SequenceNumber++
		return &gmysql.MariadbGTIDSet{Sets: map[uint32]*gmysql.MariadbGTID{
			mariaGTID.DomainID: mariaGTID,
		}}, nil
	default:
		return nil, terror.ErrBinlogGTIDSetNotValid.Generate(gSet, flavor)
	}
}

// verifySingleGTID verifies gSet whether only containing a single valid GTID.
func verifySingleGTID(flavor string, gSet gmysql.GTIDSet) (interface{}, error) {
	if gtid.CheckGTIDSetEmpty(gSet) {
		return nil, terror.ErrBinlogEmptyGTID.Generate()
	}

	switch flavor {
	case gmysql.MySQLFlavor:
		mysqlGTIDs, ok := gSet.(*gmysql.MysqlGTIDSet)
		if !ok {
			return nil, terror.ErrBinlogGTIDMySQLNotValid.Generate(gSet)
		}
		if len(*mysqlGTIDs) != 1 {
			return nil, terror.ErrBinlogOnlyOneGTIDSupport.Generate(len(*mysqlGTIDs), gSet)
		}
		var sid uuid.UUID
		var tags map[gmysql.Tag]gmysql.IntervalSlice
		for sid, tags = range *mysqlGTIDs {
		}
		// GenGTIDEvent writes the classic GTID_EVENT format. Do not silently
		// drop a tag now that the dependency can parse tagged GTID sets.
		intervals, ok := tags[gmysql.Tag{}]
		if !ok || len(tags) != 1 {
			return nil, terror.ErrBinlogGTIDMySQLNotValid.Generate(gSet)
		}
		if intervals.Len() != 1 {
			return nil, terror.ErrBinlogOnlyOneIntervalInUUID.Generate(intervals.Len(), gSet)
		}
		interval := intervals[0]
		if interval.Stop != interval.Start+1 {
			return nil, terror.ErrBinlogIntervalValueNotValid.Generate(interval, gSet)
		}
		return singleMySQLGTID{sid: sid, gno: interval.Start}, nil
	case gmysql.MariaDBFlavor:
		mariaGTIDs, ok := gSet.(*gmysql.MariadbGTIDSet)
		if !ok {
			return nil, terror.ErrBinlogGTIDMariaDBNotValid.Generate(gSet)
		}
		gtidCount := len(mariaGTIDs.Sets)
		var mariaGTID *gmysql.MariadbGTID
		for _, mariaGTID = range mariaGTIDs.Sets {
		}
		if gtidCount != 1 {
			return nil, terror.ErrBinlogOnlyOneGTIDSupport.Generate(gtidCount, gSet)
		}
		return mariaGTID, nil
	default:
		return nil, terror.ErrBinlogGTIDSetNotValid.Generate(gSet, flavor)
	}
}
