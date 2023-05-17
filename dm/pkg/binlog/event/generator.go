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
	"fmt"
	"time"

	"github.com/coreos/go-semver/semver"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// Generator represents a binlog events generator.
type Generator struct {
	Flavor        string
	ServerID      uint32
	LatestPos     uint32
	LatestGTID    gtid.Set
	ExecutedGTIDs gtid.Set
	LatestXID     uint64

	GenGTID       bool
	AnonymousGTID bool
}

// NewGenerator creates a new instance of Generator.
func NewGenerator(flavor string, serverID uint32, latestPos uint32, latestGTID gtid.Set, previousGTIDs gtid.Set, latestXID uint64) (*Generator, error) {
	return newGenerator(flavor, "5.7.0", serverID, latestPos, latestGTID, previousGTIDs, latestXID, true)
}

func NewGeneratorV2(flavor, version, latestGTIDStr string, enableGTID bool) (*Generator, error) {
	latestGTID, _ := gtid.ParserGTID(flavor, latestGTIDStr)
	previousGTIDSet, _ := gtid.ParserGTID(flavor, latestGTIDStr)
	return newGenerator(flavor, version, 1, 0, latestGTID, previousGTIDSet, 0, enableGTID)
}

func newGenerator(flavor, version string, serverID uint32, latestPos uint32, latestGTID gtid.Set, previousGTIDs gtid.Set, latestXID uint64, genGTID bool) (*Generator, error) {
	prevOrigin := previousGTIDs.Origin()
	if prevOrigin == nil {
		return nil, terror.ErrPreviousGTIDsNotValid.Generate(previousGTIDs)
	}

	singleGTID, err := verifySingleGTID(flavor, latestGTID)
	if err != nil {
		return nil, terror.Annotate(err, "verify single latest GTID in set")
	}
	var anonymousGTID bool
	switch flavor {
	case gmysql.MySQLFlavor:
		uuidSet := singleGTID.(*gmysql.UUIDSet)
		prevGSet, ok := prevOrigin.(*gmysql.MysqlGTIDSet)
		if !ok || prevGSet == nil {
			return nil, terror.ErrBinlogGTIDMySQLNotValid.Generate(previousGTIDs)
		}
		// latestGTID should be one of the latest previousGTIDs
		prevGTID, ok := prevGSet.Sets[uuidSet.SID.String()]
		if !ok || prevGTID.Intervals.Len() != 1 || prevGTID.Intervals[0].Stop != uuidSet.Intervals[0].Stop {
			return nil, terror.ErrBinlogLatestGTIDNotInPrev.Generate(latestGTID, previousGTIDs)
		}

		ver, err := semver.NewVersion(version)
		if err != nil {
			return nil, err
		}
		if ver.Compare(*semver.New("5.7.0")) >= 0 && !genGTID {
			// 5.7+ add anonymous GTID when GTID is disabled
			genGTID = true
			anonymousGTID = true
		}
	case gmysql.MariaDBFlavor:
		mariaGTID := singleGTID.(*gmysql.MariadbGTID)
		if mariaGTID.ServerID != serverID {
			return nil, terror.ErrBinlogMariaDBServerIDMismatch.Generate(mariaGTID.ServerID, serverID)
		}
		// latestGTID should be one of previousGTIDs
		prevGSet, ok := prevOrigin.(*gmysql.MariadbGTIDSet)
		if !ok || prevGSet == nil {
			return nil, terror.ErrBinlogGTIDMariaDBNotValid.Generate(previousGTIDs)
		}
		prevGTID, ok := prevGSet.Sets[mariaGTID.DomainID]
		if !ok || prevGTID.ServerID != mariaGTID.ServerID || prevGTID.SequenceNumber != mariaGTID.SequenceNumber {
			return nil, terror.ErrBinlogLatestGTIDNotInPrev.Generate(latestGTID, previousGTIDs)
		}
		// MariaDB 10.0.2+ always contains GTID
		genGTID = true
	default:
		return nil, terror.ErrBinlogFlavorNotSupport.Generate(flavor)
	}

	return &Generator{
		Flavor:        flavor,
		ServerID:      serverID,
		LatestPos:     latestPos,
		LatestGTID:    latestGTID,
		ExecutedGTIDs: previousGTIDs.Clone(),
		LatestXID:     latestXID,
		GenGTID:       genGTID,
		AnonymousGTID: anonymousGTID,
	}, nil
}

// GenFileHeader generates a binlog file header, including to PreviousGTIDsEvent/MariadbGTIDListEvent.
// for MySQL:
//  1. BinLogFileHeader, [ fe `bin` ]
//  2. FormatDescriptionEvent
//  3. PreviousGTIDsEvent
//
// for MariaDB:
//  1. BinLogFileHeader, [ fe `bin` ]
//  2. FormatDescriptionEvent
//  3. MariadbGTIDListEvent
func (g *Generator) GenFileHeader(ts int64) ([]*replication.BinlogEvent, []byte, error) {
	events, data, err := GenCommonFileHeader(g.Flavor, g.ServerID, g.ExecutedGTIDs, g.GenGTID, ts)
	if err != nil {
		return nil, nil, err
	}
	g.LatestPos = uint32(len(data)) // if generate a binlog file header then reset latest pos
	return events, data, nil
}

// GenCreateDatabaseEvents generates binlog events for `CREATE DATABASE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenCreateDatabaseEvents(schema string) ([]*replication.BinlogEvent, []byte, error) {
	query := fmt.Sprintf("CREATE DATABASE `%s`", schema)
	result, err := GenDDLEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema, query, g.GenGTID, g.AnonymousGTID, 0)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDropDatabaseEvents generates binlog events for `DROP DATABASE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenDropDatabaseEvents(schema string) ([]*replication.BinlogEvent, []byte, error) {
	query := fmt.Sprintf("DROP DATABASE `%s`", schema)
	result, err := GenDDLEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema, query, g.GenGTID, g.AnonymousGTID, 0)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenCreateTableEvents generates binlog events for `CREATE TABLE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenCreateTableEvents(schema string, query string) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDDLEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema, query, g.GenGTID, g.AnonymousGTID, 0)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDropTableEvents generates binlog events for `DROP TABLE`.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenDropTableEvents(schema string, table string) ([]*replication.BinlogEvent, []byte, error) {
	query := fmt.Sprintf("DROP TABLE `%s`.`%s`", schema, table)
	result, err := GenDDLEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema, query, g.GenGTID, g.AnonymousGTID, 0)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDDLEvents generates binlog events for DDL statements.
// events: [GTIDEvent, QueryEvent]
func (g *Generator) GenDDLEvents(schema string, query string, ts int64) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDDLEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, schema, query, g.GenGTID, g.AnonymousGTID, ts)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	return result.Events, result.Data, nil
}

// GenDMLEvents generates binlog events for `INSERT`/`UPDATE`/`DELETE`.
// events: [GTIDEvent, QueryEvent, TableMapEvent, RowsEvent, ..., XIDEvent]
// NOTE: multi <TableMapEvent, RowsEvent> pairs can be in events.
func (g *Generator) GenDMLEvents(eventType replication.EventType, dmlData []*DMLData, ts int64) ([]*replication.BinlogEvent, []byte, error) {
	result, err := GenDMLEvents(g.Flavor, g.ServerID, g.LatestPos, g.LatestGTID, eventType, g.LatestXID+1, dmlData, g.GenGTID, g.AnonymousGTID, ts)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(result.LatestPos, result.LatestGTID)
	g.LatestXID++ // increase XID
	return result.Events, result.Data, nil
}

func (g *Generator) Rotate(nextName string, ts int64) (*replication.BinlogEvent, []byte, error) {
	if ts == 0 {
		ts = time.Now().Unix()
	}
	header := &replication.EventHeader{
		Timestamp: uint32(ts),
		ServerID:  11,
		Flags:     0x01,
	}
	ev, err := GenRotateEvent(header, g.LatestPos, []byte(nextName), 4)
	if err != nil {
		return nil, nil, err
	}
	g.updateLatestPosGTID(4, nil)
	return ev, ev.RawData, nil
}

func (g *Generator) updateLatestPosGTID(latestPos uint32, latestGTID gtid.Set) {
	g.LatestPos = latestPos
	if latestGTID != nil {
		g.LatestGTID = latestGTID
		_ = g.ExecutedGTIDs.Update(latestGTID.String())
	}
}
