// Copyright 2020 PingCAP, Inc.
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

package reader

import (
	"context"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// GetGTIDsForPosFromStreamer tries to get GTID sets for the specified binlog position (for the corresponding txn) from a Streamer.
func GetGTIDsForPosFromStreamer(ctx context.Context, r Streamer, endPos gmysql.Position) (gmysql.GTIDSet, error) {
	var (
		latestPos   uint32
		latestGSet  gmysql.GTIDSet
		nextGTIDStr string // can be recorded if the coming transaction completed
		err         error
	)
	for {
		var e *replication.BinlogEvent
		e, err = r.GetEvent(ctx)
		if err != nil {
			return nil, err
		}

		// NOTE: only update endPos/GTIDs for DDL/XID to get an complete transaction.
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			parser2, err2 := event.GetParserForStatusVars(ev.StatusVars)
			if err2 != nil {
				log.L().Warn("found error when get sql_mode from binlog status_vars", zap.Error(err2))
			}

			isDDL := parser.CheckIsDDL(string(ev.Query), parser2)
			if isDDL {
				if latestGSet == nil {
					// GTID not enabled, can't get GTIDs for the position.
					return nil, errors.Errorf("should have a GTIDEvent before the DDL QueryEvent %+v", e.Header)
				}
				err2 = latestGSet.Update(nextGTIDStr)
				if err2 != nil {
					return nil, terror.Annotatef(err2, "update GTID set %v with GTID %s", latestGSet, nextGTIDStr)
				}
				latestPos = e.Header.LogPos
			}
		case *replication.XIDEvent:
			if latestGSet == nil {
				// GTID not enabled, can't get GTIDs for the position.
				return nil, errors.Errorf("should have a GTIDEvent before the XIDEvent %+v", e.Header)
			}
			err = latestGSet.Update(nextGTIDStr)
			if err != nil {
				return nil, terror.Annotatef(err, "update GTID set %v with GTID %s", latestGSet, nextGTIDStr)
			}
			latestPos = e.Header.LogPos
		case *replication.GTIDEvent:
			if latestGSet == nil {
				return nil, errors.Errorf("should have a PreviousGTIDsEvent before the GTIDEvent %+v", e.Header)
			}
			nextGTIDStr, err = event.GetGTIDStr(e)
			if err != nil {
				return nil, err
			}
		case *replication.MariadbGTIDEvent:
			if latestGSet == nil {
				return nil, errors.Errorf("should have a MariadbGTIDListEvent before the MariadbGTIDEvent %+v", e.Header)
			}
			nextGTIDStr, err = event.GetGTIDStr(e)
			if err != nil {
				return nil, err
			}
		case *replication.PreviousGTIDsEvent:
			// if GTID enabled, we can get a PreviousGTIDEvent after the FormatDescriptionEvent
			// ref: https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/binlog.cc#L4549
			// ref: https://github.com/mysql/mysql-server/blob/8cc757da3d87bf4a1f07dcfb2d3c96fed3806870/sql/binlog.cc#L5161
			latestGSet, err = gtid.ParserGTID(gmysql.MySQLFlavor, ev.GTIDSets)
			if err != nil {
				return nil, err
			}
		case *replication.MariadbGTIDListEvent:
			// a MariadbGTIDListEvent logged in every binlog to record the current replication state if GTID enabled
			// ref: https://mariadb.com/kb/en/library/gtid_list_event/
			latestGSet, err = event.GTIDsFromMariaDBGTIDListEvent(e)
			if err != nil {
				return nil, terror.Annotatef(err, "get GTID set from MariadbGTIDListEvent %+v", e.Header)
			}
		}

		if latestPos == endPos.Pos {
			// reach the end position, return the GTID sets.
			if latestGSet == nil {
				return nil, errors.Errorf("no GTIDs get for position %s", endPos)
			}
			return latestGSet, nil
		} else if latestPos > endPos.Pos {
			return nil, errors.Errorf("invalid position %s or GTID not enabled in upstream", endPos)
		}
	}
}

// GetGTIDsForPos tries to get GTID sets for the specified binlog position (for the corresponding txn).
// NOTE: this method is very similar with `relay/writer/file_util.go/getTxnPosGTIDs`, unify them if needed later.
// NOTE: this method is not well tested directly, but more tests have already been done for `relay/writer/file_util.go/getTxnPosGTIDs`.
func GetGTIDsForPos(ctx context.Context, r Reader, endPos gmysql.Position) (gmysql.GTIDSet, error) {
	// start to get and parse binlog event from the beginning of the file.
	startPos := gmysql.Position{
		Name: endPos.Name,
		Pos:  0,
	}
	err := r.StartSyncByPos(startPos)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return GetGTIDsForPosFromStreamer(ctx, r, endPos)
}

// GetPreviousGTIDFromGTIDSet tries to get previous GTID sets from Previous_GTID_EVENT GTID for the specified GITD Set.
// events should be [fake_rotate_event,format_description_event,previous_gtids_event/mariadb_gtid_list_event].
func GetPreviousGTIDFromGTIDSet(ctx context.Context, r Reader, gset gmysql.GTIDSet) (gmysql.GTIDSet, error) {
	failpoint.Inject("MockGetEmptyPreviousGTIDFromGTIDSet", func(_ failpoint.Value) {
		gset, _ = gtid.ParserGTID("mysql", "")
		failpoint.Return(gset, nil)
	})

	err := r.StartSyncByGTID(gset)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	for {
		var e *replication.BinlogEvent
		e, err = r.GetEvent(ctx)
		if err != nil {
			return nil, err
		}

		switch e.Header.EventType {
		case replication.ROTATE_EVENT:
			if utils.IsFakeRotateEvent(e.Header) {
				continue
			}
			return nil, terror.ErrPreviousGTIDNotExist.Generate(gset.String())
		case replication.FORMAT_DESCRIPTION_EVENT:
			continue
		case replication.PREVIOUS_GTIDS_EVENT:
			previousGset, err := event.GTIDsFromPreviousGTIDsEvent(e)
			return previousGset, err
		case replication.MARIADB_GTID_LIST_EVENT:
			previousGset, err := event.GTIDsFromMariaDBGTIDListEvent(e)
			return previousGset, err
		default:
			return nil, terror.ErrPreviousGTIDNotExist.Generate(gset.String())
		}
	}
}
