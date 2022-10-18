// Copyright 2022 PingCAP, Inc.
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

package binlogstream

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

func isDataEvent(e *replication.BinlogEvent) bool {
	switch e.Event.(type) {
	case *replication.TableMapEvent, *replication.RowsEvent, *replication.QueryEvent:
		return true
	}
	return false
}

// locations provides curStartLocation, curEndLocation, txnEndLocation for binlog
// events.
//
//   - for the event which isDataEvent:
//     |          +-------------+
//     |      ... |current event| ...
//     |     ^    +-------------+    ^
//     |     |                       |
//     | curStartLocation        curEndLocation
//
//     there may be more events between curStartLocation and curEndLocation due
//     to the limitation of binlog or implementation of DM, but in such scenario,
//     those events should always belong to one transaction.
//
//   - for RotateEvent:
//     the binlog filename of curEndLocation and txnEndLocation will be updated
//     to the new NextLogName in RotateEvent.
//
//   - else:
//     we do not guarantee the behaviour of 3 locations of this struct.
type locations struct {
	// curStartLocation is used when
	// - display a meaningful location
	// - match the injected location by handle-error
	// - save table checkpoint of DML
	// curEndLocation is used when
	// - handle end location of DDL, when save table checkpoint or shard-resync
	curStartLocation binlog.Location
	curEndLocation   binlog.Location

	// txnEndLocation is the end location of last seen transaction. If current event is the last event of a txn,
	// txnEndLocation will be assigned from curEndLocation
	// it is used when
	// - reset binlog replication for a finer granularity
	// - save global checkpoint
	txnEndLocation binlog.Location
}

func (l *locations) reset(loc binlog.Location) {
	// need to clone location to avoid the modification leaking outside
	clone := loc.Clone()
	l.curStartLocation = clone
	l.curEndLocation = clone
	l.txnEndLocation = clone
}

// String implements fmt.Stringer.
func (l *locations) String() string {
	return fmt.Sprintf("curStartLocation: %s, curEndLocation: %s, txnEndLocation: %s",
		l.curStartLocation.String(), l.curEndLocation.String(), l.txnEndLocation.String())
}

// updateHookFunc is used to run some logic before locationRecorder.update.
type updateHookFunc func()

// locationRecorder can maintain locations along with update(BinlogEvent). For the
// properties of locations see comments of locations struct.
// locationRecorder is not concurrent-safe.
type locationRecorder struct {
	*locations

	// DML will also generate a query event if user set session binlog_format='statement', we use this field to
	// distinguish DML query event.
	inDMLQuery bool

	preUpdateHook []updateHookFunc
}

func newLocationRecorder() *locationRecorder {
	return &locationRecorder{
		locations: &locations{},
	}
}

func (l *locationRecorder) saveTxnEndLocation() {
	l.txnEndLocation = l.curEndLocation.Clone()
}

// shouldUpdatePos returns true when the given event is from a real upstream writing, returns false when the event is
// header, heartbeat, etc.
func shouldUpdatePos(e *replication.BinlogEvent) bool {
	switch e.Header.EventType {
	case replication.FORMAT_DESCRIPTION_EVENT, replication.HEARTBEAT_EVENT, replication.IGNORABLE_EVENT,
		replication.PREVIOUS_GTIDS_EVENT, replication.MARIADB_GTID_LIST_EVENT:
		return false
	}
	//nolint:gosimple
	if e.Header.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0 {
		// ignore events with LOG_EVENT_ARTIFICIAL_F flag(0x0020) set
		// ref: https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
		return false
	}

	return true
}

func (l *locationRecorder) updateCurStartGTID() {
	gset := l.curEndLocation.GetGTID()
	if gset == nil {
		return
	}
	err := l.curStartLocation.SetGTID(gset)
	if err != nil {
		log.L().DPanic("failed to set GTID set",
			zap.Any("GTID set", gset),
			zap.Error(err))
	}
}

func (l *locationRecorder) setCurEndGTID(gtidStr string) {
	gset := l.curEndLocation.GetGTID()

	if gset == nil {
		gset, _ = gtid.ParserGTID("", gtidStr)
		_ = l.curEndLocation.SetGTID(gset)
		return
	}

	clone := gset.Clone()
	err := clone.Update(gtidStr)
	if err != nil {
		log.L().DPanic("failed to update GTID set",
			zap.String("GTID", gtidStr),
			zap.Error(err))
		return
	}

	err = l.curEndLocation.SetGTID(clone)
	if err != nil {
		log.L().DPanic("failed to set GTID set",
			zap.String("GTID", gtidStr),
			zap.Error(err))
	}
}

// update maintains the member of locationRecorder as their definitions.
// - curStartLocation is assigned to curEndLocation
// - curEndLocation is tried to be updated in-place
// - txnEndLocation is assigned to curEndLocation when `e` is the last event of a transaction.
func (l *locationRecorder) update(e *replication.BinlogEvent) {
	for _, f := range l.preUpdateHook {
		f()
	}
	// reset to zero value of slice after executed
	l.preUpdateHook = nil

	// GTID part is maintained separately
	l.curStartLocation.Position = l.curEndLocation.Position
	l.curStartLocation.Suffix = l.curEndLocation.Suffix

	if event, ok := e.Event.(*replication.RotateEvent); ok {
		nextName := string(event.NextLogName)
		if l.curEndLocation.Position.Name != nextName {
			l.curEndLocation.Position.Name = nextName
			l.curEndLocation.Position.Pos = binlog.FileHeaderLen
			l.saveTxnEndLocation()
		}
		return
	}

	if !shouldUpdatePos(e) {
		return
	}

	l.curEndLocation.Position.Pos = e.Header.LogPos

	switch ev := e.Event.(type) {
	case *replication.GTIDEvent:
		// following event should have new GTID set as end location
		gtidStr, err := event.GetGTIDStr(e)
		if err != nil {
			log.L().DPanic("failed to get GTID from event",
				zap.Any("event", e),
				zap.Error(err))
			break
		}
		l.preUpdateHook = append(l.preUpdateHook, func() {
			l.setCurEndGTID(gtidStr)
		})
	case *replication.MariadbGTIDEvent:
		// following event should have new GTID set as end location
		gtidStr, err := event.GetGTIDStr(e)
		if err != nil {
			log.L().DPanic("failed to get GTID from event",
				zap.Any("event", e),
				zap.Error(err))
			break
		}
		l.preUpdateHook = append(l.preUpdateHook, func() {
			l.setCurEndGTID(gtidStr)
		})

		if !ev.IsDDL() {
			l.inDMLQuery = true
		}
	case *replication.XIDEvent:
		// for transactional engines like InnoDB, COMMIT is xid event
		l.saveTxnEndLocation()
		l.inDMLQuery = false

		// next event can update its GTID set of start location
		l.preUpdateHook = append(l.preUpdateHook, l.updateCurStartGTID)
	case *replication.QueryEvent:
		query := strings.TrimSpace(string(ev.Query))
		switch query {
		case "BEGIN":
			// MySQL will write a "BEGIN" query event when it starts a DML transaction, we use this event to distinguish
			// DML query event which comes from a session binlog_format = STATEMENT.
			// But MariaDB will not write "BEGIN" query event, we simply hope user should not do that.
			l.inDMLQuery = true
		case "COMMIT":
			// for non-transactional engines like MyISAM, COMMIT is query event
			l.inDMLQuery = false
		}

		if l.inDMLQuery {
			return
		}

		// next event can update its GTID set of start location
		l.preUpdateHook = append(l.preUpdateHook, l.updateCurStartGTID)

		l.saveTxnEndLocation()
	}
}
