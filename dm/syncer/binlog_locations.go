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

package syncer

import (
	"fmt"
	"strings"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/binlog"
	"github.com/pingcap/ticdc/dm/pkg/log"
)

type locationRecorder struct {
	// - "Start location" means if we send this location to upstream, we will receive and see this event in first ones.
	//   So for position-based replication, this is the binlog file offset of the first byte of event minus 1. The first
	//   effective event after replication starts should be this event.
	//   For GTID-based replication, this is the GTID set just added the last transaction, and the first transaction
	//   after replication starts should contain this event. This behaviour of GTID means if we restart replication from
	//   a point inside a transaction, we will receive duplicated events from the first event of the transaction to the
	//   point where we restart.
	// - "End location" acts similarly.
	//   For position-based replication, this is the file offset of the last byte of event.
	//   For GTID-based replication, this is the GTID set just added this GTID.
	// - txnEndLocation is the end location of last transaction. If this event is the last event of a txn,
	//   txnEndLocation will be assigned from curEndLocation
	curStartLocation binlog.Location
	curEndLocation   binlog.Location
	txnEndLocation   binlog.Location

	// DML will also generate a query event if user set session binlog_format='statement', we use this field to
	// distinguish DML query event.
	inDML bool

	mu sync.Mutex // guard curEndLocation because Syncer.printStatus is reading it from another goroutine.
}

func (l *locationRecorder) reset(loc binlog.Location) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.curStartLocation = loc
	l.curEndLocation = loc
	l.txnEndLocation = loc
}

//nolint:unused
func (l *locationRecorder) getCurEndLocation() binlog.Location {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.curEndLocation
}

//nolint:unused
func (l *locationRecorder) setCurEndLocation(location binlog.Location) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.curEndLocation = location
}

// NOTE only locationRecorder should call this.
func (l *locationRecorder) saveTxnEndLocation(needLock bool) {
	if needLock {
		l.mu.Lock()
		defer l.mu.Unlock()
	}

	l.txnEndLocation = l.curEndLocation
	_ = l.txnEndLocation.SetGTID(l.curEndLocation.GetGTID().Origin().Clone())
}

// shouldUpdatePos returns true when the given event is from a real upstream writing, returns false when the event is
// header, heartbeat, etc.
func shouldUpdatePos(e *replication.BinlogEvent) bool {
	switch e.Header.EventType {
	case replication.FORMAT_DESCRIPTION_EVENT, replication.HEARTBEAT_EVENT, replication.IGNORABLE_EVENT,
		replication.PREVIOUS_GTIDS_EVENT, replication.MARIADB_GTID_LIST_EVENT:
		return false
	}
	if e.Header.Flags&replication.LOG_EVENT_ARTIFICIAL_F != 0 {
		// ignore events with LOG_EVENT_ARTIFICIAL_F flag(0x0020) set
		// ref: https://dev.mysql.com/doc/internals/en/binlog-event-flag.html
		return false
	}

	return true
}

func (l *locationRecorder) setCurrentGTID(gset mysql.GTIDSet) {
	err := l.curEndLocation.SetGTID(gset)
	if err != nil {
		log.L().DPanic("failed to set GTID set",
			zap.Any("GTID set", gset),
			zap.Error(err))
	}
}

// update maintains the member of locationRecorder as their definitions.
// - curStartLocation is assigned to curEndLocation
// - curEndLocation is tried to be updated in-place
// - txnEndLocation is assigned to curEndLocation when `e` is the last event of a transaction.
func (l *locationRecorder) update(e *replication.BinlogEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.curStartLocation = l.curEndLocation

	if !shouldUpdatePos(e) {
		// so for these events, we'll set streamer to let curStartLocation equals curEndLocation
		return
	}

	if event, ok := e.Event.(*replication.RotateEvent); ok {
		nextName := string(event.NextLogName)
		if l.curEndLocation.Position.Name != nextName {
			l.curEndLocation.Position.Name = nextName
			l.curEndLocation.Position.Pos = 4
			l.saveTxnEndLocation(false)
		}
		return
	}

	l.curEndLocation.Position.Pos = e.Header.LogPos

	switch ev := e.Event.(type) {
	case *replication.XIDEvent:
		// for transactional engines like InnoDB, COMMIT is xid event
		l.setCurrentGTID(ev.GSet)
		l.saveTxnEndLocation(false)
		l.inDML = false
	case *replication.QueryEvent:
		query := strings.TrimSpace(string(ev.Query))
		switch query {
		case "BEGIN":
			// MySQL will write a "BEGIN" query event when it starts a DML transaction, we use this event to distinguish
			// DML query event which comes from a session binlog_format = STATEMENT.
			// But MariaDB will not write "BEGIN" query event, we simply hope user should not do that.
			l.inDML = true
		case "COMMIT":
			// for non-transactional engines like MyISAM, COMMIT is query event
			l.inDML = false
		}

		if l.inDML {
			return
		}

		l.setCurrentGTID(ev.GSet)
		l.saveTxnEndLocation(false)
	case *replication.MariadbGTIDEvent:
		if !ev.IsDDL() {
			l.inDML = true
		}
	}
}

// String implements fmt.Stringer.
func (l *locationRecorder) String() string {
	return fmt.Sprintf("curStartLocation: %s, curEndLocation: %s, txnEndLocation: %s",
		l.curStartLocation.String(), l.curEndLocation.String(), l.txnEndLocation.String())
}
