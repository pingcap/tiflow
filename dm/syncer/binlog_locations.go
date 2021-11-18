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
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/binlog"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/utils"
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
	// - "End location" (next start location) is used to decide if a handle-error event should be returned, sometimes
	//   "End location" will be manually adjusted.
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

func (l *locationRecorder) getCurEndLocation() binlog.Location {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.curEndLocation
}

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

// update maintains the member of locationRecorder as their definitions.
// - curStartLocation is assigned to curEndLocation
// - curEndLocation is tried to be updated in-place
// - txnEndLocation is assigned to curEndLocation when `e` is the last event of a transaction.
func (l *locationRecorder) update(e *replication.BinlogEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.curStartLocation = l.curEndLocation
	if event, ok := e.Event.(*replication.RotateEvent); ok {
		// event for the outdated fake rotate events that is sent when MySQL replication is started, we still update
		// curEndLocation. those short-lived values should not be saved to checkpoint.
		if utils.IsFakeRotateEvent(e.Header) {
			l.curEndLocation.Position.Name = string(event.NextLogName)
		}
	}

	l.curEndLocation.Position.Pos = e.Header.LogPos

	updateGTIDSet := func(gset mysql.GTIDSet) {
		err := l.curEndLocation.SetGTID(gset)
		if err != nil {
			log.L().DPanic("failed to set GTID set",
				zap.Any("GTID set", gset),
				zap.Error(err))
		}
	}

	updateGTIDSetFromStr := func(gtidStr string) {
		gset := l.curEndLocation.GetGTID().Origin().Clone()
		err := gset.Update(gtidStr)
		if err != nil {
			log.L().DPanic("failed to update GTID set",
				zap.Any("GTID set", gset),
				zap.String("gtid", gtidStr),
				zap.Error(err))
			return
		}
	}

	switch ev := e.Event.(type) {
	// we maintain GTID set like https://github.com/go-mysql-org/go-mysql/blob/423b04c789346b2abc8e248060cf46eed834cbfa/replication/binlogsyncer.go#L792-L809
	case *replication.GTIDEvent:
		u, _ := uuid.FromBytes(ev.SID)
		gtidStr := fmt.Sprintf("%s:%d", u.String(), ev.GNO)
		updateGTIDSetFromStr(gtidStr)
	case *replication.MariadbGTIDEvent:
		gtidStr := fmt.Sprintf("%d-%d-%d", ev.GTID.DomainID, ev.GTID.ServerID, ev.GTID.SequenceNumber)
		updateGTIDSetFromStr(gtidStr)
	case *replication.XIDEvent:
		updateGTIDSet(ev.GSet)
		l.saveTxnEndLocation(false)
		l.inDML = false
	case *replication.QueryEvent:
		query := strings.TrimSpace(string(ev.Query))
		if query == "BEGIN" {
			l.inDML = true
		}

		if l.inDML {
			return
		}

		updateGTIDSet(ev.GSet)
		l.saveTxnEndLocation(false)
	}
}

// String implements fmt.Stringer.
func (l *locationRecorder) String() string {
	return fmt.Sprintf("curStartLocation: %s, curEndLocation: %s, txnEndLocation: %s",
		l.curStartLocation.String(), l.curEndLocation.String(), l.txnEndLocation.String())
}
