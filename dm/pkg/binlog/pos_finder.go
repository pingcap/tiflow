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

package binlog

import (
	"database/sql"
	"path"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/binlog/common"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

type binlogPosFinder struct {
	remote     bool
	tctx       *tcontext.Context
	enableGTID bool
	parser     *replication.BinlogParser
	flavor     string

	// fields used for remote mode
	db      *sql.DB
	syncCfg replication.BinlogSyncerConfig

	// fields used for local relay
	relayDir string // should be a directory with current UUID

	// fields used inside FindByTs
	targetBinlog        binlogSize // target binlog file the timestamp may reside
	tsBeforeFirstBinlog bool       // whether the timestamp is before the first binlog
	lastBinlogFile      bool       // whether targetBinlog is the last binlog file

	everMetGTIDEvent bool // whether ever met a GTID event
	inTransaction    bool // whether in transaction
}

type PosType int

func (b PosType) String() string {
	switch b {
	case BelowLowerBoundBinlogPos:
		return "BelowLowerBound"
	case InRangeBinlogPos:
		return "InRange"
	case AboveUpperBoundBinlogPos:
		return "AboveUpperBound"
	}
	return "Invalid"
}

const (
	InvalidBinlogPos PosType = iota
	BelowLowerBoundBinlogPos
	InRangeBinlogPos
	AboveUpperBoundBinlogPos
)

func NewLocalBinlogPosFinder(tctx *tcontext.Context, enableGTID bool, flavor string, relayDir string) *binlogPosFinder {
	parser := replication.NewBinlogParser()
	parser.SetFlavor(flavor)
	parser.SetUseDecimal(true)
	parser.SetVerifyChecksum(true)

	return &binlogPosFinder{
		remote:     false,
		tctx:       tctx,
		enableGTID: enableGTID,
		parser:     parser,
		flavor:     flavor,

		relayDir: relayDir,
	}
}

func NewRemoteBinlogPosFinder(tctx *tcontext.Context, db *sql.DB, syncCfg replication.BinlogSyncerConfig, enableGTID bool) *binlogPosFinder {
	// make sure raw mode enabled, and MaxReconnectAttempts set
	syncCfg.RawModeEnabled = true
	if syncCfg.MaxReconnectAttempts == 0 {
		syncCfg.MaxReconnectAttempts = common.MaxBinlogSyncerReconnect
	}

	parser := replication.NewBinlogParser()
	parser.SetFlavor(syncCfg.Flavor)
	parser.SetUseDecimal(true)
	parser.SetVerifyChecksum(true)

	return &binlogPosFinder{
		remote:     true,
		tctx:       tctx,
		enableGTID: enableGTID,
		parser:     parser,
		flavor:     syncCfg.Flavor,

		db:      db,
		syncCfg: syncCfg,
	}
}

func (r *binlogPosFinder) getBinlogFiles() (FileSizes, error) {
	if r.remote {
		return GetBinaryLogs(r.tctx.Ctx, r.db)
	} else {
		return GetLocalBinaryLogs(r.relayDir)
	}
}

func (r *binlogPosFinder) startSync(position mysql.Position) (reader.Reader, error) {
	if r.remote {
		binlogReader := reader.NewTCPReader(r.syncCfg)
		return binlogReader, binlogReader.StartSyncByPos(position)
	} else {
		binlogReader := reader.NewFileReader(&reader.FileReaderConfig{EnableRawMode: true})
		position.Name = path.Join(r.relayDir, position.Name)
		return binlogReader, binlogReader.StartSyncByPos(position)
	}
}

func (r *binlogPosFinder) initTargetBinlogFile(ts int64) error {
	targetTs := uint32(ts)
	var prevBinlog, currBinlog binlogSize
	var prevTs, currTs uint32
	binaryLogs, err := r.getBinlogFiles()
	if err != nil {
		return err
	}
	if len(binaryLogs) <= 0 {
		// should not happen on a master with binlog enabled
		return errors.New("cannot find binlog on master")
	}
	// large proportion of the time will be spent at finding inside the target binlog file
	// find the target binlog file is relatively fast
	// TODO maybe change to binary search later when len(binaryLogs) is large
	for _, binlogFile := range binaryLogs {
		// master switched may complicate the situation, suppose we are trying to sync mysql-bin.000003, both master
		// contains it, but they have different timestamp, we may get the wrong location.
		prevTs, currTs = currTs, 0
		prevBinlog, currBinlog = currBinlog, binlogFile
		binlogReader, err := r.startSync(mysql.Position{Name: currBinlog.name, Pos: FileHeaderLen})
		if err != nil {
			return err
		}
		for {
			ev, err := binlogReader.GetEvent(r.tctx.Ctx)
			if err != nil {
				binlogReader.Close()
				return err
			}
			// break on first non-fake event(must be a format description event)
			if !utils.IsFakeRotateEvent(ev.Header) {
				currTs = ev.Header.Timestamp
				break
			}
		}
		binlogReader.Close()

		r.tctx.L().Info("min timestamp in binlog file", zap.Reflect("file", binlogFile), zap.Uint32("ts", currTs))

		if currTs >= targetTs {
			break
		}
	}
	if currTs >= targetTs {
		if prevTs == 0 {
			// ts of first binlog event in first binlog file >= targetTs
			r.targetBinlog = currBinlog
			r.tsBeforeFirstBinlog = true
		} else {
			// ts of first event in currBinlog >= targetTs, need to search from prevBinlog
			r.targetBinlog = prevBinlog
		}
	} else {
		// find from the last binlog file, i.e. currBinlog
		r.targetBinlog = currBinlog
	}
	r.lastBinlogFile = r.targetBinlog.name == binaryLogs[len(binaryLogs)-1].name
	return nil
}

func (r *binlogPosFinder) processGTIDRelatedEvent(ev *replication.BinlogEvent, prevSet gtid.Set) (gtid.Set, error) {
	ev, err := r.parser.Parse(ev.RawData)
	if err != nil {
		return nil, err
	}
	switch ev.Header.EventType {
	case replication.PREVIOUS_GTIDS_EVENT:
		newSet, err := event.GTIDsFromPreviousGTIDsEvent(ev)
		if err != nil {
			return nil, err
		}
		return newSet, nil
	case replication.MARIADB_GTID_EVENT:
		fallthrough
	case replication.GTID_EVENT:
		gtidStr, _ := event.GetGTIDStr(ev)
		if err := prevSet.Update(gtidStr); err != nil {
			return nil, err
		}
	case replication.MARIADB_GTID_LIST_EVENT:
		newSet, err := event.GTIDsFromMariaDBGTIDListEvent(ev)
		if err != nil {
			return nil, err
		}
		return newSet, nil
	}
	return prevSet, nil
}

func (r *binlogPosFinder) checkTransactionBoundaryEvent(ev *replication.BinlogEvent) (bool, error) {
	// we find the timestamp at transaction boundary
	// When there are GTID events in this binlog file, we use GTID event as the start event, else:
	// for DML
	// 		take a 'BEGIN' query event as the start event
	//		XID event or a 'COMMIT' query event as the end event
	// for DDL
	// 		one single query event acts as both the start and end event
	var transactionBeginEvent bool
	switch ev.Header.EventType {
	case replication.FORMAT_DESCRIPTION_EVENT:
		_, err := r.parser.Parse(ev.RawData)
		if err != nil {
			return false, err
		}
	case replication.GTID_EVENT:
		fallthrough
	case replication.ANONYMOUS_GTID_EVENT: // since 5.7, when GTID not enabled. we use this to avoid parsing query event
		fallthrough
	case replication.MARIADB_GTID_EVENT:
		r.everMetGTIDEvent = true
		transactionBeginEvent = true
	case replication.QUERY_EVENT:
		if !r.everMetGTIDEvent {
			// user may change session level binlog-format=statement, but it's an unusual operation, so we parse it every time
			// In MySQL 5.6.x without GTID, the timestamp of BEGIN is not the timestamp of the transaction,
			// To simplify implementation, we use timestamp of BEGIN as the transaction timestamp,
			// but this may cause some transaction with timestamp >= target timestamp be skipped.
			// TODO maybe add backtrace to support this case later
			ev2, err := r.parser.Parse(ev.RawData)
			if err != nil {
				return false, err
			}
			e := ev2.Event.(*replication.QueryEvent)
			switch string(e.Query) {
			case "BEGIN":
				transactionBeginEvent = true
				r.inTransaction = true
			case "COMMIT": // MyISAM use COMMIT to end transaction
				r.inTransaction = false
			default:
				if !r.inTransaction {
					// DDL
					transactionBeginEvent = true
				}
			}
		}
	case replication.XID_EVENT:
		r.inTransaction = false
	}
	return transactionBeginEvent, nil
}

// FindByTs get binlog location of first event or transaction with timestamp >= ts
// go-mysql has BinlogStreamer.GetEventWithStartTime, but it doesn't fit our need. Also we need to support relay log.
func (r *binlogPosFinder) FindByTs(ts int64) (*Location, PosType, error) {
	r.tctx.L().Info("target timestamp", zap.Int64("ts", ts))

	if err := r.initTargetBinlogFile(ts); err != nil {
		return nil, InvalidBinlogPos, err
	}

	if r.tsBeforeFirstBinlog {
		loc := NewLocation(r.flavor)
		loc.Position = mysql.Position{Name: r.targetBinlog.name, Pos: FileHeaderLen}
		return &loc, BelowLowerBoundBinlogPos, nil
	}

	targetTs := uint32(ts)
	position := mysql.Position{Name: r.targetBinlog.name, Pos: FileHeaderLen}
	gtidSet := gtid.MinGTIDSet(r.flavor)

	binlogReader, err := r.startSync(position)
	if err != nil {
		return nil, InvalidBinlogPos, err
	}
	defer binlogReader.Close()
	for {
		ev, err := binlogReader.GetEvent(r.tctx.Ctx)
		// let outer layer retry
		if err != nil {
			return nil, InvalidBinlogPos, err
		}
		if utils.IsFakeRotateEvent(ev.Header) {
			continue
		}

		transactionBeginEvent, err := r.checkTransactionBoundaryEvent(ev)
		if err != nil {
			return nil, InvalidBinlogPos, err
		}

		if transactionBeginEvent && ev.Header.Timestamp >= targetTs {
			break
		}
		position.Pos = ev.Header.LogPos

		if r.enableGTID {
			eventType := ev.Header.EventType
			if eventType == replication.PREVIOUS_GTIDS_EVENT ||
				eventType == replication.MARIADB_GTID_LIST_EVENT ||
				eventType == replication.GTID_EVENT ||
				eventType == replication.MARIADB_GTID_EVENT {
				gtidSet, err = r.processGTIDRelatedEvent(ev, gtidSet)
				if err != nil {
					return nil, InvalidBinlogPos, err
				}
			}
		}

		// still not found the timestamp after reached the end of this binlog file
		if int64(position.Pos) >= r.targetBinlog.size {
			// if it's the last binlog file, then this timestamp is out of range,
			// else the end of this binlog file is the position we want,
			// since the timestamp of the first event in next binlog >= target timestamp
			if r.lastBinlogFile {
				return nil, AboveUpperBoundBinlogPos, nil
			}
			break
		}
	}
	loc := InitLocation(position, gtidSet)
	return &loc, InRangeBinlogPos, nil
}
