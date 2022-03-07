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

// FakeBinlogName is used to bypass the checking of meta in task config when start-task with --start-time.
const FakeBinlogName = "start-task with --start-time"

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

	// fields used inside FindByTimestamp
	targetBinlog        binlogSize // target binlog file the timestamp may reside
	tsBeforeFirstBinlog bool       // whether the timestamp is before the first binlog
	lastBinlogFile      bool       // whether targetBinlog is the last binlog file

	// one binlog file can either be GTID enabled or not, cannot be mixed up
	// we mark it using this field to avoid parsing events.
	everMetGTIDEvent bool
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
	}
	return GetLocalBinaryLogs(r.relayDir)
}

func (r *binlogPosFinder) startSync(position mysql.Position) (reader.Reader, error) {
	if r.remote {
		binlogReader := reader.NewTCPReader(r.syncCfg)
		return binlogReader, binlogReader.StartSyncByPos(position)
	}
	binlogReader := reader.NewFileReader(&reader.FileReaderConfig{EnableRawMode: true})
	position.Name = path.Join(r.relayDir, position.Name)
	return binlogReader, binlogReader.StartSyncByPos(position)
}

func (r *binlogPosFinder) findMinTimestampOfBinlog(currBinlog binlogSize) (uint32, error) {
	var minTS uint32
	binlogReader, err := r.startSync(mysql.Position{Name: currBinlog.name, Pos: FileHeaderLen})
	if err != nil {
		return 0, err
	}
	for {
		ev, err := binlogReader.GetEvent(r.tctx.Ctx)
		if err != nil {
			binlogReader.Close()
			return 0, err
		}
		// break on first non-fake event(must be a format description event)
		if !utils.IsFakeRotateEvent(ev.Header) {
			minTS = ev.Header.Timestamp
			break
		}
	}
	binlogReader.Close()

	return minTS, nil
}

func (r *binlogPosFinder) initTargetBinlogFile(ts int64) error {
	targetTS := uint32(ts)
	var lastTS, minTS uint32
	var lastMid int
	binaryLogs, err := r.getBinlogFiles()
	if err != nil {
		return err
	}
	if len(binaryLogs) == 0 {
		// should not happen on a master with binlog enabled
		return errors.New("cannot find binlog files")
	}

	begin, end := 0, len(binaryLogs)-1
	for begin <= end {
		mid := (begin + end) / 2
		currBinlog := binaryLogs[mid]

		minTS, err = r.findMinTimestampOfBinlog(currBinlog)
		if err != nil {
			return err
		}

		r.tctx.L().Debug("min timestamp in binlog file", zap.Reflect("file", currBinlog), zap.Uint32("ts", minTS))

		lastTS = minTS
		lastMid = mid

		if minTS >= targetTS {
			end = mid - 1
		} else {
			// current binlog maybe the target binlog file, we'll backtrace to it later.
			begin = mid + 1
		}
	}
	if lastTS >= targetTS {
		if lastMid == 0 {
			// timestamp of first binlog event in first binlog file >= targetTS
			r.targetBinlog = binaryLogs[lastMid]
			r.tsBeforeFirstBinlog = true
		} else {
			// timestamp of first event in lastMid >= targetTS, need to search from previous binlog file
			r.targetBinlog = binaryLogs[lastMid-1]
		}
	} else {
		r.targetBinlog = binaryLogs[lastMid]
	}
	r.lastBinlogFile = r.targetBinlog.name == binaryLogs[len(binaryLogs)-1].name

	r.tctx.L().Info("target binlog file", zap.Reflect("file", r.targetBinlog),
		zap.Bool("before first binlog", r.tsBeforeFirstBinlog),
		zap.Bool("last binlog", r.lastBinlogFile))

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
	case replication.MARIADB_GTID_LIST_EVENT:
		newSet, err := event.GTIDsFromMariaDBGTIDListEvent(ev)
		if err != nil {
			return nil, err
		}
		return newSet, nil
	case replication.MARIADB_GTID_EVENT, replication.GTID_EVENT:
		gtidStr, _ := event.GetGTIDStr(ev)
		if err := prevSet.Update(gtidStr); err != nil {
			return nil, err
		}
	}
	return prevSet, nil
}

func (r *binlogPosFinder) checkTransactionBeginEvent(ev *replication.BinlogEvent) (bool, error) {
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
	case replication.GTID_EVENT, replication.ANONYMOUS_GTID_EVENT, replication.MARIADB_GTID_EVENT:
		// since 5.7, when GTID not enabled, mysql add a anonymous gtid event. we use this to avoid parsing query event
		r.everMetGTIDEvent = true
		transactionBeginEvent = true
	case replication.QUERY_EVENT:
		if !r.everMetGTIDEvent {
			// user may change session level binlog-format=statement, but it's an unusual operation, so we parse it every time
			// In MySQL 5.6.x without GTID, the timestamp of BEGIN is the timestamp of the first statement in the transaction,
			// not the commit timestamp of the transaction.
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

// FindByTimestamp get binlog location of first event or transaction with timestamp >= ts
// go-mysql has BinlogStreamer.GetEventWithStartTime, but it doesn't fit our need. And we need to support relay log.
// if posType != AboveUpperBoundBinlogPos, then location is the target location we want.
// if posType == BelowLowerBoundBinlogPos, master binlog may have purged.
func (r *binlogPosFinder) FindByTimestamp(ts int64) (*Location, PosType, error) {
	r.tctx.L().Info("target timestamp", zap.Int64("ts", ts))

	if err := r.initTargetBinlogFile(ts); err != nil {
		return nil, InvalidBinlogPos, err
	}

	targetTS := uint32(ts)
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

		transactionBeginEvent, err := r.checkTransactionBeginEvent(ev)
		if err != nil {
			return nil, InvalidBinlogPos, err
		}

		if transactionBeginEvent && ev.Header.Timestamp >= targetTS {
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
				// we meet PREVIOUS_GTIDS_EVENT or MARIADB_GTID_LIST_EVENT first, so break after get previous GTIDs
				if r.tsBeforeFirstBinlog {
					break
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
	if r.tsBeforeFirstBinlog {
		// always return the position of the first event in target binlog
		loc := InitLocation(mysql.Position{Name: r.targetBinlog.name, Pos: FileHeaderLen}, gtidSet)
		return &loc, BelowLowerBoundBinlogPos, nil
	}
	loc := InitLocation(position, gtidSet)
	return &loc, InRangeBinlogPos, nil
}
