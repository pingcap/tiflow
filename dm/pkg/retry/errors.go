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

package retry

import (
	"database/sql/driver"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// some error reference: https://docs.pingcap.com/tidb/stable/tidb-limitations#limitations-on-a-single-table
var (
	// UnsupportedDDLMsgs list the error messages of some unsupported DDL in TiDB.
	UnsupportedDDLMsgs = []string{
		"can't drop column with index",
		"with tidb_enable_change_multi_schema is disable", // https://github.com/pingcap/tidb/pull/29526
		"unsupported add column",
		"unsupported modify column",
		"unsupported modify charset",
		"unsupported modify collate",
		"unsupported drop integer primary key",
		"Unsupported collation",
		"Invalid default value for",
		"Unsupported drop primary key",
		"Error 1059: Identifier name", // Limitations on identifier length
		"Error 1117: Too many columns",
		"Error 1069: Too many keys specified",
	}

	// UnsupportedDMLMsgs list the error messages of some un-recoverable DML, which is used in task auto recovery.
	UnsupportedDMLMsgs = []string{
		"Error 1062: Duplicate",
		"Error 1406: Data too long for column",
		"Error 1366", // Incorrect %s value: '%s' for column '%s' at row %d
		"Error 8025: entry too large",
	}

	// ReplicationErrMsgs list the error message of un-recoverable replication error.
	ReplicationErrMsgs = []string{
		"Could not find first log file name in binary log index file",
		"The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires",
	}

	// ParseRelayLogErrMsgs list the error messages of some un-recoverable relay log parsing error, which is used in task auto recovery.
	ParseRelayLogErrMsgs = []string{
		"binlog checksum mismatch, data may be corrupted",
		"get event err EOF",
	}

	// UnresumableErrCodes is a set of unresumeable err codes.
	UnresumableErrCodes = map[int32]struct{}{
		int32(terror.ErrSyncUnitDDLWrongSequence.Code()):    {},
		int32(terror.ErrDumpUnitGlobalLock.Code()):          {},
		int32(terror.ErrDumpUnitRuntime.Code()):             {},
		int32(terror.ErrSyncerUnitDMLColumnNotMatch.Code()): {},
		int32(terror.ErrSyncerCancelledDDL.Code()):          {},
		int32(terror.ErrLoadLightningRuntime.Code()):        {},
	}

	// UnresumableRelayErrCodes is a set of unresumeable relay unit err codes.
	UnresumableRelayErrCodes = map[int32]struct{}{
		int32(terror.ErrRelayUUIDSuffixNotValid.Code()):     {},
		int32(terror.ErrRelayUUIDSuffixLessThanPrev.Code()): {},
		int32(terror.ErrRelayBinlogNameNotValid.Code()):     {},
		int32(terror.ErrRelayNoCurrentUUID.Code()):          {},
		int32(terror.ErrRelayLogDirpathEmpty.Code()):        {},
	}
)

// IsConnectionError tells whether this error should reconnect to Database.
// Return true also means caller can retry sql safely.
func IsConnectionError(err error) bool {
	err = errors.Cause(err)
	switch err {
	case driver.ErrBadConn, tmysql.ErrBadConn, gmysql.ErrBadConn:
		return true
	}
	return false
}

// IsUnretryableConnectionError checks whether it's an unretryable connection error or not.
func IsUnretryableConnectionError(err error) bool {
	// Can't ensure whether the last write has reached the downstream or not.
	// If the last write isn't idempotent, retry it may cause problems.
	return errors.Cause(err) == dmysql.ErrInvalidConn
}
