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

package gtid

import (
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

var (
	emptyMySQLGTIDSet, _   = mysql.ParseMysqlGTIDSet("")
	emptyMariaDBGTIDSet, _ = mysql.ParseMariadbGTIDSet("")
)

// CheckGTIDSetEmpty is used to check whether a GTID set is zero.
func CheckGTIDSetEmpty(gSet mysql.GTIDSet) bool {
	return gSet == nil || gSet.Equal(emptyMySQLGTIDSet) || gSet.Equal(emptyMariaDBGTIDSet)
}

// ParserGTID parses GTID from string. If the flavor is not specified, it will
// try mysql GTID first and then MariaDB GTID.
func ParserGTID(flavor, gtidStr string) (mysql.GTIDSet, error) {
	var (
		err  error
		gtid mysql.GTIDSet
	)

	if len(flavor) == 0 && len(gtidStr) == 0 {
		// regard as mysql, mariadb always enabled gtid
		return mysql.ParseGTIDSet(mysql.MySQLFlavor, "")
	}

	fla := flavor
	switch fla {
	case mysql.MySQLFlavor:
		if IsZeroMySQLGTIDSet(gtidStr) {
			gtid, err = mysql.ParseGTIDSet(fla, "")
		} else {
			gtid, err = mysql.ParseGTIDSet(fla, gtidStr)
		}
	case mysql.MariaDBFlavor:
		if IsZeroMariaDBGTIDSet(gtidStr) {
			gtid, err = mysql.ParseGTIDSet(fla, "")
		} else {
			gtid, err = parseMariaDBGTIDSet(gtidStr)
		}
	case "":
		fla = mysql.MySQLFlavor
		gtid, err = mysql.ParseGTIDSet(fla, gtidStr)
		if err != nil {
			gtid, err = parseMariaDBGTIDSet(gtidStr)
		}
	default:
		err = terror.ErrNotSupportedFlavor.Generate(flavor)
	}

	return gtid, err
}

// parseMariaDBGTIDSet parses a MariaDB GTID set and keeps one GTID for each
// domain. The GTID set saved by older versions of DM may contain multiple GTIDs
// in one domain, e.g. "0-10-200,0-9-100" after the server ID changes. go-mysql
// keeps the last one of each domain, which may be the older one, so we keep the
// one with the largest sequence number.
func parseMariaDBGTIDSet(gtidStr string) (mysql.GTIDSet, error) {
	gSet := &mysql.MariadbGTIDSet{Sets: make(map[uint32]*mysql.MariadbGTID)}
	if gtidStr == "" {
		return gSet, nil
	}
	for _, s := range strings.Split(gtidStr, ",") {
		g, err := mysql.ParseMariadbGTID(s)
		if err != nil {
			return nil, err
		}
		if old, ok := gSet.Sets[g.DomainID]; !ok || g.SequenceNumber > old.SequenceNumber {
			gSet.Sets[g.DomainID] = g
		}
	}
	return gSet, nil
}

// ZeroGTIDSet returns an empty GTID set. The flavor must be specified.
func ZeroGTIDSet(flavor string) (mysql.GTIDSet, error) {
	return ParserGTID(flavor, "")
}

// MustZeroGTIDSet is used when you can make sure the flavor is valid.
func MustZeroGTIDSet(flavor string) mysql.GTIDSet {
	gtid, err := ZeroGTIDSet(flavor)
	if err != nil {
		panic(err)
	}
	return gtid
}

// IsZeroMySQLGTIDSet is used to meet this usage: when user wants to start binlog
// replication from scratch, a "uuid:0" (MySQL flavor) or "0-0-0" (mariaDB) GTID
// set must be written, in order to distinguish that user forgets to write it.
func IsZeroMySQLGTIDSet(gStr string) bool {
	sp := strings.Split(gStr, ",")
	if len(sp) != 1 {
		return false
	}

	sep := strings.Split(sp[0], ":")
	if len(sep) != 2 {
		return false
	}
	interval := strings.TrimSpace(sep[1])
	return interval == "0"
}

// IsZeroMariaDBGTIDSet is used to meet this usage: when user wants to start binlog
// replication from scratch, a "uuid:0" (MySQL flavor) or "0-0-0" (mariaDB) GTID
// set must be written, in order to distinguish that user forgets to write it.
//
// For MariaDB, the GTID set like "0-0-0" will confuse IsZeroGTIDSet function,
// so we also need to check the interval part.
func IsZeroMariaDBGTIDSet(gStr string) bool {
	sp := strings.Split(gStr, ",")
	if len(sp) != 1 {
		return false
	}

	sep := strings.Split(sp[0], "-")
	if len(sep) != 3 {
		return false
	}
	interval := strings.TrimSpace(sep[2])
	return interval == "0"
}
