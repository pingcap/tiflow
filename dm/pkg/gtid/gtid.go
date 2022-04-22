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
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// ParserGTID parses GTID from string. If the flavor is not specified, it will
// try mysql GTID first and then MariaDB GTID.
func ParserGTID(flavor, gtidStr string) (mysql.GTIDSet, error) {
	var (
		err  error
		gtid mysql.GTIDSet
	)

	if len(flavor) == 0 && len(gtidStr) == 0 {
		return nil, errors.Errorf("empty flavor with empty gtid is invalid")
	}

	fla := flavor
	switch fla {
	case mysql.MySQLFlavor, mysql.MariaDBFlavor:
		gtid, err = mysql.ParseGTIDSet(fla, gtidStr)
	case "":
		fla = mysql.MySQLFlavor
		gtid, err = mysql.ParseGTIDSet(fla, gtidStr)
		if err != nil {
			fla = mysql.MariaDBFlavor
			gtid, err = mysql.ParseGTIDSet(fla, gtidStr)
		}
	default:
		err = terror.ErrNotSupportedFlavor.Generate(flavor)
	}

	return gtid, err
}

// MinGTIDSet returns the min GTID set which is empty. If the flavor is not
// specified, it wil use mysql GTID.
func MinGTIDSet(flavor string) mysql.GTIDSet {
	if flavor != mysql.MariaDBFlavor && flavor != mysql.MySQLFlavor {
		flavor = mysql.MySQLFlavor
	}

	gset, err := ParserGTID(flavor, "")
	if err != nil {
		// this should not happen
		panic(err)
	}
	return gset
}
