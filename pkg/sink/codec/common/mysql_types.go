// Copyright 2023 PingCAP, Inc.
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

package common

import (
	"strings"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
)

// when encoding the canal format, for unsigned mysql type, add `unsigned` keyword.
// it should have the form `t unsigned`, such as `int unsigned`
func withUnsigned4MySQLType(mysqlType string, unsigned bool) string {
	if unsigned && mysqlType != "bit" && mysqlType != "year" {
		return mysqlType + " unsigned"
	}
	return mysqlType
}

func withZerofill4MySQLType(mysqlType string, zerofill bool) string {
	if zerofill && !strings.HasPrefix(mysqlType, "year") {
		return mysqlType + " zerofill"
	}
	return mysqlType
}

// GetMySQLType get the mysql type from column info
func GetMySQLType(columnInfo *timodel.ColumnInfo, fullType bool) string {
	if !fullType {
		result := types.TypeToStr(columnInfo.GetType(), columnInfo.GetCharset())
		result = withUnsigned4MySQLType(result, mysql.HasUnsignedFlag(columnInfo.GetFlag()))
		result = withZerofill4MySQLType(result, mysql.HasZerofillFlag(columnInfo.GetFlag()))
		return result
	}
	return columnInfo.GetTypeDesc()
}

// ExtractBasicMySQLType return the mysql type
func ExtractBasicMySQLType(mysqlType string) byte {
	for i := 0; i < len(mysqlType); i++ {
		if mysqlType[i] == '(' || mysqlType[i] == ' ' {
			return types.StrToType(mysqlType[:i])
		}
	}

	return types.StrToType(mysqlType)
}

// IsBinaryMySQLType return true if the given mysqlType string is a binary type
func IsBinaryMySQLType(mysqlType string) bool {
	return strings.Contains(mysqlType, "blob") || strings.Contains(mysqlType, "binary")
}
