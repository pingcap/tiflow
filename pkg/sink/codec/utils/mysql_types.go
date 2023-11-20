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

package utils

import (
	"strings"

	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
)

// WithUnsigned4MySQLType add `unsigned` keyword.
// it should have the form `t unsigned`, such as `int unsigned`
func WithUnsigned4MySQLType(mysqlType string, unsigned bool) string {
	if unsigned && mysqlType != "bit" && mysqlType != "year" {
		return mysqlType + " unsigned"
	}
	return mysqlType
}

// WithZerofill4MySQLType add `zerofill` keyword.
func WithZerofill4MySQLType(mysqlType string, zerofill bool) string {
	if zerofill &&
		!strings.HasPrefix(mysqlType, "bit") &&
		!strings.HasPrefix(mysqlType, "year") {
		return mysqlType + " zerofill"
	}
	return mysqlType
}

// GetMySQLType get the mysql type string.
func GetMySQLType(
	fieldType *types.FieldType, flag model.ColumnFlagType, fullType bool,
) string {
	if !fullType {
		result := types.TypeToStr(fieldType.GetType(), fieldType.GetCharset())
		result = WithUnsigned4MySQLType(result, flag.IsUnsigned())
		result = WithZerofill4MySQLType(result, flag.IsZerofill())

		return result
	}

	result := fieldType.InfoSchemaStr()
	result = WithZerofill4MySQLType(result, flag.IsZerofill())
	return result
}
