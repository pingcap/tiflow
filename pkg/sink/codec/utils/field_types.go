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

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
)

// SetBinChsClnFlag set the binary charset flag.
func SetBinChsClnFlag(ft *types.FieldType) *types.FieldType {
	ft.SetCharset(charset.CharsetBin)
	ft.SetCollate(charset.CollationBin)
	ft.AddFlag(mysql.BinaryFlag)
	return ft
}

// SetUnsigned set the unsigned flag.
func SetUnsigned(ft *types.FieldType) *types.FieldType {
	ft.SetFlag(uint(model.UnsignedFlag))
	return ft
}

// SetElems set the elems to the ft
func SetElems(ft *types.FieldType, elems []string) *types.FieldType {
	ft.SetElems(elems)
	return ft
}

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
