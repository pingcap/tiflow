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
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
)

// SetBinChsClnFlag set the binary charset flag.
func SetBinChsClnFlag(ft *types.FieldType) *types.FieldType {
	types.SetBinChsClnFlag(ft)
	return ft
}

// SetUnsigned set the unsigned flag.
func SetUnsigned(ft *types.FieldType) *types.FieldType {
	ft.SetFlag(uint(model.UnsignedFlag))
	return ft
}

// SetFlag set the specific flag to the ft
func SetFlag(ft *types.FieldType, flag uint) *types.FieldType {
	ft.SetFlag(flag)
	return ft
}

// SetElems set the elems to the ft
func SetElems(ft *types.FieldType, elems []string) *types.FieldType {
	ft.SetElems(elems)
	return ft
}

// NewTextFieldType create a new text field type.
func NewTextFieldType(tp byte) *types.FieldType {
	ft := types.NewFieldType(tp)
	ft.SetCollate(mysql.DefaultCollationName)
	ft.SetCharset(mysql.DefaultCharset)
	return ft
}
