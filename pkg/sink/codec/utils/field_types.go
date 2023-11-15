package utils

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
func NewTextFieldType(ft *types.FieldType) *types.FieldType {
	ft.SetCollate(mysql.DefaultCollationName)
	ft.SetCharset(mysql.DefaultCharset)
	return ft
}
