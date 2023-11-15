package utils

import (
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
)

func SetBinChsClnFlag(ft *types.FieldType) *types.FieldType {
	types.SetBinChsClnFlag(ft)
	return ft
}

func SetUnsigned(ft *types.FieldType) *types.FieldType {
	ft.SetFlag(uint(model.UnsignedFlag))
	return ft
}

func SetFlag(ft *types.FieldType, flag uint) *types.FieldType {
	ft.SetFlag(flag)
	return ft
}

func SetElems(ft *types.FieldType, elems []string) *types.FieldType {
	ft.SetElems(elems)
	return ft
}

func NewTextFieldType(ft *types.FieldType) *types.FieldType {
	ft.SetCollate(mysql.DefaultCollationName)
	ft.SetCharset(mysql.DefaultCharset)
	return ft
}
