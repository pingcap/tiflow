package main

import (
	"context"
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
)

func AddTable(ctx context.Context, tableID int64) error {
	fmt.Println("addtable")
	return nil
}
func RemoveTable(ctx context.Context, tableID int64) error {
	fmt.Println("remove")
	return nil
}
func RowChanged(context.Context, ...*model.RowChangedEvent) error {
	fmt.Println("row changed")
	return nil
}
func DDL(context.Context, *model.DDLEvent) error {
	fmt.Println("ddl")
	return nil
}
