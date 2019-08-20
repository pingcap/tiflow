package cdc

import (
	"context"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/types"
)

// rowFetcher convert kv to row
type rowFetcher struct {
}

func newRowFetcher() *rowFetcher {
	panic("TODO")
}

func (c *rowFetcher) TableInfoForKey(ctx context.Context, key []byte, ts uint64) (table *model.TableInfo, err error) {
	panic("TODO")
}

func (c *rowFetcher) GetRowByKey(ctx context.Context, key []byte, ts uint64) (datums []types.Datum, table *model.TableInfo, err error) {
	panic("TODO")
}
