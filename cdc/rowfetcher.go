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
