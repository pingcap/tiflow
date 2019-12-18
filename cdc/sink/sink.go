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

package sink

import (
	"context"

	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	// Emit saves the specified transactions to the sink backend
	Emit(ctx context.Context, txns ...model.Txn) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
}

// TableInfoGetter is used to get table info by table id of TiDB
type TableInfoGetter interface {
	TableByID(id int64) (info *timodel.TableInfo, ok bool)
	GetTableIDByName(schema, table string) (int64, bool)
}
