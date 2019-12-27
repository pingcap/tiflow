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

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	// EmitDMLs saves the specified DMLs to the sink backend
	EmitDMLs(ctx context.Context, txn ...model.Txn) error
	// EmitDDL saves the specified DDL to the sink backend
	EmitDDL(ctx context.Context, txn model.Txn) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
}

// TableInfoGetter is used to get table info by table id of TiDB
type TableInfoGetter interface {
	TableByID(id int64) (info *schema.TableInfo, ok bool)
	GetTableIDByName(schema, table string) (int64, bool)
}
