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

	"github.com/pingcap/tidb-cdc/cdc/txn"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	Run(ctx context.Context, txns <-chan txn.Txn) error
	Success() <-chan txn.Txn
	Error() <-chan error
	Close() error
	// TODO: Replace Emit completely with Run
	Emit(ctx context.Context, txn txn.Txn) error
}
