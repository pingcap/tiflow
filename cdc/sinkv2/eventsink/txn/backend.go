// Copyright 2022 PingCAP, Inc.
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

package txn

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
)

// backend indicates a transaction backend like MySQL, TiDB, ...
type backend interface {
	// OnTxnEvent handles one TxnCallbackableEvent.
	OnTxnEvent(e *eventsink.TxnCallbackableEvent) (needFlush bool)

	// Flush pending events in the backend.
	Flush(ctx context.Context) error

	// To reduce latency for low throughput cases.
	MaxFlushInterval() time.Duration

	// Close the backend.
	Close() error
}
