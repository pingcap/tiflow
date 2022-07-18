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
	"time"

	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
)

// backend indicates a transaction backend like MySQL, TiDB, ...
type backend interface {
	// onTxnEvent handles one TxnCallbackableEvent.
	onTxnEvent(*eventsink.TxnCallbackableEvent) error
	// timer gets a timer so that onTimeout can be called periodcally.
	timer() *time.Timer
	onTimeout() error
}
