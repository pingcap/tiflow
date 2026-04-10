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

package cancelcause

import (
	"context"

	"github.com/pingcap/tidb/lightning/pkg/importinto"
)

// WorkerStopCause returns the cancellation cause used when DM is intentionally stopping
// a worker or subtask (e.g. user requested stop).
func WorkerStopCause() error {
	return context.Canceled
}

// WorkerFailoverCause returns the cancellation cause used when a DM worker instance is
// shutting down due to failover / rebalance, and a new instance is expected to take over.
//
// This uses TiDB Lightning's sentinel so IMPORT INTO jobs are not canceled during takeover.
func WorkerFailoverCause() error {
	return importinto.ErrFailoverCancel
}
