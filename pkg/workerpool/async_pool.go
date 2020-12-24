// Copyright 2020 PingCAP, Inc.
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

package workerpool

import "context"

// AsyncPool provides a simple Goroutine pool, where the order in which jobs are run is non-deterministic.
type AsyncPool interface {
	// Go mimics the semantics of the "go" keyword, with the only difference being the `ctx` parameter,
	// which is used to cancel **the submission of task**.
	// **All** tasks successfully submitted will be run eventually, as long as Run are called infinitely many times.
	// Go might block when the AsyncPool is not running.
	Go(ctx context.Context, f func()) error

	// Run runs the AsyncPool.
	Run(ctx context.Context) error
}
