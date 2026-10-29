// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"sync"
	"time"
)

// Allow at most 384 rows across the result channel and the two processing
// stages: 382 queued, one being prepared, and one being consumed. This is a
// row-count bound, not a byte bound; a single row can be arbitrarily large.
const causalityRowQueueSize = 3*128 - 2

type preparedCausalityJob struct {
	job     *job
	keys    []string
	keyTime time.Duration
}

// causalityKeyPipeline prepares keys with one worker and publishes each row
// directly. The single producer preserves input order, including barriers, so
// no per-batch completion notification or reorder queue is needed. Key evaluation
// uses the original RowChange context and expressions, as in the serial path.
type causalityKeyPipeline struct {
	ordered chan preparedCausalityJob
	wg      sync.WaitGroup
}

func newCausalityKeyPipeline(in <-chan *job) *causalityKeyPipeline {
	p := &causalityKeyPipeline{ordered: make(chan preparedCausalityJob, causalityRowQueueSize)}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.ordered)
		for j := range in {
			prepared := preparedCausalityJob{job: j}
			if j.tp == dml {
				start := time.Now()
				prepared.keys = j.dml.CausalityKeys()
				prepared.keyTime = time.Since(start)
			}
			p.ordered <- prepared
		}
	}()
	return p
}
