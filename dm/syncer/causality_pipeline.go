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

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
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
// no per-batch completion notification or reorder queue is needed.
type causalityKeyPipeline struct {
	ordered chan preparedCausalityJob
	wg      sync.WaitGroup
}

func newCausalityKeyPipeline(in <-chan *job, ctx sessionctx.Context) *causalityKeyPipeline {
	p := &causalityKeyPipeline{ordered: make(chan preparedCausalityJob, causalityRowQueueSize)}
	generator := sqlmodel.NewCausalityKeyGenerator(privateCausalityContext(ctx))
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer close(p.ordered)
		for j := range in {
			prepared := preparedCausalityJob{job: j}
			if j.tp == dml {
				start := time.Now()
				prepared.keys = generator.Keys(j.dml)
				prepared.keyTime = time.Since(start)
			}
			// No schema/expression references survive into the next row.
			generator.Reset()
			p.ordered <- prepared
		}
	}()
	return p
}

// Type conversion can update statement state, so every worker uses its own
// context. Copy the fixed apply settings, not mutable session/statement objects.
func privateCausalityContext(source sessionctx.Context) sessionctx.Context {
	if source == nil {
		return utils.NewSessionCtx(nil)
	}
	vars := source.GetSessionVars()
	mode, _ := vars.GetSystemVar("sql_mode")
	charset, collation := vars.GetCharsetInfo()
	ctx := utils.NewSessionCtx(map[string]string{
		"sql_mode": mode, "character_set_connection": charset, "collation_connection": collation,
	})
	ctx.GetSessionVars().TimeZone = vars.Location()
	ctx.GetSessionVars().StmtCtx.SetTimeZone(vars.Location())
	return ctx
}
