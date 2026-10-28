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

package sqlmodel

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// CausalityKeyGenerator owns the mutable evaluation state of one key-preparation
// worker. Row data and schema are immutable, but session contexts and expression
// instances must not be shared between workers. Reset releases caller-scoped schema
// references; no table cache grows with the lifetime of a replication task.
type CausalityKeyGenerator struct {
	ctx     sessionctx.Context
	handles map[*WhereHandle]*WhereHandle
}

// NewCausalityKeyGenerator takes a private session context with the same settings
// as the row changes. The generator and context have a single goroutine owner.
func NewCausalityKeyGenerator(ctx sessionctx.Context) *CausalityKeyGenerator {
	return &CausalityKeyGenerator{ctx: ctx, handles: make(map[*WhereHandle]*WhereHandle)}
}

// Keys leaves row fields unchanged. Shared expression templates initialize
// under their existing sync.Once; evaluation uses worker-private clones.
func (g *CausalityKeyGenerator) Keys(row *RowChange) []string {
	r := *row
	r.tiSessionCtx = g.ctx
	r.lazyInitWhereHandle()
	if cache := r.whereHandle.hiddenGeneratedColumnExprCache; cache != nil {
		handle, ok := g.handles[r.whereHandle]
		if !ok {
			copyHandle := *r.whereHandle
			copyCache := newGeneratedColumnExprCache(cache.sourceTableInfo)
			copyCache.once.Do(func() {
				exprs, _, valid := cache.getOrBuildExprs(g.ctx)
				copyCache.exprCtx = generatedColumnExprContext(g.ctx)
				copyCache.exprs = make(map[int]expression.Expression, len(exprs))
				for offset, expr := range exprs {
					copyCache.exprs[offset] = expr.Clone()
				}
				copyCache.ok = valid
			})
			copyHandle.hiddenGeneratedColumnExprCache = copyCache
			handle = &copyHandle
			g.handles[r.whereHandle] = handle
		}
		r.whereHandle = handle
	}
	return r.CausalityKeys()
}

// Reset releases schema/expression references after the current preparation unit.
// The row-channel pipeline calls it after each row.
func (g *CausalityKeyGenerator) Reset() {
	clear(g.handles)
}
