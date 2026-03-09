// Copyright 2025 PingCAP, Inc.
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

package checker

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser"
	mysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// PrimaryKeyChecker checks whether upstream tables contain a primary key.
type PrimaryKeyChecker struct {
	upstreamDBs map[string]*conn.BaseDB
	tableMap    map[string]map[filter.Table][]filter.Table
	dumpThreads int
}

// NewPrimaryKeyChecker returns a RealChecker that only checks existence of primary key on upstream tables.
func NewPrimaryKeyChecker(
	upstreamDBs map[string]*conn.BaseDB,
	tableMap map[string]map[filter.Table][]filter.Table,
	dumpThreads int,
) RealChecker {
	if dumpThreads == 0 {
		dumpThreads = 1
	}
	c := &PrimaryKeyChecker{
		upstreamDBs: upstreamDBs,
		tableMap:    tableMap,
		dumpThreads: dumpThreads,
	}
	log.L().Logger.Debug("check primary key existence", zap.Int("channel pool size", dumpThreads))
	return c
}

type primaryKeyWorker struct {
	c              *PrimaryKeyChecker
	lastSourceID   string
	upstreamParser *parser.Parser
}

func (w *primaryKeyWorker) handle(ctx context.Context, item *checkItem) ([]*incompatibilityOption, error) {
	var (
		ret   = make([]*incompatibilityOption, 0, 1)
		table = item.upstreamTable
	)
	if w.lastSourceID == "" || w.lastSourceID != item.sourceID {
		w.lastSourceID = item.sourceID
		var err error
		w.upstreamParser, err = dbutil.GetParserForDB(ctx, w.c.upstreamDBs[w.lastSourceID].DB)
		if err != nil {
			return nil, err
		}
	}

	db := w.c.upstreamDBs[item.sourceID].DB
	upstreamSQL, err := dbutil.GetCreateTableSQL(ctx, db, table.Schema, table.Name)
	if err != nil {
		// continue if table was deleted when checking
		if isMySQLError(err, mysql.ErrNoSuchTable) {
			return nil, nil
		}
		return nil, err
	}

	upstreamStmt, err := getCreateTableStmt(w.upstreamParser, upstreamSQL)
	if err != nil {
		opt := &incompatibilityOption{
			state:      StateWarning,
			tableID:    dbutil.TableName(table.Schema, table.Name),
			errMessage: err.Error(),
		}
		ret = append(ret, opt)
		//nolint:nilerr
		return ret, nil
	}

	// check primary key existence
	pkuk := getPKAndUK(upstreamStmt)
	if _, ok := pkuk["PRIMARY"]; !ok {
		ret = append(ret, &incompatibilityOption{
			state:       StateFailure,
			tableID:     dbutil.TableName(table.Schema, table.Name),
			instruction: "You need to set primary key for the upstream table. Otherwise replication efficiency may be low, and correctness (e.g., deduplication and idempotency) may be affected if no primary key exists.",
			errMessage:  "primary key does not exist",
		})
	}

	return ret, nil
}

// Check implements RealChecker.
func (c *PrimaryKeyChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check existence of primary key in upstream tables",
		State: StateSuccess,
	}

	start := time.Now()
	sourceIDs := maps.Keys(c.tableMap)
	concurrency, err := GetConcurrency(ctx, sourceIDs, c.upstreamDBs, c.dumpThreads)
	if err != nil {
		markCheckError(r, err)
		return r
	}

	resultInstructions := map[string]struct{}{}

	pool := NewWorkerPoolWithContext[*checkItem, []*incompatibilityOption](ctx, func(opts []*incompatibilityOption) {
		for _, opt := range opts {
			tableMsg := "table " + opt.tableID + " "
			switch opt.state {
			case StateWarning:
				if r.State != StateFailure {
					r.State = StateWarning
				}
				e := NewWarn("%s", tableMsg+opt.errMessage)
				if _, ok := resultInstructions[opt.instruction]; !ok && opt.instruction != "" {
					resultInstructions[opt.instruction] = struct{}{}
				}
				r.Errors = append(r.Errors, e)
			case StateFailure:
				r.State = StateFailure
				e := NewError("%s", tableMsg+opt.errMessage)
				if _, ok := resultInstructions[opt.instruction]; !ok && opt.instruction != "" {
					resultInstructions[opt.instruction] = struct{}{}
				}
				r.Errors = append(r.Errors, e)
			}
		}
	})

	for i := 0; i < concurrency; i++ {
		worker := &primaryKeyWorker{c: c}
		pool.Go(worker.handle)
	}

	dispatchTableItemWithDownstreamTable(c.tableMap, pool)

	if err := pool.Wait(); err != nil {
		markCheckError(r, err)
		return r
	}

	// combine instructions
	instrs := make([]string, 0, len(resultInstructions))
	for k := range resultInstructions {
		instrs = append(instrs, k)
	}
	if len(instrs) > 0 {
		r.Instruction = strings.Join(instrs, "\n")
	}

	log.L().Logger.Info("check primary key over", zap.Duration("spend time", time.Since(start)))
	return r
}

// Name implements RealChecker.
func (c *PrimaryKeyChecker) Name() string {
	return "primary key existence check"
}
