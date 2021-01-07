// Copyright 2021 PingCAP, Inc.
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
	"math/rand"
	"sync"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.uber.org/zap"
)

type managerSuite struct{}

var _ = check.Suite(&managerSuite{})

type checkSink struct {
	*check.C
	rows           []*model.RowChangedEvent
	rowsMu         sync.Mutex
	lastResolvedTs uint64
}

func (c *checkSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	panic("unreachable")
}

func (c *checkSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	c.rowsMu.Lock()
	defer c.rowsMu.Unlock()
	for _, row := range rows {
		log.Info("rows in check sink", zap.Reflect("row", row))
	}
	c.rows = append(c.rows, rows...)
	return nil
}

func (c *checkSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	panic("unreachable")
}

func (c *checkSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	c.rowsMu.Lock()
	defer c.rowsMu.Unlock()
	log.Info("flush in check sink", zap.Uint64("resolved", resolvedTs))
	var newRows []*model.RowChangedEvent
	for _, row := range c.rows {
		c.Assert(row.CommitTs, check.Greater, c.lastResolvedTs)
		if row.CommitTs > resolvedTs {
			newRows = append(newRows, row)
		}
	}

	c.Assert(c.lastResolvedTs, check.LessEqual, resolvedTs)
	c.lastResolvedTs = resolvedTs
	c.rows = newRows

	return c.lastResolvedTs, nil
}

func (c *checkSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("unreachable")
}

func (c *checkSink) Close() error {
	return nil
}

func (s *managerSuite) TestManagerRandom(c *check.C) {
	defer testleak.AfterTest(c)()
	manager := NewManager(&checkSink{C: c}, 0)
	defer manager.Close()
	goroutineNum := 10
	rowNum := 100
	var wg sync.WaitGroup
	tableSinks := make([]Sink, goroutineNum)
	for i := 0; i < goroutineNum; i++ {
		tableSinks[i] = manager.CreateTableSink(model.TableID(i), 0)
	}
	for i := 0; i < goroutineNum; i++ {
		i := i
		tableSink := tableSinks[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			var lastResolvedTs uint64
			for j := 1; j < rowNum; j++ {
				if rand.Intn(10) == 0 {
					resolvedTs := lastResolvedTs + uint64(rand.Intn(j-int(lastResolvedTs)))
					_, err := tableSink.FlushRowChangedEvents(ctx, resolvedTs)
					c.Assert(err, check.IsNil)
					lastResolvedTs = resolvedTs
				} else {
					err := tableSink.EmitRowChangedEvents(ctx, &model.RowChangedEvent{
						Table:    &model.TableName{TableID: int64(i)},
						CommitTs: uint64(j),
					})
					c.Assert(err, check.IsNil)
				}
			}
			_, err := tableSink.FlushRowChangedEvents(ctx, uint64(rowNum))
			c.Assert(err, check.IsNil)
		}()
	}
	wg.Wait()
}
