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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 16)
	manager := NewManager(ctx, &checkSink{C: c}, errCh, 0)
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
	cancel()
	time.Sleep(1 * time.Second)
	close(errCh)
	for err := range errCh {
		c.Assert(err, check.IsNil)
	}
}

func (s *managerSuite) TestManagerAddRemoveTable(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 16)
	manager := NewManager(ctx, &checkSink{C: c}, errCh, 0)
	defer manager.Close()
	goroutineNum := 100
	var wg sync.WaitGroup
	const ExitSignal = uint64(math.MaxUint64)

	var maxResolvedTs uint64
	tableSinks := make([]Sink, 0, goroutineNum)
	closeChs := make([]chan struct{}, 0, goroutineNum)
	runTableSink := func(index int64, sink Sink, startTs uint64, close chan struct{}) {
		defer wg.Done()
		ctx := context.Background()
		lastResolvedTs := startTs
		for {
			select {
			case <-close:
				return
			default:
			}
			resolvedTs := atomic.LoadUint64(&maxResolvedTs)
			if resolvedTs == ExitSignal {
				return
			}
			if resolvedTs == lastResolvedTs {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			for i := lastResolvedTs + 1; i <= resolvedTs; i++ {
				err := sink.EmitRowChangedEvents(ctx, &model.RowChangedEvent{
					Table:    &model.TableName{TableID: index},
					CommitTs: i,
				})
				c.Assert(err, check.IsNil)
			}
			_, err := sink.FlushRowChangedEvents(ctx, resolvedTs)
			c.Assert(err, check.IsNil)
			lastResolvedTs = resolvedTs
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		// add three table and then remote one table
		for i := 0; i < 200; i++ {
			if i%4 != 3 {
				// add table
				table := manager.CreateTableSink(model.TableID(i), maxResolvedTs)
				close := make(chan struct{})
				tableSinks = append(tableSinks, table)
				closeChs = append(closeChs, close)
				atomic.AddUint64(&maxResolvedTs, 20)
				wg.Add(1)
				go runTableSink(int64(i), table, maxResolvedTs, close)
			} else {
				// remove table
				table := tableSinks[0]
				close(closeChs[0])
				c.Assert(table.Close(), check.IsNil)
				tableSinks = tableSinks[1:]
				closeChs = closeChs[1:]
			}
			time.Sleep(10 * time.Millisecond)
		}
		atomic.StoreUint64(&maxResolvedTs, ExitSignal)
	}()

	wg.Wait()
	cancel()
	time.Sleep(1 * time.Second)
	close(errCh)
	for err := range errCh {
		c.Assert(err, check.IsNil)
	}
}

type errorSink struct {
	*check.C
}

func (e *errorSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	panic("unreachable")
}

func (e *errorSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	return errors.New("error in emit row changed events")
}

func (e *errorSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	panic("unreachable")
}

func (e *errorSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	return 0, errors.New("error in flush row changed events")
}

func (e *errorSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("unreachable")
}

func (e *errorSink) Close() error {
	return nil
}

func (s *managerSuite) TestManagerError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 16)
	manager := NewManager(ctx, &errorSink{C: c}, errCh, 0)
	defer manager.Close()
	sink := manager.CreateTableSink(1, 0)
	err := sink.EmitRowChangedEvents(ctx, &model.RowChangedEvent{
		CommitTs: 1,
	})
	c.Assert(err, check.IsNil)
	_, err = sink.FlushRowChangedEvents(ctx, 2)
	c.Assert(err, check.IsNil)
	err = <-errCh
	c.Assert(err.Error(), check.Equals, "error in emit row changed events")
}
