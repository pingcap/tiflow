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
	"testing"
	"time"

	"github.com/pingcap/errors"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type managerSuite struct{}

var _ = check.Suite(&managerSuite{})

type checkSink struct {
	*check.C
	rows           map[model.TableID][]*model.RowChangedEvent
	rowsMu         sync.Mutex
	lastResolvedTs map[model.TableID]uint64
}

func newCheckSink(c *check.C) *checkSink {
	return &checkSink{
		C:              c,
		rows:           make(map[model.TableID][]*model.RowChangedEvent),
		lastResolvedTs: make(map[model.TableID]uint64),
	}
}

func (c *checkSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	c.rowsMu.Lock()
	defer c.rowsMu.Unlock()
	for _, row := range rows {
		c.rows[row.Table.TableID] = append(c.rows[row.Table.TableID], row)
	}
	return nil
}

func (c *checkSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	panic("unreachable")
}

func (c *checkSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
	c.rowsMu.Lock()
	defer c.rowsMu.Unlock()
	var newRows []*model.RowChangedEvent
	rows := c.rows[tableID]
	for _, row := range rows {
		if row.CommitTs <= c.lastResolvedTs[tableID] {
			return c.lastResolvedTs[tableID], errors.Errorf("commit-ts(%d) is not greater than lastResolvedTs(%d)", row.CommitTs, c.lastResolvedTs)
		}
		if row.CommitTs > resolvedTs {
			newRows = append(newRows, row)
		}
	}

	c.Assert(c.lastResolvedTs[tableID], check.LessEqual, resolvedTs)
	c.lastResolvedTs[tableID] = resolvedTs
	c.rows[tableID] = newRows

	return c.lastResolvedTs[tableID], nil
}

func (c *checkSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("unreachable")
}

func (c *checkSink) Close(ctx context.Context) error {
	return nil
}

func (c *checkSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}

func (s *managerSuite) TestManagerRandom(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 16)
	manager := NewManager(ctx, newCheckSink(c), errCh, 0, "", "")
	defer manager.Close(ctx)
	goroutineNum := 10
	rowNum := 100
	var wg sync.WaitGroup
	tableSinks := make([]Sink, goroutineNum)
	for i := 0; i < goroutineNum; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tableSinks[i] = manager.CreateTableSink(model.TableID(i), 0)
		}()
	}
	wg.Wait()
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
					_, err := tableSink.FlushRowChangedEvents(ctx, model.TableID(i), resolvedTs)
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
			_, err := tableSink.FlushRowChangedEvents(ctx, model.TableID(i), uint64(rowNum))
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
	manager := NewManager(ctx, newCheckSink(c), errCh, 0, "", "")
	defer manager.Close(ctx)
	goroutineNum := 200
	var wg sync.WaitGroup
	const ExitSignal = uint64(math.MaxUint64)

	var maxResolvedTs uint64
	tableSinks := make([]Sink, 0, goroutineNum)
	tableCancels := make([]context.CancelFunc, 0, goroutineNum)
	runTableSink := func(ctx context.Context, index int64, sink Sink, startTs uint64) {
		defer wg.Done()
		lastResolvedTs := startTs
		for {
			select {
			case <-ctx.Done():
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
			_, err := sink.FlushRowChangedEvents(ctx, sink.(*tableSink).tableID, resolvedTs)
			if err != nil {
				c.Assert(errors.Cause(err), check.Equals, context.Canceled)
			}
			lastResolvedTs = resolvedTs
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		// add three table and then remote one table
		for i := 0; i < goroutineNum; i++ {
			if i%4 != 3 {
				// add table
				table := manager.CreateTableSink(model.TableID(i), maxResolvedTs)
				ctx, cancel := context.WithCancel(ctx)
				tableCancels = append(tableCancels, cancel)
				tableSinks = append(tableSinks, table)
				atomic.AddUint64(&maxResolvedTs, 20)
				wg.Add(1)
				go runTableSink(ctx, int64(i), table, maxResolvedTs)
			} else {
				// remove table
				table := tableSinks[0]
				// note when a table is removed, no more data can be sent to the
				// backend sink, so we cancel the context of this table sink.
				tableCancels[0]()
				c.Assert(table.Close(ctx), check.IsNil)
				tableSinks = tableSinks[1:]
				tableCancels = tableCancels[1:]
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

func (s *managerSuite) TestManagerDestroyTableSink(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 16)
	manager := NewManager(ctx, newCheckSink(c), errCh, 0, "", "")
	defer manager.Close(ctx)

	tableID := int64(49)
	tableSink := manager.CreateTableSink(tableID, 100)
	err := tableSink.EmitRowChangedEvents(ctx, &model.RowChangedEvent{
		Table:    &model.TableName{TableID: tableID},
		CommitTs: uint64(110),
	})
	c.Assert(err, check.IsNil)
	_, err = tableSink.FlushRowChangedEvents(ctx, tableID, 110)
	c.Assert(err, check.IsNil)
	err = manager.destroyTableSink(ctx, tableID)
	c.Assert(err, check.IsNil)
}

// Run the benchmark
// go test -benchmem -run='^$' -bench '^(BenchmarkManagerFlushing)$' github.com/pingcap/tiflow/cdc/sink
func BenchmarkManagerFlushing(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 16)
	manager := NewManager(ctx, newCheckSink(nil), errCh, 0, "", "")

	// Init table sinks.
	goroutineNum := 2000
	rowNum := 2000
	var wg sync.WaitGroup
	tableSinks := make([]Sink, goroutineNum)
	for i := 0; i < goroutineNum; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			tableSinks[i] = manager.CreateTableSink(model.TableID(i), 0)
		}()
	}
	wg.Wait()

	// Concurrent emit events.
	for i := 0; i < goroutineNum; i++ {
		i := i
		tableSink := tableSinks[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 1; j < rowNum; j++ {
				err := tableSink.EmitRowChangedEvents(context.Background(), &model.RowChangedEvent{
					Table:    &model.TableName{TableID: int64(i)},
					CommitTs: uint64(j),
				})
				if err != nil {
					b.Error(err)
				}
			}
		}()
	}
	wg.Wait()

	// All tables are flushed concurrently, except table 0.
	for i := 1; i < goroutineNum; i++ {
		i := i
		tblSink := tableSinks[i]
		go func() {
			for j := 1; j < rowNum; j++ {
				if j%2 == 0 {
					_, err := tblSink.FlushRowChangedEvents(context.Background(), tblSink.(*tableSink).tableID, uint64(j))
					if err != nil {
						b.Error(err)
					}
				}
			}
		}()
	}

	b.ResetTimer()
	// Table 0 flush.
	tblSink := tableSinks[0]
	for i := 0; i < b.N; i++ {
		_, err := tblSink.FlushRowChangedEvents(context.Background(), tblSink.(*tableSink).tableID, uint64(rowNum))
		if err != nil {
			b.Error(err)
		}
	}
	b.StopTimer()

	cancel()
	_ = manager.Close(ctx)
	close(errCh)
	for err := range errCh {
		if err != nil {
			b.Error(err)
		}
	}
}

type errorSink struct {
	*check.C
}

func (e *errorSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	return errors.New("error in emit row changed events")
}

func (e *errorSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	panic("unreachable")
}

func (e *errorSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
	return 0, errors.New("error in flush row changed events")
}

func (e *errorSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("unreachable")
}

func (e *errorSink) Close(ctx context.Context) error {
	return nil
}

func (e *errorSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}

func (s *managerSuite) TestManagerError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 16)
	manager := NewManager(ctx, &errorSink{C: c}, errCh, 0, "", "")
	defer manager.Close(ctx)
	sink := manager.CreateTableSink(1, 0)
	err := sink.EmitRowChangedEvents(ctx, &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{TableID: 1},
	})
	c.Assert(err, check.IsNil)
	_, err = sink.FlushRowChangedEvents(ctx, 1, 2)
	c.Assert(err, check.IsNil)
	err = <-errCh
	c.Assert(err.Error(), check.Equals, "error in emit row changed events")
}
