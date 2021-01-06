package sink

import (
	"context"
	"math/rand"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type managerSuite struct{}

var _ = check.Suite(&testCausalitySuite{})

type checkSink struct {
}

func (c *checkSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	panic("implement me")
}

func (c *checkSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	panic("implement me")
}

func (c *checkSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	panic("implement me")
}

func (c *checkSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	panic("implement me")
}

func (c *checkSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	panic("implement me")
}

func (c *checkSink) Close() error {
	panic("implement me")
}

func (s *testCausalitySuite) TestManagerRandom(c *check.C) {
	manager := NewManager(&checkSink{}, 0)
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			ctx := context.Background()
			tableSink := manager.CreateTableSink(model.TableID(i), 0)
			var lastResolvedTs uint64
			for j := 0; j < 100; j++ {
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
		}()
	}
}
