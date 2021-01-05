package sink

import (
	"context"
	"math"
	"sort"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
)

type Manager struct {
	backendSink  Sink
	checkpointTs model.Ts
	tableSinks   map[model.TableID]*tableSink
}

func NewManager(backendSink Sink, checkpointTs model.Ts) *Manager {
	return &Manager{
		backendSink:  backendSink,
		checkpointTs: checkpointTs,
		tableSinks:   make(map[model.TableID]*tableSink),
	}
}

func (m *Manager) CreateTableSink(tableID model.TableID, checkpointTs model.Ts) Sink {
	if _, exist := m.tableSinks[tableID]; exist {
		log.Panic("the table sink already exists", zap.Uint64("tableID", uint64(tableID)))
	}
	sink := &tableSink{
		tableID:   tableID,
		manager:   m,
		buffer:    make([]*model.RowChangedEvent, 0, 128),
		emittedTs: checkpointTs,
	}
	m.tableSinks[tableID] = sink
	return sink
}

func (m *Manager) Close() error {
	return m.backendSink.Close()
}

func (m *Manager) getMinEmittedTs() model.Ts {
	if len(m.tableSinks) == 0 {
		return m.checkpointTs
	}
	minTs := model.Ts(math.MaxUint64)
	for _, tableSink := range m.tableSinks {
		if minTs > tableSink.emittedTs {
			minTs = tableSink.emittedTs
		}
	}
	return minTs
}

func (m *Manager) flushBackendSink(ctx context.Context) (model.Ts, error) {
	checkpointTs, err := m.backendSink.FlushRowChangedEvents(ctx, m.getMinEmittedTs())
	if err != nil {
		return m.checkpointTs, errors.Trace(err)
	}
	m.checkpointTs = checkpointTs
	return checkpointTs, nil
}

func (m *Manager) destroyTableSink(tableID model.TableID) {
	delete(m.tableSinks, tableID)
}

type tableSink struct {
	tableID   model.TableID
	manager   *Manager
	buffer    []*model.RowChangedEvent
	emittedTs model.Ts
}

func (t *tableSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// do nothing
	return nil
}

func (t *tableSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	t.buffer = append(t.buffer, rows...)
	return nil
}

func (t *tableSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// the table sink doesn't receive the DDL event
	return nil
}

func (t *tableSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	i := sort.Search(len(t.buffer), func(i int) bool {
		return t.buffer[i].CommitTs > resolvedTs
	})
	if i == 0 {
		t.emittedTs = resolvedTs
		return t.manager.flushBackendSink(ctx)
	}
	resolvedRows := t.buffer[:i]
	t.buffer = t.buffer[i:]
	err := t.manager.backendSink.EmitRowChangedEvents(ctx, resolvedRows...)
	if err != nil {
		return t.manager.checkpointTs, errors.Trace(err)
	}
	t.emittedTs = resolvedTs
	return t.manager.flushBackendSink(ctx)
}

func (t *tableSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// the table sink doesn't receive the checkpoint event
	return nil
}

func (t *tableSink) Close() error {
	t.manager.destroyTableSink(t.tableID)
	return nil
}
