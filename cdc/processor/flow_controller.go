package processor

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/common"
	"go.uber.org/zap"
)

type FlowController struct {
	changefeedID model.ChangeFeedID

	memoryQuota *common.TableMemoryQuota

	mu   sync.Mutex
	memo map[model.TableID]*tableQuotaTracker
}

func NewFlowController(changefeedID model.ChangeFeedID, quota uint64) *FlowController {
	return &FlowController{
		changefeedID: changefeedID,
		mu:           sync.Mutex{},
		memoryQuota:  common.NewTableMemoryQuota(quota),
		memo:         make(map[model.TableID]*tableQuotaTracker),
	}
}

func (f *FlowController) AddTable(tableID model.TableID) {
	if _, ok := f.memo[tableID]; ok {
		log.Panic("table already exist",
			zap.Any("changefeed", f.changefeedID),
			zap.Any("tableID", tableID))
	}
	f.memo[tableID] = NewTableQuotaTracker(tableID)
	log.Info("add table to the processor's flow controller",
		zap.Any("changefeed", f.changefeedID),
		zap.Any("tableID", tableID))
}

// RemoveTable remove the table from the controller
// should be called after the whole table pipeline closed, memory released.
func (f *FlowController) RemoveTable(tableID model.TableID) {
	tracker, ok := f.memo[tableID]
	if !ok {
		log.Panic("drop the table not found",
			zap.Any("changefeed", f.changefeedID),
			zap.Any("tableID", tableID))
	}

	bytes := tracker.resolve(math.MaxUint64)
	f.memoryQuota.Release(bytes)
	delete(f.memo, tableID)
	log.Info("table removed from the processor's flow controller",
		zap.Any("changefeed", f.changefeedID),
		zap.Any("tableID", tableID),
		zap.Any("releasedBytes", bytes))
}

//Consume is called when an event has arrived for being processed by the sink.
//It will handle transaction boundaries automatically, and will not block intra-transaction.
func (f *FlowController) Consume(tableID model.TableID, commitTs uint64, size uint64, blockCallBack func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	tracker, ok := f.memo[tableID]
	if !ok {
		log.Panic("FlowController.Consume: table not found",
			zap.Any("changefeed", f.changefeedID),
			zap.Any("tableID", tableID))
	}
	lastCommitTs := atomic.LoadUint64(&tracker.lastCommitTs)
	if commitTs < lastCommitTs {
		log.Panic("commitTs regressed, report a bug",
			zap.Any("changefeed", f.changefeedID),
			zap.Any("tableID", tableID),
			zap.Uint64("commitTs", commitTs),
			zap.Uint64("lastCommitTs", lastCommitTs))
	}
	if commitTs == lastCommitTs {
		// Here commitTs == lastCommitTs, which means that we are not crossing
		// a transaction boundary. In this situation, we use `ForceConsume` because
		// blocking the event stream mid-transaction is highly likely to cause
		// a deadlock.
		// TODO fix this in the future, after we figure out how to elegantly support large txns.
		err := f.memoryQuota.ForceConsume(size)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		atomic.StoreUint64(&tracker.lastCommitTs, commitTs)
		err := f.memoryQuota.ConsumeWithBlocking(size, blockCallBack)
		if err != nil {
			return errors.Trace(err)
		}
	}

	tracker.add(commitTs, size)

	return nil
}

// Release is called when all events committed before resolvedTs has been freed from memory.
func (f *FlowController) Release(tableID model.TableID, resolvedTs uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tracker, ok := f.memo[tableID]
	if !ok {
		log.Panic("FlowController.Release: table not found",
			zap.Any("changefeed", f.changefeedID),
			zap.Any("tableID", tableID))
	}
	bytes := tracker.resolve(resolvedTs)
	f.memoryQuota.Release(bytes)
	log.Debug("FlowController.Release",
		zap.Any("changefeed", f.changefeedID),
		zap.Any("tableID", tableID),
		zap.Any("resolvedTs", resolvedTs),
		zap.Any("bytes", bytes))
}

func (f *FlowController) Abort() {
	f.memoryQuota.Abort()
}

func (f *FlowController) GetConsumption() uint64 {
	return f.memoryQuota.GetConsumption()
}

type quotaEntry struct {
	commitTs uint64
	size     uint64
}

type tableQuotaTracker struct {
	tableID model.TableID

	// queue track the processing `quotaEntries`
	queue deque.Deque
	// lastCommitTs is the last commit ts of the table.
	lastCommitTs uint64
}

func NewTableQuotaTracker(tableID model.TableID) *tableQuotaTracker {
	return &tableQuotaTracker{
		tableID: tableID,
		queue:   deque.NewDeque(),
	}
}

func (t *tableQuotaTracker) add(commitTs, size uint64) {
	t.queue.PushBack(&quotaEntry{
		commitTs: commitTs,
		size:     size,
	})
	if commitTs > t.lastCommitTs {
		t.lastCommitTs = commitTs
	}
}

func (t *tableQuotaTracker) resolve(resolvedTs uint64) uint64 {
	var result uint64
	for t.queue.Len() > 0 {
		peeked := t.queue.Front().(*quotaEntry)
		if peeked.commitTs > resolvedTs {
			break
		}
		result += peeked.size
		t.queue.PopFront()
	}
	return result
}
