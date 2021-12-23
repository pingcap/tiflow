package runtime

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/test"
)

type TaskStatus int32

const (
	Runnable TaskStatus = iota
	Blocked
	Waking
	Stop
)

type Record struct {
	FlowID  string
	End     time.Time
	Payload interface{}
	Tid     int32
}

type Channel struct {
	innerChan chan *Record
	sendCtx   *TaskContext
	recvCtx   *TaskContext
}

func (c *Channel) readBatch(batch int) []*Record {
	records := make([]*Record, 0, batch)
	for i := 0; i < batch; i++ {
		select {
		case record := <-c.innerChan:
			records = append(records, record)
		default:
			break
		}
	}
	if len(records) > 0 {
		c.sendCtx.Wake()
	}
	return records
}

func (c *Channel) writeBatch(records []*Record) ([]*Record, bool) {
	for i, record := range records {
		select {
		case c.innerChan <- record:
		default:
			if i > 0 {
				c.recvCtx.Wake()
			}
			return records[i:], i == 0
		}
	}
	c.recvCtx.Wake()
	return nil, false
}

type TaskContext struct {
	Wake func()
	// err error // record error during async job
	TestCtx *test.Context
}

// a vector of records
type Chunk []*Record

type taskContainer struct {
	cfg         *model.Task
	id          model.ID
	status      int32
	inputCache  []Chunk
	outputCache []Chunk
	op          Operator
	inputs      []*Channel
	outputs     []*Channel
	ctx         *TaskContext

	stopLock sync.Mutex
}

func (t *taskContainer) prepare() error {
	t.inputCache = make([]Chunk, len(t.inputs))
	t.outputCache = make([]Chunk, len(t.outputs))
	return t.op.Prepare(t.ctx)
}

func (t *taskContainer) tryAwake() bool {
	for {
		if atomic.LoadInt32(&t.status) == int32(Stop) {
			return false
		}
		//		log.Printf("try wake task %d", t.id)
		if atomic.CompareAndSwapInt32(&t.status, int32(Blocked), int32(Waking)) {
			// log.Printf("wake task %d successful", t.id)
			return true
		}

		if atomic.CompareAndSwapInt32(&t.status, int32(Runnable), int32(Waking)) {
			// log.Printf("task %d runnable", t.id)
			return false
		}

		if atomic.LoadInt32(&t.status) == int32(Waking) {
			// log.Printf("task %d waking", t.id)
			return false
		}
	}
}

func (t *taskContainer) tryBlock() bool {
	return atomic.CompareAndSwapInt32(&t.status, int32(Runnable), int32(Blocked))
}

func (t *taskContainer) setRunnable() {
	atomic.CompareAndSwapInt32(&t.status, int32(Waking), int32(Runnable))
}

func (t *taskContainer) tryFlush() (blocked bool) {
	hasBlocked := false
	for i, cache := range t.outputCache {
		blocked := false
		t.outputCache[i], blocked = t.outputs[i].writeBatch(cache)
		if blocked {
			hasBlocked = true
		}
	}
	return hasBlocked
}

func (t *taskContainer) readDataFromInput(idx int, batch int) Chunk {
	switch idx {
	case DontNeedData:
		return make(Chunk, 1) // return a record to drive
	case DontRequireIndex:
		var ret Chunk
		for i, cache := range t.inputCache {
			ret = append(ret, cache...)
			t.inputCache[i] = t.inputCache[i][:0]
		}
		if len(ret) != 0 {
			return ret
		}
		for _, input := range t.inputs {
			ret = append(ret, input.readBatch(32)...)
		}
		return ret
	}
	if len(t.inputCache[idx]) != 0 {
		chk := t.inputCache[idx]
		t.inputCache[idx] = t.inputCache[idx][:0]
		return chk
	}
	return t.inputs[idx].readBatch(batch)
}

const (
	DontNeedData     int = -1
	DontRequireIndex int = -2
)

func (t *taskContainer) Stop() error {
	for {
		if atomic.LoadInt32(&t.status) == int32(Stop) {
			return nil
		}

		if atomic.CompareAndSwapInt32(&t.status, int32(Runnable), int32(Stop)) {
			break
		}

		if atomic.CompareAndSwapInt32(&t.status, int32(Blocked), int32(Stop)) {
			break
		}
	}

	t.stopLock.Lock()
	defer t.stopLock.Unlock()

	return t.op.Close()
}

func (t *taskContainer) Poll() TaskStatus {
	t.stopLock.Lock()
	defer t.stopLock.Unlock()
	if atomic.LoadInt32(&t.status) == int32(Stop) {
		return Stop
	}
	if t.tryFlush() {
		return Blocked
	}
	idx := t.op.NextWantedInputIdx()
	r := t.readDataFromInput(idx, 128)

	if len(r) == 0 && len(t.inputs) != 0 {
		// we don't have any input data
		return Blocked
	}

	// do compute
	blocked := false
	var outputs []Chunk
	var err error
	for i, record := range r {
		outputs, blocked, err = t.op.Next(t.ctx, record, idx)
		if err != nil {
			// TODO: report error to job manager
			panic(err)
		}
		for i, output := range outputs {
			t.outputCache[i] = append(t.outputCache[i], output...)
		}
		// TODO: limit the amount of output records
		if blocked {
			if i+1 < len(r) {
				if idx == DontRequireIndex {
					// If we didn't require an index, put them to position 0.
					idx = 0
				}
				t.inputCache[idx] = append(t.inputCache[idx], r[i+1:]...)
			}
			break
		}
	}

	if t.tryFlush() {
		// log.Printf("task %d flush blocked", t.id)
		return Blocked
	}
	if blocked {
		return Blocked
	}
	return Runnable
}
