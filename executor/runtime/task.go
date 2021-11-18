package runtime

import (
	"fmt"
	//"log"
	"sync/atomic"
	"time"

	"github.com/hanfei1991/microcosom/model"
)

type TaskStatus int32

const (
	Runnable TaskStatus = iota
	Blocked
	Waking
)

type Record struct {
	start   time.Time
	end     time.Time
	payload []byte
	hashVal uint32
	tid     int32
}

func (r *Record) toString() string {
	return fmt.Sprintf("start %s end %s payload %s\n", r.start.String(), r.end.String(), string(r.payload))
}

type Channel struct {
	innerChan chan *Record
	sendWaker func()
	recvWaker func()
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
		c.sendWaker()
	}
	return records
}

func (c *Channel) writeBatch(records []*Record) ([]*Record, bool) {
	for i, record := range records {
		select {
		case c.innerChan <- record:
		default:
			if i > 0 {
				c.recvWaker()
			}
			return records[i:], i == 0
		}
	}
	c.recvWaker()
	return nil, false
}

type taskContainer struct {
	cfg    *model.Task
	id     model.TaskID
	status int32
	cache  [][]*Record
	op     operator
	inputs []*Channel
	output []*Channel
	//	waker  func()
	ctx *taskContext
}

func (t *taskContainer) prepare() error {
	t.cache = make([][]*Record, len(t.output))
	return t.op.prepare()
}

func (t *taskContainer) tryAwake() bool {
	for {
		//		log.Printf("try wake task %d", t.id)
		if atomic.CompareAndSwapInt32(&t.status, int32(Blocked), int32(Waking)) {
			//log.Printf("wake task %d successful", t.id)
			return true
		}

		if atomic.CompareAndSwapInt32(&t.status, int32(Runnable), int32(Waking)) {
			//log.Printf("task %d runnable", t.id)
			return false
		}

		if atomic.LoadInt32(&t.status) == int32(Waking) {
			//log.Printf("task %d waking", t.id)
			return false
		}
	}
}

func (t *taskContainer) tryBlock() bool {
	return atomic.CompareAndSwapInt32(&t.status, int32(Runnable), int32(Blocked))
}

func (t *taskContainer) setRunnable() {
	atomic.StoreInt32(&t.status, int32(Runnable))
}

func (t *taskContainer) tryFlush() (blocked bool) {
	hasBlocked := false
	for i, cache := range t.cache {
		blocked := false
		t.cache[i], blocked = t.output[i].writeBatch(cache)
		if blocked {
			hasBlocked = true
		}
	}
	return hasBlocked
}

func (t *taskContainer) Poll() TaskStatus {
	//	log.Printf("task %d polling", t.id)
	if t.tryFlush() {
		return Blocked
	}
	r := make([]*Record, 0, len(t.inputs)*128)
	for _, input := range t.inputs {
		tmp := input.readBatch(128)
		r = append(r, tmp...)
	}

	if len(r) == 0 && len(t.inputs) > 0 {
		return Blocked
	}

	// do compute
	blocked := false
	t.cache, blocked = t.op.next(t.ctx, r)

	if t.tryFlush() {
		//log.Printf("task %d flush blocked", t.id)
		return Blocked
	}
	if blocked {
		return Blocked
	} else {
		return Runnable
	}
}
