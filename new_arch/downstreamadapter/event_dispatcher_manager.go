// Copyright 2024 PingCAP, Inc.
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

package downstreamadapter

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/new_arch/downstreamadapter/messages"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/zap"
)

type Action string

const (
	WriteEvent Action = "write event"
	PassEvent  Action = "pass event"
	WaitEvent  Action = "wait event"
	workerNum  int    = 8
)

const expiredTime = 10 * time.Second //超时时间

type worker struct {
	ctx  context.Context
	id   int
	pool *PullEventTaskThreadPool
}

func newWorker(ctx context.Context, id int, pool *PullEventTaskThreadPool) *worker {
	return &worker{ctx: ctx, id: id, pool: pool}
}

func (w *worker) run() error {
	for {
		select {
		case <-w.ctx.Done():
			log.Info("worker exists as canceled")
			return nil
		default:
			w.processTask()
		}
	}
}

func (w *worker) processTask() {
	w.pool.queueMutex.Lock()
	if w.pool.queue.Len() == 0 {
		w.pool.queueMutex.Unlock()
		// sleep?
		return
	}

	// 弹出任务队列中优先级最高的任务（即时间戳最早的任务）
	task := heap.Pop(&w.pool.queue).(PullEventTask)
	w.pool.queueMutex.Unlock()

	// 执行任务
	memoryUsageMap, _ := task.fn() // 这里本质要限制每次能拿到的内存开销最大不超过多少，这样才能比较平稳。

	// 更新内存使用情况
	w.pool.memoryMutex.Lock()
	for commitTs, memoryUsage := range memoryUsageMap {
		w.pool.memoryCostMap[task.span][commitTs] = memoryUsage
		w.pool.usedMemory += memoryUsage
	}

	// 检查内存是否超过限制
	for w.pool.usedMemory >= w.pool.maxMemory {
		// 可以这边直接持续拿锁，直到内存合适了再释放，这样就阻塞所有的 worker 去做新任务（最多做一个）
		time.Sleep(10 * time.Millisecond) // TODO:有没有更优雅的
	}
	w.pool.memoryMutex.Unlock()
}

type TableCheckpointTsPair struct {
	span         *tablepb.Span
	checkpointTs uint64
}

type TableTsPair struct {
	span     *tablepb.Span
	commitTs uint64
}

// 用来管理
type PullEventTaskThreadPool struct {
	ctx               context.Context
	pullEventTaskChan chan *PullEventTask
	maxMemory         uint64
	usedMemory        uint64

	queueMutex sync.Mutex // for queue
	queue      TaskHeap

	cond *sync.Cond

	workers []*worker

	checkpointCh chan TableCheckpointTsPair

	memoryMutex   sync.Mutex // for queue
	memoryCostMap map[*tablepb.Span]map[uint64]uint64
}

func createPullEventTaskThreadPool(ctx context.Context, maxMemory uint64) *PullEventTaskThreadPool {
	pullEventTaskThreadPool := PullEventTaskThreadPool{
		ctx:               ctx,
		pullEventTaskChan: make(chan *PullEventTask, 200), // 这个值要想一想
		maxMemory:         maxMemory,
		queue:             make(TaskHeap, 0),
		workers:           make([]*worker, workerNum),
		usedMemory:        0,
		checkpointCh:      make(chan TableCheckpointTsPair, 200), // 这个不会阻塞吧，要防一手么？
		memoryCostMap:     make(map[*tablepb.Span]map[uint64]uint64),
	}
	heap.Init(&pullEventTaskThreadPool.queue)
	for i := 0; i < workerNum; i++ {
		pullEventTaskThreadPool.workers[i] = newWorker(ctx, i, &pullEventTaskThreadPool)
	}

	return &pullEventTaskThreadPool
}

func (p *PullEventTaskThreadPool) Run() error {
	for i := 0; i < workerNum; i++ {
		go p.workers[i].run()
	}
	for {
		select {
		case <-p.ctx.Done():
			log.Info("PullEventTaskScheduler exits as canceled")
			return nil
		case pullEventTask := <-p.pullEventTaskChan:
			p.queueMutex.Lock()
			heap.Push(&p.queue, pullEventTask)
			p.queueMutex.Unlock()
		case tableCheckpointTsPair := <-p.checkpointCh:
			p.memoryMutex.Lock()
			// TODO：可以做个排序，然后二分优化
			for commitTs, memoryCost := range p.memoryCostMap[tableCheckpointTsPair.span] {
				if commitTs <= tableCheckpointTsPair.checkpointTs {
					p.usedMemory -= memoryCost
				}
			}
			p.memoryMutex.Unlock()
		}
	}
}

// 用来管理所有的 event dispatcher，以及收集 table event dispatcher 的 status， 传给 maintainer，协调后通知回去
// EventDispatcherManager 如果挂了，下面的所有 dispatcher 都应该一起挂，所以他们应该要共享同一份 context
// EventDispatcherManager 如果跟 maintainer 的通信中断超过了一定时间，就要自己 close
type EventDispatcherManager struct {
	ctx          context.Context
	changefeedID model.ChangeFeedID
	sinkType     SinkType
	//filter                      filter.Filter
	eventDispatcherMap          map[*tablepb.Span]*TableEventDispatcher // store all tables' event
	tableTriggerEventDispatcher *TableTriggerEventDispatcher            // table trigger event dispatcher
	// watermarkEventDispatcher    *WatermarkEventDispatcher               // watermark event dispatcher, only work when the downstream is non-mysql class
	barrierTs int64 // 如果 有 redo 就 heartbeat 通知这个，作为上限，没有的话，就是 -1

	cancel            context.CancelFunc
	heartBeatInterval time.Duration
	lastCommunication time.Time

	pullEventTaskThreadPool *PullEventTaskThreadPool

	trans transport.Transport
}

func createEventDispatcherManager(changefeedID model.ChangeFeedID, filter filter.Filter, startTs uint64) *EventDispatcherManager { // TODO :应该还需要一个 capture 的编号
	ctx, cancel := context.WithCancel(context.Background())
	eventDispatcherManager := EventDispatcherManager{
		ctx:          ctx,
		cancel:       cancel, // 这个刚好用来取消对应的 ctx
		changefeedID: changefeedID,
		//filter:             filter,
		eventDispatcherMap:      map[*tablepb.Span]*TableEventDispatcher{},
		barrierTs:               -1,
		heartBeatInterval:       50 * time.Millisecond,
		pullEventTaskThreadPool: createPullEventTaskThreadPool(ctx, 10*1024*1024*1024), // 先拍个10G
	}
	eventDispatcherManager.tableTriggerEventDispatcher = createTableTriggerEventDispatcher(eventDispatcherManager.ctx, changefeedID, startTs, filter)
	go eventDispatcherManager.run()

	return &eventDispatcherManager
}

// 这玩意需要返回啥？失败了就直接报错，然后调用 close？然后 maintainer 介入呗
func (e *EventDispatcherManager) run() {
	go e.pullEventTaskThreadPool.Run()

	defer e.cancel() // 也就是如果 eventDispatcherManager 退出了要先 canel （但这个是不是不应该放在 run 里？应该放在 close？)

	ticker := time.NewTicker(e.heartBeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// TODO: 拆分 handle 和 send 成两个单独的线程，测一下有没有必要为这个性能增加额外的复杂性
			recvMsgs, err := e.recvMsgs(e.ctx)
			if err != nil {
				log.Error("Event Dispatcher Manager failed to receive heart bear response message")
			}
			e.handleHeartBeatResponse(recvMsgs)

			if time.Since(e.lastCommunication) > expiredTime {
				e.close()
				log.Warn("Event Dispatcher Manager Exits due to communication timeout", zap.Any("ChangefeedID", e.changefeedID))
				return
			}

			e.sendHeartBeat() // 这个要测过性能，从触发到发送完成要多久 -- 以及这个要保证她能正常结束么，不然卡在这边没法正常退出就...
		}
	}

}
func (e *EventDispatcherManager) sendHeartBeat() {
	for span, eventDispatcher := range e.eventDispatcherMap {
		state := eventDispatcher.States()
		e.pullEventTaskThreadPool.checkpointCh <- TableCheckpointTsPair{span, state.checkpointTs}
	}

	table_trigger_state := e.tableTriggerEventDispatcher.States()

	// get state from tableTriggerEventDispatcher（包括要新建的表，以及 ddl 这边 checkpointTs 推进的情况）

	// get resource information

	// 拼接 tableSpan-state 的 message，然后做压缩以后发送

	// send heartbeat
}

func (e *EventDispatcherManager) handleHeartBeatResponse(messages *messages.HeartBeatResponse) {
	for _, new_table := range messages.NewTable {
		createTableEventDispatcher(e.ctx, new_table.Span, e.changefeedID, e.sinkType, new_table.StartTs, e.pullEventTaskThreadPool) // if needed
	}
	e.tableTriggerEventDispatcher.handleHeartBeatResponse(messages.NewTables)

	for _, tableResponse := range tableRepsonse.table_response {
		if dispatcher, ok := e.eventDispatcherMap[tableResponse.tableSpan]; ok {
			dispatcher.handleHeartBeatResponse(tableResponse.Action)
		}
	}

	// 有新消息就 update lastCommunication
	// 收到哪些 table 的 block 完成了，可以推进或者需要当前节点推进

}

// 理论上只能有且只有一条？
func (e *EventDispatcherManager) recvMsgs(ctx context.Context) (*messages.HeartBeatResponse, error) {
	recvMsgs, err := e.trans.Recv(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n := 0
	for _, msg := range recvMsgs {
		// TODO:Filter stale messages and lost messages.
		recvMsgs[n] = msg
		n++
	}
	//e.compat.AfterTransportReceive(recvMsgs[:n]) 先不做 compact
	return recvMsgs[:n], nil
}

func (e *EventDispatcherManager) createNewTableEventDispatcher(tableSpan *tablepb.Span) {

}

// 发起请求，然后返回 endTs
func (e *EventDispatcherManager) removeTableEventDispatcher() uint64 {
	// 通知 tableEventDispatcher 写完 worker 里的？然后返回 endTs
}

func (e *EventDispatcherManager) close() {
	// 首先要等所有的 dispatcher 正常 close，才能 close
}
