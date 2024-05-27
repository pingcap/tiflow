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
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/new_arch/downstreamadapter/messages"
)

// TableEventDispatcher is responsible to pull events from TableStreamManager, and dispatch them to downstream.
// TableEventDispatcher only works for one table in one changefeed.
// TODO:挂了的话也是应该重启
type TableEventDispatcher struct {
	span         *messages.Span
	changefeedID model.ChangeFeedID

	mutex           sync.Mutex  // use to protect txnEvents and largestCommitTs
	txnEvents       []*TxnEvent // use to store the events pulled but not dispatched to downstream，不如用 channel
	largestCommitTs uint64      // the largest commitTs in the txnEvents, 主要用于判断 txnEvent 是不是不会有新数据了，这个在 pullEvents 更新，如果 endTs 没拉完就 commitTs - 1

	// sink is used to encode the event, and push downstream
	// sink is a interface, it could be supported for different downstream,
	// such as mysql / tidb / kafka / cloudStorage etc.
	sink Sink

	state *State // 也要 mutex 保护一下的

	blockCh chan int
	endTs   uint64

	cancel context.CancelFunc
	ctx    context.Context
	pool   *PullEventTaskThreadPool
}

func createTableEventDispatcher(ctx context.Context, span *messages.Span, changeFeedID model.ChangeFeedID, sinkType SinkType, startTs uint64, pool *PullEventTaskThreadPool) *TableEventDispatcher {
	ctx, cancel := context.WithCancel(ctx)
	tableEventDispatcher := TableEventDispatcher{
		span:         span,
		changefeedID: changeFeedID,
		blockCh:      make(chan int, 1),
		ctx:          ctx,
		cancel:       cancel,
		pool:         pool,
	}
	tableEventDispatcher.sink = createSink(ctx, sinkType)
	tableEventDispatcher.generatePullEventsTask(startTs)
	go tableEventDispatcher.Run()
	return &tableEventDispatcher
}

func (d *TableEventDispatcher) generatePullEventsTask(startTs uint64) {
	task := PullEventTask{
		fn: func() (map[uint64]uint64, error) {
			txnEvents, isEnd, _ := API(d.span, startTs, d.changefeedID) // todo
			memoryUsage := make(map[uint64]uint64)
			for _, txnEvent := range txnEvents {
				memoryUsage[txnEvent.CommitTs] = txnEvent.memoryUsage()

			}
			d.mutex.Lock()
			d.txnEvents = append(d.txnEvents, txnEvents...)
			d.mutex.Unlock()
			if isEnd {
				d.largestCommitTs = txnEvents[len(txnEvents)-1].CommitTs
			}

			d.generatePullEventsTask(d.largestCommitTs)

			return memoryUsage, nil
		},
		span:    d.span,
		StartTs: startTs,
	}
	d.pool.pullEventTaskChan <- &task
}

func (d *TableEventDispatcher) States() State {
	// 上锁
	//如果 没有 block住，就去拿 sink.getCheckpointTs
	return *d.state
}

// It's a background goroutine, to get txnEvent and push to the workers
// 感觉用 ch 来存 txn 是不是更合适，这样就不用加锁，也不影响后面的push
func (d *TableEventDispatcher) Run() error {
	for {
		d.mutex.Lock()
		if len(d.txnEvents) == 0 {
			d.mutex.Unlock()
			time.Sleep(time.Second) // 挫逼写法，后面 mutex 把 txn 包起来统一接口
			continue
		}
		// txn event is available
		if d.txnEvents[0].CommitTs <= d.largestCommitTs {
			// deal with the txn
			event := d.txnEvents[0]
			d.txnEvents = d.txnEvents[1:]
			d.mutex.Unlock()

			if event.eventType == DMLEvent {
				err := d.sink.WriteTxnEvent(event)
				if err != nil {
					return err
				}
			} else if event.eventType == DDLEvent {
				// 先看有没有资格往下推，等到 isEmpty true
				// 先判断是不是 单表的 ddl，是的话，就等 sink.InEmtpy true 的时候往下推
				// 如果不是单表或者是 syncEvent，就直接写自己的 block state，等待 dispatcher给自己发送想要的消息，然后设置 state
				// 后面如果推动了要重置 state -- 这边可以用一个  channel 来管，收到了就下推
				value := <-d.blockCh

				if value == 1 {
					continue // (看一下要不要推动 checkpointTs 啥的)
				} else if value == 2 {
					d.sink.FlushEvent(event)
				}
			}
		}
	}
}

func (d *TableEventDispatcher) handleHeartBeatResponse(action Action) {
	if action == PassEvent {
		d.blockCh <- 1
	} else if action == WriteEvent {
		d.blockCh <- 2
	}
}

func (d *TableEventDispatcher) Close() error {
	e.sink.close()
	e.cancel()
}
