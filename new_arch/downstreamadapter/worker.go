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
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/pkg/causality"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const maxRowsCount = 256 // todo:随便拍一个，后面这个是要做测试的
const maxFlushInterval = 10 * time.Millisecond

// 负责把数据彻底写入下游 mysql / tidb 中，对外提供 writeEvents(也就是内部 bg 负责 flush)，以及显式 flush 的接口
// 要看一下 mounter 的工作到底是在这里做还是在前面做掉。
// 考虑一下这个到底要不要额外加一层 backend
// TODO:这个东西写挂了，就报错 -- 有数据没写下去了 -- 这个要怎么处理？
type Worker struct {
	ctx           context.Context
	txnCh         <-chan causality.TxnWithNotifier[*TxnEvent] // 从这里拿要扔下去的 events
	id            int
	events        []*causality.TxnWithNotifier[*TxnEvent]
	rowsCount     int64
	flushInterval time.Duration
}

func NewWorker(ctx context.Context, txnCh <-chan causality.TxnWithNotifier[*TxnEvent], id int) *Worker {
	ctx, _ = context.WithCancel(ctx)
	worker := Worker{
		ctx:           ctx,
		txnCh:         txnCh,
		id:            id,
		rowsCount:     0,
		events:        make([]*causality.TxnWithNotifier[*TxnEvent], 0, 1024),
		flushInterval: maxFlushInterval,
	}
	go worker.WriteEvents()
	return &worker
}

func (w *Worker) WriteEvents() error {
	for {
		select {
		case <-w.ctx.Done():
			log.Info("Worker exits as cancelled", zap.Any("workerID", w.id))
			return w.ctx.Err()
		case txn := <-w.txnCh:
			needFlush := w.appendTxn(txn)
			if !needFlush {
				delay := time.NewTimer(w.flushInterval)
				// 当还没达到需要 flush 阈值时，就先不断的从 txnCh 拿数据，只有 txnCh 空了，且到了 tick 时间才进行 flush
				for !needFlush {
					select {
					case txn := <-w.txnCh:
						needFlush = w.appendTxn(txn)
					case <-delay.C:
						needFlush = true
					}
				}
				// Release resources promptly
				if !delay.Stop() {
					select {
					case <-delay.C:
					default:
					}
				}
			}
			// needFlush must be true here, so we can do flush.
			if err := w.doFlush(); err != nil {
				log.Error("worker do flush failed",
					zap.Int("workerID", w.id),
					zap.Error(err))
				return err
			}
		}
	}
}

// 把收到的 event 存在 events, 直到超过阈值时触发 flush
func (w *Worker) appendTxn(txn causality.TxnWithNotifier[*TxnEvent]) bool {
	w.events = append(w.events, txn)
	w.rowsCount += int64(len(txn.TxnEvent.Rows))
	if w.rowsCount >= maxRowsCount {
		return true
	}
	return false
}

// 只会在所有 workers的 dml 都写完，conflict detector 也是空的时候调用，直接刷下去
func (w *Worker) FlushEvent(txn *TxnEvent) error {
}

// 把 event 转换成写入下游的 sql。
// 这边的翻译转化先做串行，后面做并行加速
func (w *Worker) doFlush() error {
	dmls, err := w.prepareDMLs()
	if err != nil {
		log.Error("worker prepare DMLs failed", zap.Error(err))
		return errors.Trace(err)
	}

	if err := w.execDMLWithMaxRetries(dmls); err != nil {
		if errors.Cause(err) != context.Canceled {
			log.Error("execute DMLs failed", zap.Error(err))
		}
		return errors.Trace(err)
	}

}

// TODO: filter dml 过滤操作应该放在这里么
func (w *Worker) prepareDMLs() ([]string, error) {
	sqls := make([]string, 0, w.rowsCount)
	postTxnExecuted := make([]func(), 0, len(w.events)) // 其实可以直接执行了，如果卡住的话，后面再 double check 一下

	for _, event := range w.events {
		if len(event.TxnEvent.Rows) == 0 {
			continue
		}
		postTxnExecuted = append(postTxnExecuted, event.PostTxnExecuted)

		for _, row := range event.TxnEvent.Rows {
			// decode row event for the columns and precolunms
			preColumns, Columns, err := decodeRowKVEntry(event.TxnEvent.tableInfo, row.Key, row.Value, row.OldValue)
		}
		// 然后用 columns 信息去构建 sqls ，这个跟原来实现没有区别，先不写了。
	}

}

func (w *Worker) execDMLWithMaxRetries(dmls []string) error {

}

func (w *Worker) IsEmpty() bool {
	//考虑一下这个怎么做。实在不行就 sink 带一个计数器
}
