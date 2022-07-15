// Copyright 2022 PingCAP, Inc.
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

package txn

import (
	"encoding/binary"
	"hash/crc64"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/causality"
	"go.uber.org/zap"
)

const (
	conflictDetectorSlots int64 = 1024 * 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*Sink)(nil)

// Sink is the sink for SingleTableTxn.
type Sink struct {
	conflictDetector *causality.ConflictDetector[*worker, txnEvent]
	workers          []*worker
}

func newSink(bes []backend) Sink {
	var workers []*worker = make([]*worker, 0, len(bes))
	for i, be := range bes {
		w := newWorker(i, be)
		w.wg.Add(1)
		go w.Run()
		workers = append(workers, w)
	}
	detector := causality.NewConflictDetector[*worker, txnEvent](workers, conflictDetectorSlots)
	return Sink{detector, workers}
}

// WriteEvents writes events to the sink.
func (s *Sink) WriteEvents(rows ...*eventsink.TxnCallbackableEvent) {
	for _, row := range rows {
		_ = s.conflictDetector.Add(txnEvent{row})
	}
}

// Close closes the sink. It won't wait for all pending items be handled.
func (s *Sink) Close() error {
	for _, w := range s.workers {
		w.Close()
	}
	s.conflictDetector.Close()
	return nil
}

type txnEvent struct {
	*eventsink.TxnCallbackableEvent
}

// ConflictKeys implements causality.txnEvent interface.
func (e txnEvent) ConflictKeys() (sums []int64) {
	keys := genTxnKeys(e.TxnCallbackableEvent.Event)
	sums = make([]int64, 0, len(keys))
	for _, key := range keys {
		hasher := crc64.New(crc64.MakeTable(crc64.ISO))
		if _, err := hasher.Write(key); err != nil {
			log.Panic("crc64 hasher fail")
		}
		sums = append(sums, int64(hasher.Sum64()))
	}
	return
}

type worker struct {
	ID      int
	txnCh   chan txnWithNotifier
	stopped chan struct{}
	wg      sync.WaitGroup

	be backend
}

type txnWithNotifier struct {
	txnEvent
	wantMore func()
}

func newWorker(ID int, be backend) *worker {
	return &worker{
		ID:      ID,
		txnCh:   make(chan txnWithNotifier, 1),
		stopped: make(chan struct{}, 1),
		be:      be,
	}
}

func (w *worker) Add(txn txnEvent, unlock func()) {
	w.txnCh <- txnWithNotifier{txn, unlock}
}

func (w *worker) Close() {
	w.stopped <- struct{}{}
	w.wg.Wait()
}

func (w *worker) Run() {
	defer w.wg.Done()
	notifier := w.be.notifier()
	for {
		select {
		case <-w.stopped:
			return
		case txn := <-w.txnCh:
			txn.wantMore()
			if err := w.be.onTxnEvent(txn.txnEvent.TxnCallbackableEvent); err != nil {
				// TODO: handle errors.
				return
			}
		case <-notifier.C:
			if err := w.be.onNotify(); err != nil {
				// TODO: handle errors.
				return
			}
		}
	}
}

func genTxnKeys(txn *model.SingleTableTxn) [][]byte {
	if len(txn.Rows) == 0 {
		return nil
	}
	keysSet := make(map[string]struct{}, len(txn.Rows))
	for _, row := range txn.Rows {
		rowKeys := genRowKeys(row)
		for _, key := range rowKeys {
			keysSet[string(key)] = struct{}{}
		}
	}
	keys := make([][]byte, 0, len(keysSet))
	for key := range keysSet {
		keys = append(keys, []byte(key))
	}
	return keys
}

func genRowKeys(row *model.RowChangedEvent) [][]byte {
	var keys [][]byte
	if len(row.Columns) != 0 {
		for iIdx, idxCol := range row.IndexColumns {
			key := genKeyList(row.Columns, iIdx, idxCol, row.Table.TableID)
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(row.PreColumns) != 0 {
		for iIdx, idxCol := range row.IndexColumns {
			key := genKeyList(row.PreColumns, iIdx, idxCol, row.Table.TableID)
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		// use table ID as key if no key generated (no PK/UK),
		// no concurrence for rows in the same table.
		log.Debug("use table id as the key", zap.Int64("tableID", row.Table.TableID))
		tableKey := make([]byte, 8)
		binary.BigEndian.PutUint64(tableKey, uint64(row.Table.TableID))
		keys = [][]byte{tableKey}
	}
	return keys
}

func genKeyList(columns []*model.Column, iIdx int, colIdx []int, tableID int64) []byte {
	var key []byte
	for _, i := range colIdx {
		// if a column value is null, we can ignore this index
		// If the index contain generated column, we can't use this key to detect conflict with other DML,
		// Because such as insert can't specified the generated value.
		if columns[i] == nil || columns[i].Value == nil || columns[i].Flag.IsGeneratedColumn() {
			return nil
		}
		key = append(key, []byte(model.ColumnValueString(columns[i].Value))...)
		key = append(key, 0)
	}
	if len(key) == 0 {
		return nil
	}
	tableKey := make([]byte, 16)
	binary.BigEndian.PutUint64(tableKey[:8], uint64(iIdx))
	binary.BigEndian.PutUint64(tableKey[8:], uint64(tableID))
	key = append(key, tableKey...)
	return key
}
