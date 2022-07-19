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
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

const (
	defaultConflictDetectorSlots int64 = 1024 * 1024
)

var crcTable *crc64.Table = crc64.MakeTable(crc64.ISO)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*sink)(nil)

// sink is the sink for SingleTableTxn.
type sink struct {
	conflictDetector *causality.ConflictDetector[*worker, *txnEvent]
	workers          []*worker
}

func newSink(backends []backend, conflictDetectorSlots int64) sink {
	workers := make([]*worker, 0, len(backends))
	for i, backend := range backends {
		w := newWorker(i, backend)
		w.Run()
		workers = append(workers, w)
	}
	detector := causality.NewConflictDetector[*worker, *txnEvent](workers, conflictDetectorSlots)
	return sink{conflictDetector: detector, workers: workers}
}

// WriteEvents writes events to the sink.
func (s *sink) WriteEvents(rows ...*eventsink.TxnCallbackableEvent) {
	for _, row := range rows {
		_ = s.conflictDetector.Add(&txnEvent{row, nil})
	}
}

// Close closes the sink. It won't wait for all pending items backend handled.
func (s *sink) Close() error {
	for _, w := range s.workers {
		w.Close()
	}
	s.conflictDetector.Close()
	return nil
}

type txnEvent struct {
	*eventsink.TxnCallbackableEvent
	conflictKeys []int64
}

// ConflictKeys implements causality.txnEvent interface.
func (e *txnEvent) ConflictKeys() (sums []int64) {
	if len(e.conflictKeys) > 0 {
		return e.conflictKeys
	}

	keys := genTxnKeys(e.TxnCallbackableEvent.Event)
	sums = make([]int64, 0, len(keys))
	for _, key := range keys {
		hasher := crc64.New(crcTable)
		if _, err := hasher.Write(key); err != nil {
			log.Panic("crc64 hasher fail")
		}
		sums = append(sums, int64(hasher.Sum64()))
	}
	e.conflictKeys = sums
	return
}

type worker struct {
	ID      int
	txnCh   *chann.Chann[txnWithNotifier]
	stopped chan struct{}
	wg      sync.WaitGroup

	backend backend
}

type txnWithNotifier struct {
	*txnEvent
	wantMore func()
}

func newWorker(ID int, backend backend) *worker {
	return &worker{
		ID:      ID,
		txnCh:   chann.New[txnWithNotifier](chann.Cap(-1 /*unbounded*/)),
		stopped: make(chan struct{}),
		backend: backend,
	}
}

func (w *worker) runBackground() {
	go w.Run()
}

func (w *worker) Add(txn *txnEvent, unlock func()) {
	w.txnCh.In() <- txnWithNotifier{txn, unlock}
}

func (w *worker) Close() {
	close(w.stopped)
	w.wg.Wait()
	w.txnCh.Close()
}

// Run a background loop.
func (w *worker) Run() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-w.stopped:
				return
			case txn := <-w.txnCh.Out():
				txn.wantMore()
				if err := w.backend.onTxnEvent(txn.txnEvent.TxnCallbackableEvent); err != nil {
					// TODO: handle errors.
					return
				}
			case <-w.backend.timer().C:
				if err := w.backend.onTimeout(); err != nil {
					// TODO: handle errors.
					return
				}
			}
		}
	}()
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
