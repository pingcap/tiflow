// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

// encodingWorker denotes the worker responsible for encoding RowChangedEvents
// to messages formatted in the specific protocol.
type encodingWorker struct {
	id           int
	changeFeedID model.ChangeFeedID
	wg           sync.WaitGroup
	encoder      codec.EventBatchEncoder
	isClosed     uint64
	inputCh      *chann.Chann[eventFragment]
	defragmenter *defragmenter
	errCh        chan<- error
}

func newEncodingWorker(
	workerID int,
	changefeedID model.ChangeFeedID,
	encoder codec.EventBatchEncoder,
	inputCh *chann.Chann[eventFragment],
	defragmenter *defragmenter,
	errCh chan<- error,
) *encodingWorker {
	return &encodingWorker{
		id:           workerID,
		changeFeedID: changefeedID,
		encoder:      encoder,
		inputCh:      inputCh,
		defragmenter: defragmenter,
		errCh:        errCh,
	}
}

func (w *encodingWorker) run(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		log.Debug("encoding worker started", zap.Int("workerID", w.id),
			zap.String("namespace", w.changeFeedID.Namespace),
			zap.String("changefeed", w.changeFeedID.ID))
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case frag, ok := <-w.inputCh.Out():
				if !ok || atomic.LoadUint64(&w.isClosed) == 1 {
					return
				}
				err := w.encodeEvents(ctx, frag)
				if err != nil {
					w.errCh <- err
					return
				}
			}
		}
	}()
}

func (w *encodingWorker) encodeEvents(ctx context.Context, frag eventFragment) error {
	var err error
	length := len(frag.event.Event.Rows)

	for idx, event := range frag.event.Event.Rows {
		// because each TxnCallbackableEvent contains one Callback and multiple RowChangedEvents,
		// we only append RowChangedEvent attached with a Callback to EventBatchEncoder for the
		// last RowChangedEvent.
		if idx != length-1 {
			err = w.encoder.AppendRowChangedEvent(ctx, "", event, nil)
		} else {
			err = w.encoder.AppendRowChangedEvent(ctx, "", event, frag.event.Callback)
		}
		if err != nil {
			return err
		}
	}

	msgs := w.encoder.Build()
	frag.encodedMsgs = msgs
	w.defragmenter.registerFrag(frag)

	return nil
}

func (w *encodingWorker) close() {
	if !atomic.CompareAndSwapUint64(&w.isClosed, 0, 1) {
		return
	}
	w.wg.Wait()
}
