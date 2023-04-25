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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// encodingWorker denotes the worker responsible for encoding RowChangedEvents
// to messages formatted in the specific protocol.
type encodingWorker struct {
	id           int
	changeFeedID model.ChangeFeedID
	encoder      codec.EventBatchEncoder
	isClosed     uint64
	inputCh      <-chan eventFragment
	outputCh     chan<- eventFragment
}

func newEncodingWorker(
	workerID int,
	changefeedID model.ChangeFeedID,
	encoder codec.EventBatchEncoder,
	inputCh <-chan eventFragment,
	outputCh chan<- eventFragment,
) *encodingWorker {
	return &encodingWorker{
		id:           workerID,
		changeFeedID: changefeedID,
		encoder:      encoder,
		inputCh:      inputCh,
		outputCh:     outputCh,
	}
}

func (w *encodingWorker) run(ctx context.Context) error {
	log.Debug("encoding worker started", zap.Int("workerID", w.id),
		zap.String("namespace", w.changeFeedID.Namespace),
		zap.String("changefeed", w.changeFeedID.ID))

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case frag, ok := <-w.inputCh:
				if !ok || atomic.LoadUint64(&w.isClosed) == 1 {
					return nil
				}
				err := w.encodeEvents(ctx, frag)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	})

	return eg.Wait()
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
	w.outputCh <- frag

	return nil
}

func (w *encodingWorker) close() {
	if !atomic.CompareAndSwapUint64(&w.isClosed, 0, 1) {
		return
	}
}
